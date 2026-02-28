"""Integration tests for video scene indexing: persistence and resume (testcontainers Postgres)."""

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from sqlalchemy import text
from sqlmodel import SQLModel

from src.ai.schema import ModelCard, VisualAnalysis
from src.ai.vision_base import BaseVisionAnalyzer
from src.models.entities import AssetType, Library, SceneKeepReason, SystemMetadata
from src.repository.asset_repo import AssetRepository
from src.repository.video_scene_repo import (
    VideoActiveState,
    VideoSceneListItem,
    VideoSceneRepository,
    VideoSceneRow,
)
from src.video.high_res_extractor import SOI, EOI
from src.video.indexing import needs_ocr, run_video_scene_indexing, run_vision_on_scenes
from src.video.scene_segmenter import SceneResult

pytestmark = [pytest.mark.slow]


class _SequentialDescriptionAnalyzer(BaseVisionAnalyzer):
    """Returns descriptions from a list in order (for dedup testing)."""

    def __init__(self, descriptions: list[str]):
        self._descriptions = list(descriptions)
        self._index = 0

    def get_model_card(self) -> ModelCard:
        return ModelCard(name="test-sequential", version="1.0")

    def analyze_image(
        self,
        image_path: Path,
        mode: str = "full",
        max_tokens: int | None = None,
        should_flush_memory: bool = False,
    ) -> VisualAnalysis:
        desc = self._descriptions[self._index] if self._index < len(self._descriptions) else ""
        self._index += 1
        return VisualAnalysis(description=desc, tags=[], ocr_text=None)


def _create_tables_and_seed(engine, session_factory):
    """Create all tables and seed schema_version. Return (AssetRepository, VideoSceneRepository)."""
    SQLModel.metadata.create_all(engine)
    session = session_factory()
    try:
        existing = session.get(SystemMetadata, "schema_version")
        if existing is None:
            session.add(SystemMetadata(key="schema_version", value="1"))
            session.commit()
    finally:
        session.close()
    return AssetRepository(session_factory), VideoSceneRepository(session_factory)


def _ensure_library_and_asset(session_factory, slug: str) -> int:
    """Insert a library and one video asset. Return asset id."""
    session = session_factory()
    try:
        session.add(
            Library(
                slug=slug,
                name="Video Lib",
                absolute_path="/tmp/vid-lib",
                is_active=True,
                sampling_limit=100,
            )
        )
        session.commit()
    finally:
        session.close()
    asset_repo = AssetRepository(session_factory)
    asset_repo.upsert_asset(slug, "video.mp4", AssetType.video, 1000.0, 5000)
    session = session_factory()
    try:
        row = session.execute(
            text("SELECT id FROM asset WHERE library_id = :slug AND rel_path = 'video.mp4'"),
            {"slug": slug},
        ).fetchone()
        assert row is not None
        return row[0]
    finally:
        session.close()


def test_indexing_persists_scenes_and_clears_state_on_eof(engine, _session_factory, tmp_path):
    """Full run: mock segmenter yields two scenes then EOF; assert 2 rows in video_scenes and no active_state."""
    asset_repo, video_repo = _create_tables_and_seed(engine, _session_factory)
    asset_id = _ensure_library_and_asset(_session_factory, "vid-int-full")
    w, h = 480, 270
    frame_size = w * h * 3
    fake_frame = b"\x80" * frame_size

    scene1 = SceneResult(
        best_frame_bytes=fake_frame,
        best_pts=1.0,
        scene_start_pts=0.0,
        scene_end_pts=5.0,
        keep_reason=SceneKeepReason.phash,
        sharpness_score=10.0,
    )
    state1 = ("abc123", 5.0, 5.0, -1.0)
    scene2 = SceneResult(
        best_frame_bytes=fake_frame,
        best_pts=8.0,
        scene_start_pts=5.0,
        scene_end_pts=12.0,
        keep_reason=SceneKeepReason.forced,
        sharpness_score=20.0,
    )
    mock_yields = [(scene1, state1), (scene2, None)]

    video_path = tmp_path / "video.mp4"
    video_path.write_bytes(b"fake")
    mock_config = MagicMock()
    mock_config.data_dir = str(tmp_path)
    with patch("src.video.indexing.get_config", return_value=mock_config):
        with patch("src.video.indexing.VideoScanner") as MockScanner:
            MockScanner.return_value.out_width = w
            MockScanner.return_value.out_height = h
            with patch("src.video.indexing.SceneSegmenter") as MockSegmenter:
                MockSegmenter.return_value.iter_scenes.return_value = mock_yields
                run_video_scene_indexing(asset_id, video_path, "vid-int-full", video_repo)

    assert video_repo.get_max_end_ts(asset_id) == 12.0
    assert video_repo.get_active_state(asset_id) is None
    session = _session_factory()
    try:
        count = session.execute(
            text("SELECT COUNT(*) FROM video_scenes WHERE asset_id = :aid"),
            {"aid": asset_id},
        ).scalar()
        assert count == 2
    finally:
        session.close()

    # Scene indexing does not build preview.webp; preview image is derived from first/best scene frame in API.
    session = _session_factory()
    try:
        count = session.execute(
            text("SELECT COUNT(*) FROM video_scenes WHERE asset_id = :aid"),
            {"aid": asset_id},
        ).scalar()
        assert count == 2
    finally:
        session.close()


def test_indexing_resume_continues_and_clears_state(engine, _session_factory, tmp_path):
    """Resume: pre-seed one scene and active_state; mock segmenter yields one more scene (EOF). Assert 2 scenes and no active_state."""
    asset_repo, video_repo = _create_tables_and_seed(engine, _session_factory)
    asset_id = _ensure_library_and_asset(_session_factory, "vid-int-resume")
    # Pre-seed JPEG for the pre-inserted scene
    rep_frame_dir = tmp_path / "video_scenes" / "vid-int-resume" / str(asset_id)
    rep_frame_dir.mkdir(parents=True)
    (rep_frame_dir / "0.000_5.000.jpg").write_bytes(b"fake jpeg")
    # Pre-insert one scene and active state (simulate crash after first scene)
    video_repo.save_scene_and_update_state(
        asset_id,
        VideoSceneRow(
            start_ts=0.0,
            end_ts=5.0,
            description=None,
            metadata=None,
            sharpness_score=10.0,
            rep_frame_path=f"video_scenes/vid-int-resume/{asset_id}/0.000_5.000.jpg",
            keep_reason="phash",
        ),
        VideoActiveState(
            anchor_phash="resumed_hash",
            scene_start_ts=5.0,
            current_best_pts=7.0,
            current_best_sharpness=5.0,
        ),
    )
    w, h = 480, 270
    frame_size = w * h * 3
    fake_frame = b"\x80" * frame_size

    scene_final = SceneResult(
        best_frame_bytes=fake_frame,
        best_pts=8.0,
        scene_start_pts=5.0,
        scene_end_pts=12.0,
        keep_reason=SceneKeepReason.forced,
        sharpness_score=20.0,
    )
    video_path = tmp_path / "video.mp4"
    video_path.write_bytes(b"fake")
    with patch("src.video.indexing.VideoScanner") as MockScanner:
        MockScanner.return_value.out_width = w
        MockScanner.return_value.out_height = h
        with patch("src.video.indexing.SceneSegmenter") as MockSegmenter:
            MockSegmenter.return_value.iter_scenes.return_value = [(scene_final, None)]
            run_video_scene_indexing(asset_id, video_path, "vid-int-resume", video_repo)

    assert video_repo.get_max_end_ts(asset_id) == 12.0
    assert video_repo.get_active_state(asset_id) is None
    session = _session_factory()
    try:
        count = session.execute(
            text("SELECT COUNT(*) FROM video_scenes WHERE asset_id = :aid"),
            {"aid": asset_id},
        ).scalar()
        assert count == 2
    finally:
        session.close()


def test_indexing_vision_dedup_flags_semantic_duplicate(engine, _session_factory, tmp_path):
    """With vision_analyzer, two scenes with similar descriptions get semantic_duplicate on the second."""
    _, video_repo = _create_tables_and_seed(engine, _session_factory)
    asset_id = _ensure_library_and_asset(_session_factory, "vid-int-dedup")
    w, h = 480, 270
    frame_size = w * h * 3
    fake_frame = b"\x80" * frame_size
    # Descriptions chosen so token_set_ratio > 85
    desc1 = "A man standing in a room"
    desc2 = "A man standing in a room with a window"
    analyzer = _SequentialDescriptionAnalyzer([desc1, desc2])

    scene1 = SceneResult(
        best_frame_bytes=fake_frame,
        best_pts=1.0,
        scene_start_pts=0.0,
        scene_end_pts=5.0,
        keep_reason=SceneKeepReason.phash,
        sharpness_score=10.0,
    )
    state1 = ("abc123", 5.0, 5.0, -1.0)
    scene2 = SceneResult(
        best_frame_bytes=fake_frame,
        best_pts=8.0,
        scene_start_pts=5.0,
        scene_end_pts=12.0,
        keep_reason=SceneKeepReason.forced,
        sharpness_score=20.0,
    )
    mock_yields = [(scene1, state1), (scene2, None)]

    minimal_jpeg = SOI + b"x" + EOI
    showinfo_line = "showinfo pts_time:1.0 n:30"

    video_path = tmp_path / "video.mp4"
    video_path.write_bytes(b"fake")
    with patch("src.video.indexing.VideoScanner") as MockScanner:
        MockScanner.return_value.out_width = w
        MockScanner.return_value.out_height = h
        with patch("src.video.indexing.SceneSegmenter") as MockSegmenter:
            MockSegmenter.return_value.iter_scenes.return_value = mock_yields
            with patch("src.video.indexing.extract_high_res_frame") as mock_extract:
                mock_extract.return_value = (minimal_jpeg, showinfo_line)
                run_video_scene_indexing(
                    asset_id, video_path, "vid-int-dedup", video_repo, vision_analyzer=analyzer
                )

    session = _session_factory()
    try:
        rows = session.execute(
            text(
                "SELECT description, metadata FROM video_scenes WHERE asset_id = :aid ORDER BY start_ts"
            ),
            {"aid": asset_id},
        ).fetchall()
        assert len(rows) == 2
        assert rows[0][0] == desc1
        assert rows[1][0] == desc2
        meta2 = rows[1][1]
        assert meta2 is not None
        assert meta2.get("semantic_duplicate") is True
    finally:
        session.close()


def test_indexing_raises_when_truncated(engine, _session_factory, tmp_path):
    """When segmenter yields scenes but max_end_ts is short of video duration, run_video_scene_indexing raises ValueError."""
    asset_repo, video_repo = _create_tables_and_seed(engine, _session_factory)
    asset_id = _ensure_library_and_asset(_session_factory, "vid-int-truncated")
    w, h = 480, 270
    frame_size = w * h * 3
    fake_frame = b"\x80" * frame_size

    # Scene ends at 9.0s; video duration is 60.0s -> truncated
    scene1 = SceneResult(
        best_frame_bytes=fake_frame,
        best_pts=5.0,
        scene_start_pts=0.0,
        scene_end_pts=9.0,
        keep_reason=SceneKeepReason.forced,
        sharpness_score=10.0,
    )
    mock_yields = [(scene1, None)]

    video_path = tmp_path / "video.mp4"
    video_path.write_bytes(b"fake")
    mock_config = MagicMock()
    mock_config.data_dir = str(tmp_path)
    with patch("src.video.indexing.get_config", return_value=mock_config):
        with patch("src.video.indexing.probe_video_duration", return_value=60.0):
            with patch("src.video.indexing.VideoScanner") as MockScanner:
                MockScanner.return_value.out_width = w
                MockScanner.return_value.out_height = h
                with patch("src.video.indexing.SceneSegmenter") as MockSegmenter:
                    MockSegmenter.return_value.iter_scenes.return_value = mock_yields
                    with pytest.raises(ValueError, match="truncated") as exc_info:
                        run_video_scene_indexing(
                            asset_id, video_path, "vid-int-truncated", video_repo
                        )
    assert "indexed to 9.0" in str(exc_info.value)
    assert "duration is 60.0" in str(exc_info.value)


def test_indexing_raises_when_no_scenes_produced(engine, _session_factory, tmp_path):
    """When segmenter yields no scenes (e.g. no frames from decoder), run_video_scene_indexing raises ValueError."""
    asset_repo, video_repo = _create_tables_and_seed(engine, _session_factory)
    asset_id = _ensure_library_and_asset(_session_factory, "vid-int-zero")
    video_path = tmp_path / "video.mp4"
    video_path.write_bytes(b"x")

    mock_scanner = patch("src.video.indexing.VideoScanner")
    mock_segmenter = patch("src.video.indexing.SceneSegmenter")
    mock_yields = [(None, None)]  # one open-state update, no closed scene
    with mock_scanner as MockScanner, mock_segmenter as MockSegmenter:
        MockScanner.return_value.out_width = 480
        MockScanner.return_value.out_height = 270
        MockScanner.return_value.ffmpeg_repro_command.return_value = "ffmpeg -hide_banner ..."
        MockScanner.return_value.stderr_tail.return_value = ""
        MockSegmenter.return_value.iter_scenes.return_value = mock_yields
        with pytest.raises(ValueError, match="No frames produced by decoder"):
            run_video_scene_indexing(asset_id, video_path, "vid-int-zero", video_repo)


@pytest.mark.fast
def test_needs_ocr_returns_true_when_ocr_key_absent():
    """needs_ocr returns True when ocr_text key is absent, False when present (even if empty)."""
    scene_no_ocr = VideoSceneListItem(
        id=1, start_ts=0.0, end_ts=5.0, description="A scene",
        metadata={"moondream": {"description": "a", "tags": ["x"]}},
        sharpness_score=10.0, rep_frame_path="x.jpg", keep_reason="phash"
    )
    assert needs_ocr(scene_no_ocr) is True

    scene_with_empty_ocr = VideoSceneListItem(
        id=2, start_ts=0.0, end_ts=5.0, description="B scene",
        metadata={"moondream": {"description": "b", "tags": [], "ocr_text": ""}},
        sharpness_score=10.0, rep_frame_path="y.jpg", keep_reason="phash"
    )
    assert needs_ocr(scene_with_empty_ocr) is False

    scene_with_none_ocr = VideoSceneListItem(
        id=3, start_ts=0.0, end_ts=5.0, description="C scene",
        metadata={"moondream": {"description": "c", "tags": [], "ocr_text": None}},
        sharpness_score=10.0, rep_frame_path="z.jpg", keep_reason="phash"
    )
    assert needs_ocr(scene_with_none_ocr) is False


def test_run_vision_on_scenes_strict_merge_full_preserves_light_tags(
    engine, _session_factory, tmp_path
):
    """Strict merge (Full): scene with Light tags gets OCR added; tags and description preserved."""
    _, video_repo = _create_tables_and_seed(engine, _session_factory)
    asset_id = _ensure_library_and_asset(_session_factory, "vid-int-merge-full")
    slug = "vid-int-merge-full"
    rep_path_rel = f"video_scenes/{slug}/{asset_id}/0.000_5.000.jpg"
    rep_dir = tmp_path / "video_scenes" / slug / str(asset_id)
    rep_dir.mkdir(parents=True)
    (tmp_path / rep_path_rel).write_bytes(SOI + b"x" + EOI)

    scene_id = video_repo.save_scene_and_update_state(
        asset_id,
        VideoSceneRow(
            start_ts=0.0,
            end_ts=5.0,
            description="A person in a room",
            metadata={"moondream": {"description": "A person in a room", "tags": ["indoor", "person"]}},
            sharpness_score=10.0,
            rep_frame_path=rep_path_rel,
            keep_reason="phash",
        ),
        None,
    )

    mock_analysis = VisualAnalysis(
        description="A person in a room",
        tags=["indoor", "person"],
        ocr_text="HELLO WORLD",
    )
    mock_analyzer = MagicMock()
    mock_analyzer.analyze_image.return_value = mock_analysis

    mock_config = MagicMock()
    mock_config.data_dir = str(tmp_path)
    with patch("src.video.indexing.get_config", return_value=mock_config):
        run_vision_on_scenes(
            asset_id,
            slug,
            video_repo,
            mock_analyzer,
            effective_model_id=1,
            mode="full",
            asset_analysis_model_id=1,
        )

    scenes = video_repo.list_scenes(asset_id)
    assert len(scenes) == 1
    meta = scenes[0].metadata
    assert meta is not None
    md = meta.get("moondream", {})
    assert md.get("description") == "A person in a room"
    assert md.get("tags") == ["indoor", "person"]
    assert md.get("ocr_text") == "HELLO WORLD"


def test_run_vision_on_scenes_strict_merge_light_preserves_showinfo(
    engine, _session_factory, tmp_path
):
    """Strict merge (Light): scene with existing showinfo preserves it when adding moondream."""
    _, video_repo = _create_tables_and_seed(engine, _session_factory)
    asset_id = _ensure_library_and_asset(_session_factory, "vid-int-merge-light")
    slug = "vid-int-merge-light"
    rep_path_rel = f"video_scenes/{slug}/{asset_id}/0.000_5.000.jpg"
    rep_dir = tmp_path / "video_scenes" / slug / str(asset_id)
    rep_dir.mkdir(parents=True)
    (tmp_path / rep_path_rel).write_bytes(SOI + b"x" + EOI)

    video_repo.save_scene_and_update_state(
        asset_id,
        VideoSceneRow(
            start_ts=0.0,
            end_ts=5.0,
            description=None,
            metadata={"showinfo": "pts_time:1.0 n:30"},
            sharpness_score=10.0,
            rep_frame_path=rep_path_rel,
            keep_reason="phash",
        ),
        None,
    )

    mock_analysis = VisualAnalysis(
        description="A room",
        tags=["indoor"],
        ocr_text=None,
    )
    mock_analyzer = MagicMock()
    mock_analyzer.analyze_image.return_value = mock_analysis

    mock_config = MagicMock()
    mock_config.data_dir = str(tmp_path)
    with patch("src.video.indexing.get_config", return_value=mock_config):
        run_vision_on_scenes(
            asset_id,
            slug,
            video_repo,
            mock_analyzer,
            effective_model_id=1,
            mode="light",
        )

    scenes = video_repo.list_scenes(asset_id)
    assert len(scenes) == 1
    meta = scenes[0].metadata
    assert meta is not None
    assert meta.get("showinfo") == "pts_time:1.0 n:30"
    assert "moondream" in meta


def test_run_vision_on_scenes_model_mismatch_full_replace(engine, _session_factory, tmp_path):
    """When asset_analysis_model_id != effective_model_id, full mode does full replace (no merge)."""
    _, video_repo = _create_tables_and_seed(engine, _session_factory)
    asset_id = _ensure_library_and_asset(_session_factory, "vid-int-model-mismatch")
    slug = "vid-int-model-mismatch"
    rep_path_rel = f"video_scenes/{slug}/{asset_id}/0.000_5.000.jpg"
    rep_dir = tmp_path / "video_scenes" / slug / str(asset_id)
    rep_dir.mkdir(parents=True)
    (tmp_path / rep_path_rel).write_bytes(SOI + b"x" + EOI)

    video_repo.save_scene_and_update_state(
        asset_id,
        VideoSceneRow(
            start_ts=0.0,
            end_ts=5.0,
            description="Old model description",
            metadata={
                "moondream": {
                    "description": "Old model description",
                    "tags": ["old"],
                },
            },
            sharpness_score=10.0,
            rep_frame_path=rep_path_rel,
            keep_reason="phash",
        ),
        None,
    )

    mock_analysis = VisualAnalysis(
        description="New model description",
        tags=["new", "indoor"],
        ocr_text="NEW OCR",
    )
    mock_analyzer = MagicMock()
    mock_analyzer.analyze_image.return_value = mock_analysis

    mock_config = MagicMock()
    mock_config.data_dir = str(tmp_path)
    with patch("src.video.indexing.get_config", return_value=mock_config):
        run_vision_on_scenes(
            asset_id,
            slug,
            video_repo,
            mock_analyzer,
            effective_model_id=1,
            mode="full",
            asset_analysis_model_id=99,
        )

    mock_analyzer.analyze_image.assert_called_once()
    call_kwargs = mock_analyzer.analyze_image.call_args[1]
    assert call_kwargs.get("mode") == "light", "Model mismatch should force mode=light (full replace)"

    scenes = video_repo.list_scenes(asset_id)
    assert len(scenes) == 1
    meta = scenes[0].metadata
    assert meta is not None
    md = meta.get("moondream", {})
    assert md.get("description") == "New model description"
    assert md.get("tags") == ["new", "indoor"]
    assert md.get("ocr_text") == "NEW OCR"
