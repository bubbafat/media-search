"""Fast tests for SimilarityScope and related models."""

import json

import pytest

from src.models.similarity import CameraSpec, DateRange, SimilarityScope

pytestmark = [pytest.mark.fast]


def test_similarity_scope_parses_valid_json():
    raw = {
        "library": "tuta",
        "asset_types": ["image"],
        "date_range": {"from_ts": 1000.0, "to_ts": 2000.0},
        "min_sharpness": 0.5,
        "has_face": True,
        "cameras": [{"make": "Canon", "model": "R5"}],
    }
    scope = SimilarityScope.model_validate(raw)
    assert scope.library == "tuta"
    assert scope.asset_types == ["image"]
    assert isinstance(scope.date_range, DateRange)
    assert scope.date_range.from_ts == 1000.0
    assert scope.date_range.to_ts == 2000.0
    assert scope.min_sharpness == 0.5
    assert scope.has_face is True
    assert scope.cameras is not None
    assert scope.cameras[0] == CameraSpec(make="Canon", model="R5")


def test_similarity_scope_uses_permissive_defaults_when_fields_absent():
    scope = SimilarityScope()
    assert scope.library == "all"
    assert scope.asset_types == "all"
    assert scope.date_range is None
    assert scope.min_sharpness is None
    assert scope.has_face is None
    assert scope.cameras is None


def test_similarity_scope_ignores_unknown_fields():
    raw = {"library": "tuta", "unknown_field": "ignored"}
    scope = SimilarityScope.model_validate(raw)
    assert scope.library == "tuta"
    assert not hasattr(scope, "unknown_field")

