"""Project repository: create projects and manage project-asset associations."""

from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Callable

from sqlalchemy import text
from sqlalchemy.orm import Session

from src.models.entities import Asset, Library, Project


class ProjectRepository:
    """
    Database access for Project bins and their asset associations.
    """

    def __init__(self, session_factory: Callable[[], Session]) -> None:
        self._session_factory = session_factory

    @contextmanager
    def _session_scope(self, write: bool = False) -> Iterator[Session]:
        session = self._session_factory()
        try:
            yield session
            if write:
                session.commit()
        finally:
            session.close()

    def create_project(self, name: str, export_path: str | None = None) -> Project:
        """
        Create a new project row and return the Project.
        """
        with self._session_scope(write=True) as session:
            row = session.execute(
                text(
                    """
                    INSERT INTO project (name, export_path)
                    VALUES (:name, :export_path)
                    RETURNING id, name, created_at, export_path
                    """
                ),
                {"name": name, "export_path": export_path},
            ).fetchone()
        assert row is not None
        return Project(
            id=row[0],
            name=row[1],
            created_at=row[2],
            export_path=row[3],
        )

    def get_project(self, project_id: int) -> Project | None:
        """
        Return a single project by id, or None if not found.
        """
        with self._session_scope(write=False) as session:
            row = session.execute(
                text(
                    "SELECT id, name, created_at, export_path "
                    "FROM project WHERE id = :id"
                ),
                {"id": project_id},
            ).fetchone()
        if row is None:
            return None
        return Project(
            id=row[0],
            name=row[1],
            created_at=row[2],
            export_path=row[3],
        )

    def list_projects(self) -> list[Project]:
        """
        Return all projects ordered by created_at descending.
        """
        with self._session_scope(write=False) as session:
            rows = session.execute(
                text(
                    "SELECT id, name, created_at, export_path "
                    "FROM project ORDER BY created_at DESC, id DESC"
                )
            ).fetchall()
        return [
            Project(
                id=row[0],
                name=row[1],
                created_at=row[2],
                export_path=row[3],
            )
            for row in rows
        ]

    def add_asset_to_project(self, project_id: int, asset_id: int) -> None:
        """
        Associate an asset with a project. Idempotent if the pair already exists.
        """
        with self._session_scope(write=True) as session:
            session.execute(
                text(
                    """
                    INSERT INTO project_assets (project_id, asset_id)
                    VALUES (:project_id, :asset_id)
                    ON CONFLICT (project_id, asset_id) DO NOTHING
                    """
                ),
                {"project_id": project_id, "asset_id": asset_id},
            )

    def remove_asset_from_project(self, project_id: int, asset_id: int) -> None:
        """
        Remove the association between a project and an asset.
        """
        with self._session_scope(write=True) as session:
            session.execute(
                text(
                    "DELETE FROM project_assets "
                    "WHERE project_id = :project_id AND asset_id = :asset_id"
                ),
                {"project_id": project_id, "asset_id": asset_id},
            )

    def get_project_assets(self, project_id: int) -> list[str]:
        """
        Return absolute source paths for all assets in the project.

        Joins project_assets → asset → library and skips soft-deleted libraries.
        """
        with self._session_scope(write=False) as session:
            rows = session.execute(
                text(
                    """
                    SELECT l.absolute_path, a.rel_path
                    FROM project_assets pa
                    JOIN asset a ON pa.asset_id = a.id
                    JOIN library l ON a.library_id = l.slug
                    WHERE pa.project_id = :project_id
                      AND l.deleted_at IS NULL
                    """
                ),
                {"project_id": project_id},
            ).fetchall()
        paths: list[str] = []
        for absolute_root, rel_path in rows:
            if absolute_root is None or rel_path is None:
                continue
            paths.append(str(Path(absolute_root) / rel_path))
        return paths

