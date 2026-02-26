"""Sanity check: verify Postgres testcontainer and DB connection work."""

import pytest
from sqlalchemy import text

pytestmark = [pytest.mark.slow]


def test_db_connection(session):
    """Verify we can execute a query against the Postgres testcontainer."""
    result = session.execute(text("SELECT 1 AS one"))
    assert result.scalar() == 1
