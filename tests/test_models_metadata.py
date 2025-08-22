from __future__ import annotations

import pytest

pytest.importorskip("sqlalchemy")
from sqlalchemy import UniqueConstraint, create_engine, inspect

from nzbidx_api.models import Base, Release


def test_release_model_matches_db() -> None:
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)

    insp = inspect(engine)
    columns = {c["name"] for c in insp.get_columns("release")}
    assert {"norm_title", "category_id"} <= columns

    meta_ucs = [
        c for c in Release.__table__.constraints if isinstance(c, UniqueConstraint)
    ]
    assert any(set(c.columns.keys()) == {"norm_title", "category_id"} for c in meta_ucs)

    db_ucs = insp.get_unique_constraints("release")
    assert {"norm_title", "category_id"} in [set(uc["column_names"]) for uc in db_ucs]
