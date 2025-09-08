from __future__ import annotations

from sqlalchemy import (
    BigInteger,
    Boolean,
    Column,
    Integer,
    JSON,
    Text,
    TIMESTAMP,
    UniqueConstraint,
)
from sqlalchemy.orm import declarative_base

Base = declarative_base()


class Release(Base):
    __tablename__ = "release"
    __table_args__ = (
        UniqueConstraint("norm_title", "category_id", "posted_at"),
        {"postgresql_partition_by": "RANGE (category_id)"},
    )

    id = Column(BigInteger, primary_key=True)
    norm_title = Column(Text)
    category = Column(Text)
    category_id = Column(Integer)
    language = Column(Text, nullable=False, default="und")
    tags = Column(Text, nullable=False, default="")
    source_group = Column(Text)
    size_bytes = Column(BigInteger)
    posted_at = Column(TIMESTAMP(timezone=True))
    segments = Column(JSON)
    has_parts = Column(Boolean, nullable=False, default=False)
    part_count = Column(Integer, nullable=False, default=0)
