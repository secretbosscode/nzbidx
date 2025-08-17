from __future__ import annotations

from sqlalchemy import (
    BigInteger,
    Boolean,
    Column,
    Integer,
    JSON,
    Text,
    TIMESTAMP,
)
from sqlalchemy.orm import declarative_base

Base = declarative_base()


class Release(Base):
    __tablename__ = "release"
    __table_args__ = {"postgresql_partition_by": "RANGE (category_id)"}

    id = Column(BigInteger, primary_key=True)
    norm_title = Column(Text, unique=True)
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


class ReleaseMovies(Release):
    __tablename__ = "release_movies"
    __mapper_args__ = {"polymorphic_identity": "movies", "concrete": True}


class ReleaseMusic(Release):
    __tablename__ = "release_music"
    __mapper_args__ = {"polymorphic_identity": "music", "concrete": True}


class ReleaseTV(Release):
    __tablename__ = "release_tv"
    __mapper_args__ = {"polymorphic_identity": "tv", "concrete": True}


class ReleaseAdult(Release):
    __tablename__ = "release_adult"
    __mapper_args__ = {"polymorphic_identity": "adult", "concrete": True}


class ReleaseBooks(Release):
    __tablename__ = "release_books"
    __mapper_args__ = {"polymorphic_identity": "books", "concrete": True}


class ReleaseOther(Release):
    __tablename__ = "release_other"
    __mapper_args__ = {"polymorphic_identity": "other", "concrete": True}
