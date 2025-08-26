CREATE EXTENSION IF NOT EXISTS pg_trgm;

CREATE TABLE IF NOT EXISTS release (
    id BIGSERIAL,
    norm_title TEXT,
    category TEXT,
    category_id INT,
    language TEXT NOT NULL DEFAULT 'und',
    tags TEXT NOT NULL DEFAULT '',
    source_group TEXT,
    size_bytes BIGINT,
    posted_at TIMESTAMPTZ,
    segments JSONB,
    has_parts BOOLEAN NOT NULL DEFAULT FALSE,
    part_count INT NOT NULL DEFAULT 0,
    UNIQUE (norm_title, category_id, posted_at)
) PARTITION BY RANGE (category_id);

CREATE TABLE IF NOT EXISTS release_movies PARTITION OF release
    FOR VALUES FROM (2000) TO (3000);
CREATE TABLE IF NOT EXISTS release_music PARTITION OF release
    FOR VALUES FROM (3000) TO (4000);
CREATE TABLE IF NOT EXISTS release_tv PARTITION OF release
    FOR VALUES FROM (5000) TO (6000);
CREATE TABLE IF NOT EXISTS release_adult PARTITION OF release
    FOR VALUES FROM (6000) TO (7000)
    PARTITION BY RANGE (posted_at);
CREATE TABLE IF NOT EXISTS release_adult_2024 PARTITION OF release_adult
    FOR VALUES FROM ('2024-01-01') TO ('2025-01-01');
CREATE TABLE IF NOT EXISTS release_adult_default PARTITION OF release_adult DEFAULT;
CREATE TABLE IF NOT EXISTS release_books PARTITION OF release
    FOR VALUES FROM (7000) TO (8000);
CREATE TABLE IF NOT EXISTS release_other PARTITION OF release DEFAULT;

DROP INDEX IF EXISTS release_embedding_idx;
ALTER TABLE IF EXISTS release DROP COLUMN IF EXISTS embedding;

ALTER TABLE IF EXISTS release ADD COLUMN IF NOT EXISTS norm_title TEXT;
ALTER TABLE IF EXISTS release ADD COLUMN IF NOT EXISTS category TEXT;
ALTER TABLE IF EXISTS release ADD COLUMN IF NOT EXISTS category_id INT;
ALTER TABLE IF EXISTS release ADD COLUMN IF NOT EXISTS language TEXT NOT NULL DEFAULT 'und';
ALTER TABLE IF EXISTS release ADD COLUMN IF NOT EXISTS tags TEXT NOT NULL DEFAULT '';
ALTER TABLE IF EXISTS release ADD COLUMN IF NOT EXISTS source_group TEXT;
ALTER TABLE IF EXISTS release ADD COLUMN IF NOT EXISTS size_bytes BIGINT;
ALTER TABLE IF EXISTS release ADD COLUMN IF NOT EXISTS posted_at TIMESTAMPTZ;
ALTER TABLE IF EXISTS release ADD COLUMN IF NOT EXISTS segments JSONB;
ALTER TABLE IF EXISTS release ADD COLUMN IF NOT EXISTS has_parts BOOLEAN NOT NULL DEFAULT FALSE;
ALTER TABLE IF EXISTS release ADD COLUMN IF NOT EXISTS part_count INT NOT NULL DEFAULT 0;
ALTER TABLE IF EXISTS release DROP CONSTRAINT IF EXISTS release_norm_title_key;
ALTER TABLE IF EXISTS release DROP CONSTRAINT IF EXISTS release_norm_title_category_id_key;
DO $$
BEGIN
    ALTER TABLE IF EXISTS release
        ADD CONSTRAINT release_norm_title_category_id_posted_at_key UNIQUE (norm_title, category_id, posted_at);
EXCEPTION
    WHEN duplicate_table THEN NULL;
END $$;

UPDATE release SET language = 'und' WHERE language IS NULL;
UPDATE release SET tags = '' WHERE tags IS NULL;
UPDATE release SET category_id = NULLIF(category, '')::INT WHERE category_id IS NULL AND category ~ '^[0-9]+$';
ALTER TABLE IF EXISTS release ALTER COLUMN language SET DEFAULT 'und';
ALTER TABLE IF EXISTS release ALTER COLUMN tags SET DEFAULT '';
ALTER TABLE IF EXISTS release ALTER COLUMN language SET NOT NULL;
ALTER TABLE IF EXISTS release ALTER COLUMN tags SET NOT NULL;

CREATE INDEX IF NOT EXISTS release_category_idx ON release (category);
CREATE INDEX IF NOT EXISTS release_category_id_idx ON release (category_id);
CREATE INDEX IF NOT EXISTS release_language_idx ON release (language);
CREATE INDEX IF NOT EXISTS release_tags_idx ON release USING GIN (tags gin_trgm_ops);
CREATE INDEX IF NOT EXISTS release_norm_title_idx ON release USING GIN (norm_title gin_trgm_ops);
CREATE INDEX IF NOT EXISTS release_source_group_idx ON release (source_group);
CREATE INDEX IF NOT EXISTS release_size_bytes_idx ON release (size_bytes);
CREATE INDEX IF NOT EXISTS release_posted_at_idx ON release (posted_at DESC);
CREATE UNIQUE INDEX IF NOT EXISTS release_norm_title_category_id_posted_at_key ON release (norm_title, category_id, posted_at);
