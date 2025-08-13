CREATE EXTENSION IF NOT EXISTS pg_trgm;

CREATE TABLE IF NOT EXISTS release (
    id SERIAL PRIMARY KEY,
    norm_title TEXT UNIQUE,
    category TEXT,
    language TEXT NOT NULL DEFAULT 'und',
    tags TEXT NOT NULL DEFAULT '',
    source_group TEXT
);

DROP INDEX IF EXISTS release_embedding_idx;
ALTER TABLE IF EXISTS release DROP COLUMN IF EXISTS embedding;

ALTER TABLE IF EXISTS release ADD COLUMN IF NOT EXISTS norm_title TEXT;
ALTER TABLE IF EXISTS release ADD COLUMN IF NOT EXISTS category TEXT;
ALTER TABLE IF EXISTS release ADD COLUMN IF NOT EXISTS language TEXT NOT NULL DEFAULT 'und';
ALTER TABLE IF EXISTS release ADD COLUMN IF NOT EXISTS tags TEXT NOT NULL DEFAULT '';
ALTER TABLE IF EXISTS release ADD COLUMN IF NOT EXISTS source_group TEXT;

UPDATE release SET language = 'und' WHERE language IS NULL;
UPDATE release SET tags = '' WHERE tags IS NULL;
ALTER TABLE IF EXISTS release ALTER COLUMN language SET DEFAULT 'und';
ALTER TABLE IF EXISTS release ALTER COLUMN tags SET DEFAULT '';
ALTER TABLE IF EXISTS release ALTER COLUMN language SET NOT NULL;
ALTER TABLE IF EXISTS release ALTER COLUMN tags SET NOT NULL;

CREATE INDEX IF NOT EXISTS release_category_idx ON release (category);
CREATE INDEX IF NOT EXISTS release_language_idx ON release (language);
CREATE INDEX IF NOT EXISTS release_tags_idx ON release USING GIN (tags gin_trgm_ops);
CREATE INDEX IF NOT EXISTS release_norm_title_idx ON release USING GIN (norm_title gin_trgm_ops);
CREATE INDEX IF NOT EXISTS release_source_group_idx ON release (source_group);
