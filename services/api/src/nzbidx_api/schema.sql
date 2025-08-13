CREATE EXTENSION IF NOT EXISTS vector;
CREATE EXTENSION IF NOT EXISTS pg_trgm;

CREATE TABLE IF NOT EXISTS release (
    id SERIAL PRIMARY KEY,
    norm_title TEXT UNIQUE,
    category TEXT,
    language TEXT NOT NULL DEFAULT 'und',
    tags TEXT NOT NULL DEFAULT '',
    embedding vector(768)
);

ALTER TABLE IF EXISTS release ADD COLUMN IF NOT EXISTS norm_title TEXT;
ALTER TABLE IF EXISTS release ADD COLUMN IF NOT EXISTS category TEXT;
ALTER TABLE IF EXISTS release ADD COLUMN IF NOT EXISTS language TEXT NOT NULL DEFAULT 'und';
ALTER TABLE IF EXISTS release ADD COLUMN IF NOT EXISTS tags TEXT NOT NULL DEFAULT '';
ALTER TABLE IF EXISTS release ADD COLUMN IF NOT EXISTS embedding vector(768);

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
CREATE INDEX IF NOT EXISTS release_embedding_idx ON release USING ivfflat (embedding vector_l2_ops) WITH (lists = 100);
