CREATE EXTENSION IF NOT EXISTS pg_trgm;

CREATE TABLE IF NOT EXISTS release (
    id BIGSERIAL PRIMARY KEY,
    norm_title TEXT UNIQUE,
    category TEXT,
    language TEXT NOT NULL DEFAULT 'und',
    tags TEXT NOT NULL DEFAULT '',
    source_group TEXT,
    size_bytes BIGINT,
    has_parts BOOLEAN NOT NULL DEFAULT FALSE,
    part_count INT NOT NULL DEFAULT 0
);

DROP INDEX IF EXISTS release_embedding_idx;
ALTER TABLE IF EXISTS release DROP COLUMN IF EXISTS embedding;

ALTER TABLE IF EXISTS release ADD COLUMN IF NOT EXISTS norm_title TEXT;
ALTER TABLE IF EXISTS release ADD COLUMN IF NOT EXISTS category TEXT;
ALTER TABLE IF EXISTS release ADD COLUMN IF NOT EXISTS language TEXT NOT NULL DEFAULT 'und';
ALTER TABLE IF EXISTS release ADD COLUMN IF NOT EXISTS tags TEXT NOT NULL DEFAULT '';
ALTER TABLE IF EXISTS release ADD COLUMN IF NOT EXISTS source_group TEXT;
ALTER TABLE IF EXISTS release ADD COLUMN IF NOT EXISTS size_bytes BIGINT;
ALTER TABLE IF EXISTS release ADD COLUMN IF NOT EXISTS has_parts BOOLEAN NOT NULL DEFAULT FALSE;
ALTER TABLE IF EXISTS release ADD COLUMN IF NOT EXISTS part_count INT NOT NULL DEFAULT 0;

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
CREATE INDEX IF NOT EXISTS release_size_bytes_idx ON release (size_bytes);

CREATE TABLE IF NOT EXISTS release_part (
    release_id BIGINT REFERENCES release(id) ON DELETE CASCADE,
    segment_number INT,
    message_id TEXT,
    group_name TEXT,
    size_bytes BIGINT,
    PRIMARY KEY (release_id, segment_number)
);

CREATE INDEX IF NOT EXISTS release_part_rel_seg_idx
    ON release_part (release_id, segment_number);

CREATE OR REPLACE FUNCTION update_release_part_stats()
RETURNS TRIGGER AS $$
BEGIN
    IF TG_OP = 'INSERT' THEN
        UPDATE release
        SET part_count = part_count + 1,
            size_bytes = COALESCE(size_bytes, 0) + NEW.size_bytes,
            has_parts = TRUE
        WHERE id = NEW.release_id;
        RETURN NEW;
    ELSIF TG_OP = 'DELETE' THEN
        UPDATE release
        SET part_count = GREATEST(part_count - 1, 0),
            size_bytes = GREATEST(COALESCE(size_bytes, 0) - OLD.size_bytes, 0),
            has_parts = (part_count - 1) > 0
        WHERE id = OLD.release_id;
        RETURN OLD;
    END IF;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER release_part_stats_trg
AFTER INSERT OR DELETE ON release_part
FOR EACH ROW EXECUTE FUNCTION update_release_part_stats();
