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

DROP VIEW IF EXISTS release_with_parts;
CREATE VIEW release_with_parts AS
SELECT r.*,
       COALESCE(
           json_agg(
               json_build_object(
                   'segment_number', rp.segment_number,
                   'message_id', rp.message_id,
                   'group_name', rp.group_name,
                   'size_bytes', rp.size_bytes
               )
               ORDER BY rp.segment_number
           ) FILTER (WHERE rp.segment_number IS NOT NULL),
           '[]'::json
       ) AS parts
FROM release r
LEFT JOIN release_part rp ON rp.release_id = r.id
GROUP BY r.id;
