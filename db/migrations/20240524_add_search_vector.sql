-- Add generated search_vector column; existing rows are populated automatically
ALTER TABLE IF EXISTS release
    ADD COLUMN IF NOT EXISTS search_vector tsvector
    GENERATED ALWAYS AS (to_tsvector('simple', coalesce(norm_title,'') || ' ' || coalesce(tags,''))) STORED;

-- Build the index without locking writes
CREATE INDEX CONCURRENTLY IF NOT EXISTS release_search_idx ON release USING GIN (search_vector);
