CREATE TABLE IF NOT EXISTS usenet_group (
    id SERIAL PRIMARY KEY,
    name TEXT UNIQUE NOT NULL
);

CREATE TABLE IF NOT EXISTS poster (
    id SERIAL PRIMARY KEY,
    name TEXT UNIQUE NOT NULL
);

CREATE TABLE IF NOT EXISTS release (
    id SERIAL PRIMARY KEY,
    group_id INTEGER REFERENCES usenet_group(id),
    poster_id INTEGER REFERENCES poster(id),
    title TEXT NOT NULL,
    category TEXT,
    language TEXT,
    tags TEXT[]
);

ALTER TABLE IF EXISTS release ADD COLUMN IF NOT EXISTS title TEXT;
ALTER TABLE IF EXISTS release ADD COLUMN IF NOT EXISTS group_id INTEGER REFERENCES usenet_group(id);
ALTER TABLE IF EXISTS release ADD COLUMN IF NOT EXISTS poster_id INTEGER REFERENCES poster(id);
ALTER TABLE IF EXISTS release ADD COLUMN IF NOT EXISTS category TEXT;
ALTER TABLE IF EXISTS release ADD COLUMN IF NOT EXISTS language TEXT;
ALTER TABLE IF EXISTS release ADD COLUMN IF NOT EXISTS tags TEXT[];
ALTER TABLE IF EXISTS release
    ALTER COLUMN tags TYPE TEXT[]
    USING (
        CASE
            WHEN pg_typeof(tags) = 'text'::regtype THEN string_to_array(tags::text, ',')
            ELSE tags::TEXT[]
        END
    );
ALTER TABLE IF EXISTS release ADD COLUMN IF NOT EXISTS embedding vector(1536);

CREATE TABLE IF NOT EXISTS release_file (
    id SERIAL PRIMARY KEY,
    release_id INTEGER REFERENCES release(id),
    filename TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS release_group_id_idx ON release (group_id);
CREATE INDEX IF NOT EXISTS release_poster_id_idx ON release (poster_id);
CREATE INDEX IF NOT EXISTS release_category_idx ON release (category);
CREATE INDEX IF NOT EXISTS release_language_idx ON release (language);
CREATE INDEX IF NOT EXISTS release_tags_idx ON release USING GIN (tags);
CREATE INDEX IF NOT EXISTS release_title_idx ON release USING GIN (title gin_trgm_ops);
CREATE INDEX IF NOT EXISTS release_embedding_idx ON release USING ivfflat (embedding vector_l2_ops) WITH (lists = 100);
CREATE INDEX IF NOT EXISTS release_file_release_id_idx ON release_file (release_id);
