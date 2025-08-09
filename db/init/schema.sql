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

ALTER TABLE IF EXISTS release ADD COLUMN IF NOT EXISTS category TEXT;
ALTER TABLE IF EXISTS release ADD COLUMN IF NOT EXISTS language TEXT;
ALTER TABLE IF EXISTS release ADD COLUMN IF NOT EXISTS tags TEXT[];

CREATE TABLE IF NOT EXISTS release_file (
    id SERIAL PRIMARY KEY,
    release_id INTEGER REFERENCES release(id),
    filename TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS file_segments (
    id SERIAL PRIMARY KEY,
    file_id INTEGER REFERENCES release_file(id),
    segment_number INTEGER,
    size INTEGER
);
