# Postgres

Manual steps to create the database for NZBidx.

## Prerequisites

* PostgreSQL 15+.
* Superuser access to install extensions. Having permission to create databases alone is not enough.

## Create role and database

Connect as a superuser and run:

```sql
CREATE ROLE nzbidx LOGIN PASSWORD 'nzbidx';
CREATE DATABASE nzbidx OWNER nzbidx;
```

## Install extensions

Extensions must be installed by a superuser before the application starts:

```sql
\c nzbidx
CREATE EXTENSION IF NOT EXISTS pg_trgm;
```

## Apply schema

Load the schema file to create tables and indexes:

```bash
psql -U postgres -d nzbidx -f db/init/schema.sql
```

After the extensions are installed and the schema applied, the `nzbidx` role does
not require superuser privileges. The schema application routine automatically
revokes superuser rights from the role as a cleanup step, so subsequent
connections run as an ordinary user. Point `DATABASE_URL` at the database, e.g.
`postgres://nzbidx:nzbidx@localhost:5432/nzbidx`.

Existing deployments with an unpartitioned `release` table are migrated automatically when the application starts using a superuser `DATABASE_URL`; no manual script is required.

## Full-text search

After the base schema is applied, run the migration that adds the full-text
search vector. This step is required before search endpoints will function:

```bash
psql -U postgres -d nzbidx -f db/migrations/20240524_add_search_vector.sql
```

This migration must be executed by a superuser.

Verify the column and index were created:

```psql
\d release
SELECT to_regclass('release_search_idx');
```

The `\d` output should include `search_vector tsvector`, and the
`to_regclass` call returns `release_search_idx` when the index exists.

## Event loop considerations

The API uses a global async SQLAlchemy engine that is bound to the event loop
where it was initialized. Reusing this engine across different event loops is
unsupported and will raise a `RuntimeError` when accessed. Each event loop
should invoke `init_engine()` before using the database.
