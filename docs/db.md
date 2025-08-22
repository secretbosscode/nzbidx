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

## Event loop considerations

The API uses a global async SQLAlchemy engine that is bound to the event loop
where it was initialized. Reusing this engine across different event loops is
unsupported and will raise a `RuntimeError` when accessed. Each event loop
should invoke `init_engine()` before using the database.
