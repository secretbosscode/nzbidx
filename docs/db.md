# Postgres

Manual steps to create the database for NZBIdx.

## Prerequisites

* PostgreSQL 15+ with the [pgvector](https://github.com/pgvector/pgvector) extension installed on the server.
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
CREATE EXTENSION IF NOT EXISTS vector;
```

If the `vector` extension is missing, schema application fails with `type "vector" does not exist` when the `embedding` column is created.

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
