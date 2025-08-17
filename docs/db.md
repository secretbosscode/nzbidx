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

## Migrate existing ``release`` table

Upgrades from versions without a partitioned ``release`` table require an extra
step before starting the ingest worker. Run the migration script to convert the
table and drop the old copy:

```bash
python scripts/migrate_release_partitions.py
```

The script renames the existing table, creates a partitioned replacement, copies
all rows and then removes the legacy table.
