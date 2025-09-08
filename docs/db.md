# Postgres

Manual steps to create the database for NZBidx.

When using the provided Docker setup, the database and required extensions are
provisioned automatically. The manual steps below apply only to custom
installations. The schema in `db/init/schema.sql` must be loaded before
starting the application so all tables and generated columns exist.

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

Run the schema file before starting the application to create tables and
indexes:

```bash
psql -U postgres -d nzbidx -f db/init/schema.sql
```

Verify that the `search_vector` column was created on the `release` table:

```bash
psql -U postgres -d nzbidx -c "\\d release"
```

`search_vector` should appear in the column list.

After the extensions are installed and the schema applied, the `nzbidx` role does
not require superuser privileges. The schema application routine automatically
revokes superuser rights from the role as a cleanup step, so subsequent
connections run as an ordinary user. Point `DATABASE_URL` at the database, e.g.
`postgres://nzbidx:nzbidx@localhost:5432/nzbidx`.

Existing deployments with an unpartitioned `release` table are migrated automatically when the application starts using a superuser `DATABASE_URL`; no manual script is required.

The application also pre-creates yearly `release_<category>` partitions for the
current and next calendar years on startup so upcoming releases have a
destination table without manual intervention.

## Partitioning release tables

Each `release_<category>` table must be partitioned by the `posted_at` column so
rows are stored in yearly partitions. If a category table is missing this
partitioning, the application logs `release_<cat>_table_not_partitioned` during
startup. Seeing this message means the table needs to be partitioned using one
of the procedures below.

### Using the partitioning script

1. Stop the application.
2. Export a superuser `DATABASE_URL` pointing at the database.
3. From the repository root, run:

   ```bash
   DATABASE_URL="postgres://nzbidx:nzbidx@localhost:5432/nzbidx" \
     ./scripts/partition_release_by_date.py
   ```

   The script creates yearly partitions on each `release_<category>` table and
   moves existing rows into the appropriate partition.
4. Restart the application.

### Dropping and recreating tables

If you prefer to recreate the tables instead of migrating data:

1. Connect as a superuser and drop the affected `release_<category>` table, for
   example:

   ```sql
   DROP TABLE release_movies CASCADE;
   ```

   Repeat for other categories as needed.
2. Reapply the schema to recreate the tables:

   ```bash
   psql -U postgres -d nzbidx -f db/init/schema.sql
   ```
3. Restart the application.

## Full-text search

The schema defines a `search_vector` column on the `release` table and a GIN
index for full-text search. Once the schema is applied, no additional migration
steps are required.

## Event loop considerations

The API uses a global async SQLAlchemy engine that is bound to the event loop
where it was initialized. Reusing this engine across different event loops is
unsupported and will raise a `RuntimeError` when accessed. Each event loop
should invoke `init_engine()` before using the database.
