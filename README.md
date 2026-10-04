# Migration: SQLite → external Postgres

Loads `tin_data.db` (the 3.9 GB crawled SQLite file) into any Postgres 14+
database — Supabase, Neon, RDS, or your own — using plain Python. **No Docker,
no scp:** the script reads the SQLite file locally and writes over the network
to the provider, so you run it wherever the `.db` file already is.

## 1. Prereqs

```bash
pip install -r requirements.txt     # just psycopg2-binary
```

## 2. Get your connection string

From your provider's dashboard, copy the **direct** connection string and append
`?sslmode=require` if it isn't already there:

```
postgresql://USER:PASSWORD@HOST:5432/DBNAME?sslmode=require
```

> Use the **direct** connection for the migration, **not** a transaction-pooler
> endpoint (Supabase's port 6543 pooler, Neon's pooled `-pooler` host). Long
> batch sessions want a stable dedicated connection. You can point the backend
> at a pooler later if you like; the migration should not use one.

## 3. Run it (from this folder)

```bash
export DATABASE_URL="postgresql://USER:PASSWORD@HOST:5432/DBNAME?sslmode=require"

python etl.py --apply-schema --sqlite /path/to/tin_data.db
python backfill.py
```

Windows PowerShell instead of `export`:

```powershell
$env:DATABASE_URL = "postgresql://USER:PASSWORD@HOST:5432/DBNAME?sslmode=require"
python etl.py --apply-schema --sqlite C:\path\to\tin_data.db
python backfill.py
```

What each step does:

| Command | Does |
| --- | --- |
| `etl.py --apply-schema` | Creates all tables, streams the 2.84M crawl rows into `businesses` / `associates` / `business_licenses` / `license_subgroups`, then builds FTS + trigram indexes |
| `backfill.py` | Fills the denormalized columns the API sorts/filters on: `primary_business_code/description`, `licenses_count`, `associates_count` |

**Duration**: ~30–90 min with the DB in the same region as the machine you run
it from; longer over a WAN. It logs progress every 5,000 rows.

**Resumable**: interrupted? Just rerun the same command — it continues from
`.etl_state` in this folder. Delete `.etl_state` to start over.

## 4. Verify

```bash
python -c "
import os, psycopg2
con = psycopg2.connect(os.environ['DATABASE_URL'])
cur = con.cursor()
cur.execute('SELECT (SELECT count(*) FROM businesses), (SELECT count(*) FROM business_licenses), (SELECT count(*) FROM associates)')
print(cur.fetchone())"
```

Expect roughly `(2_731_578, 3_700_000+, 3_800_000+)`.

## Notes

- **Already migrated?** If your database predates manager-name search, apply the
  two trigram indexes once (schema.sql includes them for fresh runs):

  ```sql
  CREATE EXTENSION IF NOT EXISTS pg_trgm;
  CREATE INDEX IF NOT EXISTS associates_manager_eng_trgm ON associates USING GIN (manager_name_eng gin_trgm_ops);
  CREATE INDEX IF NOT EXISTS associates_manager_amh_trgm ON associates USING GIN (manager_name_amh gin_trgm_ops);
  ```

  Takes a few minutes on ~3.8M `associates` rows; safe to run while the API is up.
- **Provider storage**: expect **5–8 GB** of data + indexes. Free tiers (~0.5 GB)
  will not fit; the smallest paid tier with ~10 GB storage is the target.
- **Freshness**: after a new crawl, delete `.etl_state` and rerun `etl.py`.
  The ETL inserts/upserts rows from the file; it does not delete rows absent
  from the new crawl.
- **Why not pgloader?** It would recreate the SQLite-shaped schema, not the
  normalized one the API queries.
