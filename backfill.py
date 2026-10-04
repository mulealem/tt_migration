#!/usr/bin/env python3
"""
Fast backfill that works on partial state.

  1. Verify FTS column is gone (idempotent).
  2. Pre-compute license/associate counts and primary_business_code/desc in
     temp tables.
  3. Apply UPDATE in 50k id-range batches with per-batch COMMIT so we never
     hold a single huge transaction.
  4. Recreate the FTS column (generated STORED) and its GIN index.
  5. ANALYZE for fresh query-planner statistics.
"""
import os
import sys
import time

import psycopg2

DEFAULT_URL = "postgresql://postgres:qazwsx123!@localhost:5432/tintin"
STEP = 50_000


def log(msg: str) -> None:
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


def main() -> int:
    url = os.environ.get("DATABASE_URL") or DEFAULT_URL
    log(f"connecting {url.split('@')[-1]}")
    conn = psycopg2.connect(url, connect_timeout=30)
    conn.autocommit = False
    cur = conn.cursor()
    started = time.time()

    try:
        # 1. FTS may already be dropped; skip if missing.
        cur.execute("""
            SELECT EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_name='businesses' AND column_name='fts'
            )
        """)
        fts_exists = cur.fetchone()[0]
        if fts_exists:
            log("step 1: dropping FTS column + GIN index")
            cur.execute("DROP INDEX IF EXISTS businesses_fts_idx")
            cur.execute("ALTER TABLE businesses DROP COLUMN fts")
            conn.commit()
        else:
            log("step 1: FTS column already absent -- skipping")

        # 2a. License counts.
        log("step 2a: precomputing _lic_counts")
        cur.execute("""
            CREATE TEMP TABLE _lic_counts ON COMMIT DROP AS
            SELECT business_id, COUNT(*)::int AS cnt
            FROM business_licenses GROUP BY business_id
        """)
        cur.execute("CREATE INDEX ON _lic_counts (business_id)")
        cur.execute("ANALYZE _lic_counts")

        # 2b. Associate counts.
        log("step 2b: precomputing _asc_counts")
        cur.execute("""
            CREATE TEMP TABLE _asc_counts ON COMMIT DROP AS
            SELECT business_id, COUNT(*)::int AS cnt
            FROM associates GROUP BY business_id
        """)
        cur.execute("CREATE INDEX ON _asc_counts (business_id)")
        cur.execute("ANALYZE _asc_counts")

        # 2c. Primary business type.
        log("step 2c: precomputing _top_subgroup")
        cur.execute("""
            CREATE TEMP TABLE _top_subgroup ON COMMIT DROP AS
            SELECT business_id, code, description
              FROM (
                SELECT bl.business_id, ls.code, ls.description,
                       ROW_NUMBER() OVER (PARTITION BY bl.business_id
                                          ORDER BY COUNT(*) DESC, ls.code) AS rn
                  FROM business_licenses bl
                  JOIN license_subgroups ls ON ls.license_id = bl.id
                 GROUP BY bl.business_id, ls.code, ls.description
              ) t WHERE rn = 1
        """)
        cur.execute("CREATE INDEX ON _top_subgroup (business_id)")
        cur.execute("ANALYZE _top_subgroup")

        # 3. Batched UPDATE by id range.
        cur.execute("SELECT MIN(id), MAX(id) FROM businesses")
        min_id, max_id = cur.fetchone()
        log(f"step 3: batched UPDATE in {STEP:,}-row chunks (id range {min_id}..{max_id})")

        cur_id = min_id
        done = 0
        while cur_id <= max_id:
            end = min(cur_id + STEP - 1, max_id)
            # Apply license/associate counts to businesses in this id range.
            cur.execute("""
                UPDATE businesses b SET
                    licenses_count   = COALESCE(l.cnt, 0),
                    associates_count = COALESCE(a.cnt, 0)
                FROM _lic_counts l
                LEFT JOIN _asc_counts a USING (business_id)
                WHERE b.id = l.business_id
                  AND b.id BETWEEN %s AND %s
            """, (cur_id, end))
            # Apply primary_business_code/description.
            cur.execute("""
                UPDATE businesses b SET
                    primary_business_code        = t.code,
                    primary_business_description = t.description
                FROM _top_subgroup t
                WHERE b.id = t.business_id
                  AND b.id BETWEEN %s AND %s
            """, (cur_id, end))
            conn.commit()
            done += (end - cur_id + 1)
            if done % (STEP * 4) == 0 or done == (max_id - min_id + 1):
                log(f"  progress: {done:,} rows touched")
            cur_id = end + 1

        cur.execute("DROP TABLE _lic_counts")
        cur.execute("DROP TABLE _asc_counts")
        cur.execute("DROP TABLE _top_subgroup")
        conn.commit()

        # 4. Recreate FTS column + GIN index.
        log("step 4: recreate FTS column + GIN index")
        cur.execute("""
            ALTER TABLE businesses ADD COLUMN fts tsvector
            GENERATED ALWAYS AS (
                setweight(to_tsvector('simple', coalesce(business_name, '')), 'A') ||
                setweight(to_tsvector('simple', coalesce(business_name_amh, '')), 'B') ||
                setweight(to_tsvector('simple', coalesce(tin, '')), 'A') ||
                setweight(to_tsvector('simple', coalesce(reg_no, '')), 'C')
            ) STORED
        """)
        cur.execute("CREATE INDEX businesses_fts_idx ON businesses USING GIN (fts)")

        # 5. ANALYZE.
        log("step 5: ANALYZE")
        cur.execute("ANALYZE businesses")
        cur.execute("ANALYZE business_licenses")
        cur.execute("ANALYZE license_subgroups")
        cur.execute("ANALYZE associates")
        conn.commit()

        cur.execute("""
            SELECT
                COUNT(*) FILTER (WHERE licenses_count > 0) AS with_licenses,
                COUNT(*) FILTER (WHERE associates_count > 0) AS with_assoc,
                COUNT(*) FILTER (WHERE primary_business_code IS NOT NULL) AS with_type,
                COUNT(*) FILTER (WHERE fts IS NOT NULL) AS with_fts,
                SUM(licenses_count) AS total_licenses,
                SUM(associates_count) AS total_assoc
            FROM businesses
        """)
        log(f"final stats: {cur.fetchone()}")
        log(f"all done in {(time.time()-started)/60:.1f}min")
    except Exception as e:
        conn.rollback()
        log(f"FAILED: {e}")
        raise
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
