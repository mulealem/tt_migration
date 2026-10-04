#!/usr/bin/env python3
"""
ETL: tin_data.db (SQLite) -> local PostgreSQL (tintin).

Reads rows from `tin_responses` WHERE status='completed', parses the JSON
response, and populates the normalized schema:

  businesses           - one row per company (TIN)
  associates           - managers / owners
  business_licenses    - the Businesses[] array entries (trade licences)
  license_subgroups    - SubGroups[] entries per licence

Features:
  * Streams in chunks (CHUNK_SIZE rows per round-trip)
  * Idempotent (INSERT ... ON CONFLICT DO NOTHING)
  * Resumable (state file `db/.etl_state` records last processed SQLite id)
  * Optional `--apply-schema` to bootstrap the schema first
  * Bounded memory, prints progress every 5_000 rows

Usage:
  python db/etl.py --apply-schema --full
  python db/etl.py --resume
  python db/etl.py --limit 1000    # for testing
  python db/etl.py --skip-fts      # don't run FTS DDL after load
"""
import argparse
import json
import os
import sqlite3
import sys
import time
from pathlib import Path
from typing import Any, Iterable

import psycopg2
import psycopg2.extras

HERE = Path(__file__).resolve().parent
DEFAULT_DB_URL = os.environ.get("DATABASE_URL") or "postgresql://postgres:qazwsx123!@localhost:5432/tintin"
SQLITE_PATH = Path("tin_data.db")
# Script-relative so etl.py runs from any working directory.
STATE_FILE = HERE / ".etl_state"
SCHEMA_FILE = HERE / "schema.sql"
CHUNK_SIZE = 1000
LOG_EVERY = 5_000


# ---------------------------------------------------------------------------
# Schema DDL (subset that must run BEFORE data load)
# ---------------------------------------------------------------------------
BOOTSTRAP_DDL = """
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE TABLE IF NOT EXISTS businesses (
    id                SERIAL PRIMARY KEY,
    tin               VARCHAR(20) NOT NULL UNIQUE,
    legal_condition   VARCHAR(10),
    reg_no            VARCHAR(100),
    reg_date          VARCHAR(20),
    business_name     VARCHAR(500) NOT NULL,
    business_name_amh TEXT,
    paid_up_capital   NUMERIC
);

CREATE TABLE IF NOT EXISTS associates (
    id               SERIAL PRIMARY KEY,
    business_id      INTEGER NOT NULL REFERENCES businesses(id) ON DELETE CASCADE,
    position         VARCHAR(100),
    manager_name_amh VARCHAR(255),
    manager_name_eng VARCHAR(255),
    mobile_phone     VARCHAR(50),
    regular_phone    VARCHAR(50)
);

CREATE TABLE IF NOT EXISTS business_licenses (
    id                   UUID PRIMARY KEY,
    business_id          INTEGER NOT NULL REFERENCES businesses(id) ON DELETE CASCADE,
    owner_tin            VARCHAR(20),
    date_registered      VARCHAR(20),
    trade_name_amh       TEXT,
    trade_name_eng       TEXT,
    licence_number       VARCHAR(100),
    renewal_date         VARCHAR(20),
    renewed_from         VARCHAR(20),
    renewed_to           VARCHAR(20),
    licensing_group_main VARCHAR(255)
);

CREATE TABLE IF NOT EXISTS license_subgroups (
    id          SERIAL PRIMARY KEY,
    license_id  UUID NOT NULL REFERENCES business_licenses(id) ON DELETE CASCADE,
    code        INTEGER NOT NULL,
    description TEXT
);
"""


POST_LOAD_DDL = """
CREATE EXTENSION IF NOT EXISTS pg_trgm;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_name='businesses' AND column_name='fts'
  ) THEN
    ALTER TABLE businesses
      ADD COLUMN fts tsvector
      GENERATED ALWAYS AS (
        setweight(to_tsvector('simple', coalesce(business_name, '')), 'A') ||
        setweight(to_tsvector('simple', coalesce(business_name_amh, '')), 'B') ||
        setweight(to_tsvector('simple', coalesce(tin, '')), 'A') ||
        setweight(to_tsvector('simple', coalesce(reg_no, '')), 'C')
      ) STORED;
  END IF;
END $$;

CREATE INDEX IF NOT EXISTS businesses_fts_idx     ON businesses USING GIN (fts);
CREATE INDEX IF NOT EXISTS businesses_capital_idx ON businesses (paid_up_capital DESC NULLS LAST);
CREATE INDEX IF NOT EXISTS businesses_regdate_idx ON businesses (reg_date);
CREATE INDEX IF NOT EXISTS businesses_legal_idx   ON businesses (legal_condition);
CREATE INDEX IF NOT EXISTS businesses_name_trgm   ON businesses USING GIN (business_name gin_trgm_ops);

CREATE INDEX IF NOT EXISTS associates_business_id_idx        ON associates (business_id);
CREATE INDEX IF NOT EXISTS business_licenses_business_id_idx ON business_licenses (business_id);
CREATE INDEX IF NOT EXISTS license_subgroups_license_id_idx  ON license_subgroups (license_id);
CREATE INDEX IF NOT EXISTS license_subgroups_code_idx        ON license_subgroups (code);

CREATE OR REPLACE VIEW vw_company_business_type AS
SELECT
  b.id            AS business_id,
  b.tin,
  sub.code        AS primary_business_code,
  sub.description AS primary_business_description,
  sub.subgroup_count
FROM businesses b
LEFT JOIN LATERAL (
  SELECT
    ls.code,
    ls.description,
    COUNT(*) AS subgroup_count
  FROM business_licenses bl
  JOIN license_subgroups ls ON ls.license_id = bl.id
  WHERE bl.business_id = b.id
  GROUP BY ls.code, ls.description
  ORDER BY COUNT(*) DESC, ls.code
  LIMIT 1
) sub ON TRUE;
"""


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def log(msg: str) -> None:
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


def apply_bootstrap_ddl(conn) -> None:
    log("Applying bootstrap DDL (creates tables if missing)...")
    with conn.cursor() as cur:
        cur.execute(BOOTSTRAP_DDL)
    conn.commit()


def apply_post_load_ddl(conn) -> None:
    log("Applying post-load DDL (FTS, indexes, view)...")
    with conn.cursor() as cur:
        cur.execute(POST_LOAD_DDL)
    conn.commit()


def get_sqlite_total(sq: sqlite3.Connection) -> int:
    cur = sq.execute("SELECT COUNT(*) FROM tin_responses WHERE status='completed'")
    return int(cur.fetchone()[0])


def get_sqlite_max_id(sq: sqlite3.Connection) -> int:
    cur = sq.execute("SELECT COALESCE(MAX(id), 0) FROM tin_responses WHERE status='completed'")
    return int(cur.fetchone()[0])


def fetch_chunk(sq: sqlite3.Connection, last_id: int, limit: int) -> list[tuple[int, str]]:
    cur = sq.execute(
        "SELECT id, response FROM tin_responses WHERE status='completed' AND id > ? ORDER BY id LIMIT ?",
        (last_id, limit),
    )
    return [(int(r[0]), r[1]) for r in cur.fetchall()]


def parse_row(record: dict[str, Any]):
    """Yield (company_tuple, [associates], [(license, [subgroups])])."""
    company = (
        record.get("Tin"),
        str(record.get("LegalCondtion") or "")[:10] or None,
        record.get("RegNo"),
        record.get("RegDate"),
        record.get("BusinessName") or "",
        record.get("BusinessNameAmh"),
        record.get("PaidUpCapital"),
    )

    assocs_raw = record.get("AssociateShortInfos") or []
    associates = []
    for a in assocs_raw:
        associates.append((
            a.get("Position"),
            a.get("ManagerName"),
            a.get("ManagerNameEng"),
            a.get("MobilePhone"),
            a.get("RegularPhone"),
        ))

    bizes_raw = record.get("Businesses") or []
    businesses = []
    for b in bizes_raw:
        main_guid = b.get("MainGuid")
        if not main_guid:
            continue
        license = (
            main_guid,
            record.get("Tin"),
            b.get("DateRegistered"),
            b.get("TradeNameAmh"),
            b.get("TradesName"),
            b.get("LicenceNumber"),
            b.get("RenewalDate"),
            b.get("RenewedFrom"),
            b.get("RenewedTo"),
            b.get("BusinessLicensingGroupMain"),
        )
        subs_raw = b.get("SubGroups") or []
        subgroups = [(s.get("Code"), s.get("Description")) for s in subs_raw if s.get("Code") is not None]
        businesses.append((license, subgroups))
    return company, associates, businesses


def load_state() -> int:
    if STATE_FILE.exists():
        try:
            return int(STATE_FILE.read_text().strip() or "0")
        except Exception:
            return 0
    return 0


def save_state(last_id: int) -> None:
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    STATE_FILE.write_text(str(last_id))


def process_chunk(conn, rows: list[tuple[int, str]], stats: dict[str, int]) -> None:
    """Insert a chunk of rows in a single transaction."""
    with conn.cursor() as cur:
        company_sql = """
        INSERT INTO businesses
            (tin, legal_condition, reg_no, reg_date, business_name, business_name_amh, paid_up_capital)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (tin) DO NOTHING
        RETURNING id, tin
        """
        # We need business_id for FK rows but INSERT ... ON CONFLICT doesn't
        # expose existing IDs. Workaround: pre-lookup using a temp table or
        # use INSERT ... RETURNING then fallback to SELECT for conflicts.
        # Simpler approach for bulk: use savepoints + SELECT for conflicts.
        # But for performance we batch with executemany + ON CONFLICT then
        # SELECT all TINs after to get IDs.

        tins = []
        parsed_rows = []
        for sqlite_id, raw in rows:
            try:
                rec = json.loads(raw)
            except Exception:
                stats["bad_json"] += 1
                continue
            tin = rec.get("Tin")
            if not tin or not isinstance(tin, str) or not tin.strip():
                stats["no_tin"] += 1
                continue
            name = rec.get("BusinessName")
            if not name or not isinstance(name, str) or not name.strip():
                # Still usable as a company but rare; allow only if tin present
                pass
            tins.append(tin)
            parsed_rows.append((sqlite_id, rec))

        if not tins:
            return

        # Upsert companies
        def scrub(v, max_len=None):
            if v is None:
                return None
            if isinstance(v, str):
                v = v.replace("\x00", "")
                if max_len:
                    v = v[:max_len]
            return v

        company_tuples = []
        for _, rec in parsed_rows:
            company_tuples.append((
                rec.get("Tin"),
                scrub(rec.get("LegalCondtion"), 10) or None,
                scrub(rec.get("RegNo"), 100) or None,
                scrub(rec.get("RegDate"), 20) or None,
                scrub(rec.get("BusinessName"), 500) or "",
                scrub(rec.get("BusinessNameAmh")) or None,
                rec.get("PaidUpCapital"),
            ))
        psycopg2.extras.execute_values(
            cur,
            """INSERT INTO businesses
                 (tin, legal_condition, reg_no, reg_date, business_name, business_name_amh, paid_up_capital)
               VALUES %s ON CONFLICT (tin) DO NOTHING""",
            company_tuples,
            page_size=CHUNK_SIZE,
        )

        # Resolve business_id for these TINs
        cur.execute(
            "SELECT id, tin FROM businesses WHERE tin = ANY(%s)",
            (tins,),
        )
        id_map = {r[1]: r[0] for r in cur.fetchall()}

        # Insert associates
        assoc_tuples = []
        for _, rec in parsed_rows:
            bid = id_map.get(rec.get("Tin"))
            if bid is None:
                continue
            for a in (rec.get("AssociateShortInfos") or []):
                if not isinstance(a, dict):
                    continue
                assoc_tuples.append((
                    bid,
                    scrub(a.get("Position"), 100) or None,
                    scrub(a.get("ManagerName"), 255) or None,
                    scrub(a.get("ManagerNameEng"), 255) or None,
                    scrub(a.get("MobilePhone"), 50) or None,
                    scrub(a.get("RegularPhone"), 50) or None,
                ))
        if assoc_tuples:
            psycopg2.extras.execute_values(
                cur,
                """INSERT INTO associates
                     (business_id, position, manager_name_amh, manager_name_eng, mobile_phone, regular_phone)
                   VALUES %s""",
                assoc_tuples,
                page_size=CHUNK_SIZE,
            )

        # Insert business_licenses + subgroups
        license_tuples = []
        sub_link_tuples = []
        for _, rec in parsed_rows:
            bid = id_map.get(rec.get("Tin"))
            if bid is None:
                continue
            owner_tin = rec.get("Tin")
            for b in (rec.get("Businesses") or []):
                if not isinstance(b, dict):
                    continue
                main_guid = b.get("MainGuid")
                if not main_guid or "\x00" in str(main_guid):
                    continue
                license_tuples.append((
                    scrub(main_guid, 50),
                    bid,
                    owner_tin,
                    scrub(b.get("DateRegistered"), 20) or None,
                    scrub(b.get("TradeNameAmh")) or None,
                    scrub(b.get("TradesName")) or None,
                    scrub(b.get("LicenceNumber"), 100) or None,
                    scrub(b.get("RenewalDate"), 20) or None,
                    scrub(b.get("RenewedFrom"), 20) or None,
                    scrub(b.get("RenewedTo"), 20) or None,
                    scrub(b.get("BusinessLicensingGroupMain"), 255) or None,
                ))
                for s in (b.get("SubGroups") or []):
                    if isinstance(s, dict) and s.get("Code") is not None:
                        sub_link_tuples.append((scrub(main_guid, 50), s.get("Code"), scrub(s.get("Description")) or ""))

        if license_tuples:
            psycopg2.extras.execute_values(
                cur,
                """INSERT INTO business_licenses
                     (id, business_id, owner_tin, date_registered, trade_name_amh,
                      trade_name_eng, licence_number, renewal_date, renewed_from,
                      renewed_to, licensing_group_main)
                   VALUES %s ON CONFLICT (id) DO NOTHING""",
                license_tuples,
                page_size=CHUNK_SIZE,
            )

        if sub_link_tuples:
            psycopg2.extras.execute_values(
                cur,
                """INSERT INTO license_subgroups (license_id, code, description) VALUES %s""",
                sub_link_tuples,
                page_size=CHUNK_SIZE,
            )

        stats["rows_processed"] += len(rows)
        stats["companies_inserted"] += len(tins)


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--apply-schema", action="store_true", help="Apply bootstrap DDL before load")
    p.add_argument("--skip-fts", action="store_true", help="Skip post-load DDL (FTS/indexes/view)")
    p.add_argument("--full", action="store_true", help="Run full load (ignore state file)")
    p.add_argument("--limit", type=int, default=0, help="Stop after N rows (test mode)")
    p.add_argument("--db-url", default=os.environ.get("DATABASE_URL") or DEFAULT_DB_URL)
    p.add_argument("--sqlite", default=str(SQLITE_PATH))
    args = p.parse_args()

    log(f"Connecting to Postgres: {args.db_url.split('@')[-1]}")
    pg = psycopg2.connect(args.db_url, connect_timeout=10)
    pg.autocommit = False

    log(f"Opening SQLite source: {args.sqlite}")
    sq = sqlite3.connect(args.sqlite)
    sq.row_factory = sqlite3.Row

    if args.apply_schema:
        apply_bootstrap_ddl(pg)

    total = get_sqlite_total(sq)
    max_id = get_sqlite_max_id(sq)
    log(f"SQLite source: {total:,} completed rows (max id={max_id})")

    last_id = 0 if args.full else load_state()
    if last_id:
        log(f"Resuming from state: last_id={last_id:,}")
    else:
        log("Starting from id=0")

    stats = {
        "rows_processed": 0,
        "companies_inserted": 0,
        "bad_json": 0,
        "no_tin": 0,
    }
    started = time.time()
    next_log = LOG_EVERY
    last_sqlite_id = last_id

    try:
        while True:
            chunk = fetch_chunk(sq, last_id, CHUNK_SIZE)
            if not chunk:
                break
            process_chunk(pg, chunk, stats)
            pg.commit()
            last_sqlite_id = chunk[-1][0]
            last_id = last_sqlite_id
            save_state(last_id)

            if stats["rows_processed"] >= next_log:
                elapsed = time.time() - started
                rate = stats["rows_processed"] / elapsed if elapsed else 0
                eta = (total - stats["rows_processed"]) / rate if rate else 0
                log(
                    f"  progress: {stats['rows_processed']:,}/{total:,} "
                    f"({stats['rows_processed']/total*100:.1f}%) "
                    f"rate={rate:.0f}/s eta={eta/60:.1f}min "
                    f"last_id={last_sqlite_id:,} bad_json={stats['bad_json']} no_tin={stats['no_tin']}"
                )
                next_log += LOG_EVERY

            if args.limit and stats["rows_processed"] >= args.limit:
                log(f"Hit --limit {args.limit}, stopping.")
                break

        log(
            f"Done: {stats['rows_processed']:,} rows processed, "
            f"{stats['companies_inserted']:,} companies inserted, "
            f"{stats['bad_json']:,} bad-json rows, "
            f"{stats['no_tin']:,} no-tin rows, "
            f"elapsed {(time.time()-started)/60:.1f}min"
        )
    except KeyboardInterrupt:
        log("Interrupted - state saved, run with --resume to continue.")
    finally:
        sq.close()
        pg.close()

    if not args.skip_fts:
        # Reopen connection for post-load DDL
        pg2 = psycopg2.connect(args.db_url, connect_timeout=10)
        try:
            apply_post_load_ddl(pg2)
            log("FTS/indexes/view applied.")
        finally:
            pg2.close()

    return 0


if __name__ == "__main__":
    sys.exit(main())
