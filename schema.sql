-- ===========================================================================
-- Business Directory - Database Schema
--
-- The Postgres instance (75.119.148.56:7239/postgres) already contains data
-- migrated from tin_data.db (tin_responses table) by an earlier ETL run:
--
--   businesses           - one row per company (TIN), ~2.7M rows
--   associates           - managers/owners of a business
--   business_licenses    - the Businesses[] array entries (trade licences)
--   license_subgroups    - SubGroups[] entries per licence
--
-- This file ONLY adds:
--   1. ENABLE pg_trgm (typo-tolerant search)
--   2. tsvector column + GIN index on businesses (full-text search)
--   3. helpful indexes (capital, reg_date, legal_condition, business_name ILIKE)
--   4. a view vw_company_public exposing a company with its primary business
--      description. Carefully designed to not be O(N) per row -- it uses a
--      LATERAL join that exploits the licenses/subgroups indexes and is only
--      computed when indexed by `businesses.id`.
--
-- The API uses raw SQL with prepared statements to JOIN associates and
-- business_licenses for full detail lookups (single-TIN endpoints).
-- ===========================================================================

CREATE EXTENSION IF NOT EXISTS pg_trgm;

-- ===========================================================================
-- Add FTS column + indexes to existing businesses table  (idempotent)
-- ===========================================================================

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

-- Manager-name search (api /search?q and manager= filter use ILIKE '%q%' on these)
CREATE INDEX IF NOT EXISTS associates_manager_eng_trgm ON associates USING GIN (manager_name_eng gin_trgm_ops);
CREATE INDEX IF NOT EXISTS associates_manager_amh_trgm ON associates USING GIN (manager_name_amh gin_trgm_ops);

-- ===========================================================================
-- View: a company joined with its dominant business description.
-- Uses LATERAL with LIMIT 1 so cost stays ~index-scan, not full count.
-- ===========================================================================

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

-- Denormalized columns on businesses for fast lookups (backfilled by db/backfill.py)
DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='businesses' AND column_name='licenses_count') THEN
    ALTER TABLE businesses ADD COLUMN licenses_count INTEGER NOT NULL DEFAULT 0;
  END IF;
  IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='businesses' AND column_name='associates_count') THEN
    ALTER TABLE businesses ADD COLUMN associates_count INTEGER NOT NULL DEFAULT 0;
  END IF;
  IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='businesses' AND column_name='primary_business_code') THEN
    ALTER TABLE businesses ADD COLUMN primary_business_code INTEGER;
  END IF;
  IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='businesses' AND column_name='primary_business_description') THEN
    ALTER TABLE businesses ADD COLUMN primary_business_description TEXT;
  END IF;
END $$;

CREATE INDEX IF NOT EXISTS businesses_primary_business_code_idx ON businesses (primary_business_code);
CREATE INDEX IF NOT EXISTS businesses_licenses_count_idx       ON businesses (licenses_count);
CREATE INDEX IF NOT EXISTS businesses_associates_count_idx     ON businesses (associates_count);

COMMENT ON VIEW vw_company_business_type IS
  'One row per company with its primary (most common) business subgroup code/desc. Indexed by businesses.id by lookup.';
