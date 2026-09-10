-- KS-10: staging snapshot for the KGS monthly lease-production importer.
-- Confirmed live 2026-09-05 against the real decade archives
-- (`oil_leases_2020_present.zip` etc.) — real header:
-- LEASE_KID,LEASE,DOR_CODE,API_NUMBER,FIELD,PRODUCING_ZONE,OPERATOR,
-- COUNTY,TOWNSHIP,TWN_DIR,RANGE,RANGE_DIR,SECTION,SPOT,LATITUDE,
-- LONGITUDE,MONTH-YEAR,PRODUCT,WELLS,PRODUCTION,URL — this is genuinely a
-- different file from KS-09's all-leases archive (confirmed by direct
-- download of both), even though `KGS_LEASE_FORMAT`'s documentation page
-- describes *this* file, not that one (see KS-09's own item 53).
--
-- One run per decade-file-and-product combination (8 files total:
-- oil/gas x 2020-present/2010-2019/2000-2009/1990-1999) — each gets its
-- own `ks_ingestion_run` under the shared `KGS_PRODUCTION_OIL`/
-- `KGS_PRODUCTION_GAS` source codes (already seeded at KS-02), not one
-- run per file lumped together, so a bad file doesn't block the other
-- seven from publishing.
--
-- Rollback (plain single-file migrations, no paired .down.sql):
--   DROP TABLE IF EXISTS ks_lease_production_raw_snapshot;
CREATE TABLE IF NOT EXISTS ks_lease_production_raw_snapshot (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    ingestion_run_id uuid NOT NULL REFERENCES ks_ingestion_run(id) ON DELETE CASCADE,
    source_file text NOT NULL,
    lease_kid text,
    rejection_reason text,
    reported_lease_name text,
    dor_code text,
    api_number_raw text,
    field_name text,
    producing_zone text,
    operator text,
    county text,
    township smallint,
    township_direction text,
    range smallint,
    range_direction text,
    section smallint,
    spot text,
    source_latitude double precision,
    source_longitude double precision,
    month_year_raw text,
    year integer,
    month smallint,
    product_raw text,
    reported_contributing_wells integer,
    production_volume double precision,
    source_url text,
    raw_data jsonb NOT NULL,
    source_record_hash text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT ks_lease_production_raw_snapshot_identity_check
        CHECK (lease_kid IS NOT NULL OR rejection_reason IS NOT NULL)
);

CREATE INDEX IF NOT EXISTS ks_lease_production_raw_snapshot_lease_kid_idx ON ks_lease_production_raw_snapshot (lease_kid) WHERE lease_kid IS NOT NULL;
-- materialize::kgs_production pages through one run's rows via keyset
-- pagination (`WHERE ingestion_run_id = $1 AND id > $2 ORDER BY id LIMIT N`).
-- A real performance bug was found and fixed here before this migration
-- ever shipped: a plain `(ingestion_run_id)` index alone leaves Postgres
-- scanning + sorting every remaining row of a run on every single page
-- (effectively O(n^2) over an 11M-row real production run), rather than
-- a real per-page index range scan. This composite index (replacing what
-- would otherwise have been a separate single-column `ingestion_run_id`
-- index) is what makes each page O(log n + page_size) instead.
CREATE INDEX IF NOT EXISTS ks_lease_production_raw_snapshot_run_id_idx ON ks_lease_production_raw_snapshot (ingestion_run_id, id);
