-- KS-09: staging snapshot for the KGS all-leases archive importer.
-- Confirmed live 2026-09-05 by downloading and inspecting the real
-- `all_kansas_leases.zip` (-> `all_kansas_leases.txt`, 98,639 real rows,
-- header: LEASE_KID,LEASE,DOR_CODE,API_NUMBER,OPERATOR,COUNTY,TOWNSHIP,
-- TWN_DIR,RANGE,RANGE_DIR,SECTION,SPOT,LATITUDE,LONGITUDE,PRODUCES,
-- PRODUCTION,YEAR_START,YEAR_STOP,URL).
--
-- Real, confirmed discrepancy worth recording: the task doc's field list
-- includes "field" (a field-name association) as something to store, but
-- the real file has no FIELD column at all — `field_name` below is
-- nullable and will always be NULL until/unless a different source
-- provides it. `kgs_lease_format.html`'s documented column list (PRODUCT,
-- WELLS, MONTH-YEAR, FIELD) describes the *monthly production* file
-- format, not this static full-extract file — the two share KGS's lease
-- vocabulary but are genuinely different files with different columns;
-- do not assume they match without checking a real download of each.
--
-- PRODUCES is confirmed live to sometimes literally be the string
-- "NULL" (2,359 of 98,639 real rows), not a blank/empty value —
-- `normalize::kgs_leases::derive_product` must treat that as `Unknown`,
-- never as a parse failure or a silently-guessed product.
--
-- Rollback (plain single-file migrations, no paired .down.sql):
--   DROP TABLE IF EXISTS ks_lease_raw_snapshot;
CREATE TABLE IF NOT EXISTS ks_lease_raw_snapshot (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    ingestion_run_id uuid NOT NULL REFERENCES ks_ingestion_run(id) ON DELETE CASCADE,
    lease_kid text,
    rejection_reason text,
    reported_lease_name text,
    dor_code text,
    -- Verbatim comma-separated list exactly as KGS reports it (e.g.
    -- "15-131-20143, 15-131-20089-0002, 15-131-20239") — parsed into
    -- individual API tokens at materialize time by
    -- `materialize::kgs_leases`, never split here, so the raw evidence
    -- stays byte-for-byte auditable.
    api_number_raw text,
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
    produces_raw text,
    production double precision,
    year_start integer,
    year_stop integer,
    source_url text,
    raw_data jsonb NOT NULL,
    source_record_hash text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT ks_lease_raw_snapshot_identity_check
        CHECK (lease_kid IS NOT NULL OR rejection_reason IS NOT NULL)
);

CREATE INDEX IF NOT EXISTS ks_lease_raw_snapshot_run_idx ON ks_lease_raw_snapshot (ingestion_run_id);
CREATE INDEX IF NOT EXISTS ks_lease_raw_snapshot_lease_kid_idx ON ks_lease_raw_snapshot (lease_kid) WHERE lease_kid IS NOT NULL;
