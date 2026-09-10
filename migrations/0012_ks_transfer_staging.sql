-- KS-07: staging snapshot for the KCC approved-operator-transfer daily
-- importer.
--
-- Rollback (plain single-file migrations, no paired .down.sql):
--   DROP TABLE IF EXISTS ks_transfer_raw_snapshot;
--
-- NOTE (KS-07, unverified assumption): same `kcc.ks.gov` unreachability
-- as KS-06 (see docs/ops/ks-07-kcc-transfers-unverified.md). "source
-- file" here is treated as the daily search-results page/file as a whole
-- (plural transfer rows per file), unlike KS-06's "source filename" which
-- is treated as a per-record document filename — the task doc's own
-- wording ("source file," singular, for a "search" results source)
-- supports this reading, but it is still an assumption.
CREATE TABLE IF NOT EXISTS ks_transfer_raw_snapshot (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    ingestion_run_id uuid NOT NULL REFERENCES ks_ingestion_run(id) ON DELETE CASCADE,
    source_file text NOT NULL,
    past_operator_name text,
    past_operator_license text,
    new_operator_name text,
    new_operator_license text,
    received_date date,
    effective_date date,
    approved_date date,
    lease_or_well_label text,
    section smallint,
    township smallint,
    range smallint,
    range_direction text,
    district text,
    county text,
    raw_data jsonb NOT NULL,
    source_record_hash text NOT NULL,
    observed_at timestamptz NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS ks_transfer_raw_snapshot_run_idx ON ks_transfer_raw_snapshot (ingestion_run_id);
