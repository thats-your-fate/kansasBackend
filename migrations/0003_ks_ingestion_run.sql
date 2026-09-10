-- KS-02: ingestion-run lineage. One row per attempt to fetch+process a
-- ks_source. No canonical entity tables exist yet (KS-005+ add those) —
-- this schema exists so lineage/idempotency/policy-enforcement can be
-- built and tested now, ahead of any parser that will use it.
--
-- Rollback (plain single-file migrations, no paired .down.sql):
--   DROP TABLE IF EXISTS ks_source_rejection;
--   DROP TABLE IF EXISTS ks_ingestion_run;
-- Run before rolling back 0002_ks_source_registry.sql, since both
-- reference ks_source.
--
-- Idempotency: `ks_ingestion_run_source_checksum_unique` is the mechanism
-- behind "identical snapshots are idempotent" — a caller must check for an
-- existing `completed` run with the same (source_id, payload_checksum)
-- before doing real parse/materialize work (see
-- src/source/ingestion_run.rs::find_completed_run_by_checksum).
--
-- Partial-replace protection: a run's `status` only reaches `completed`
-- via `running`, never directly from `pending` (enforced in
-- src/source/ingestion_run.rs, not just documented) — "failed runs cannot
-- partially replace the published dataset" means future materializers
-- (KS-005+) must gate any canonical-table write on
-- `last_successful_ingestion_run_id` pointing at a `completed` run, never
-- a `failed`/`running` one. That FK doesn't exist yet because the
-- canonical tables don't exist yet; recorded here so KS-005 wires it up
-- rather than re-deriving the requirement.

CREATE TABLE IF NOT EXISTS ks_ingestion_run (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    source_id uuid NOT NULL REFERENCES ks_source(id),
    source_url text NOT NULL,
    retrieved_at timestamptz NOT NULL,
    source_modified_at timestamptz,
    payload_checksum text NOT NULL,
    schema_fingerprint text,
    row_count bigint,
    rejected_row_count bigint NOT NULL DEFAULT 0,
    status text NOT NULL DEFAULT 'pending',
    coverage_start date,
    coverage_end date,
    parser_version text NOT NULL,
    failure_message text,
    started_at timestamptz NOT NULL DEFAULT now(),
    completed_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT ks_ingestion_run_source_checksum_unique UNIQUE (source_id, payload_checksum),
    CONSTRAINT ks_ingestion_run_status_check
        CHECK (status IN ('pending', 'running', 'completed', 'failed', 'rejected')),
    CONSTRAINT ks_ingestion_run_coverage_order_check
        CHECK (coverage_start IS NULL OR coverage_end IS NULL OR coverage_start <= coverage_end),
    CONSTRAINT ks_ingestion_run_rejected_count_check CHECK (rejected_row_count >= 0)
);

CREATE INDEX IF NOT EXISTS ks_ingestion_run_source_started_idx
    ON ks_ingestion_run (source_id, started_at DESC);
CREATE INDEX IF NOT EXISTS ks_ingestion_run_status_idx
    ON ks_ingestion_run (status, started_at DESC);

CREATE TABLE IF NOT EXISTS ks_source_rejection (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    ingestion_run_id uuid NOT NULL REFERENCES ks_ingestion_run(id) ON DELETE CASCADE,
    row_number bigint,
    raw_data jsonb,
    reason text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS ks_source_rejection_run_idx
    ON ks_source_rejection (ingestion_run_id);
