-- KS-05: staging snapshot for the KCC operator-license importer.
--
-- Rollback (plain single-file migrations, no paired .down.sql):
--   DROP TABLE IF EXISTS ks_operator_raw_snapshot;
--
-- Mirrors ks_well_raw_snapshot's (KS-03) shape and guarantees: one
-- immutable row per raw record captured during a run, scoped to that run,
-- never read by a materializer unless the run has reached `completed`.
--
-- NOTE (KS-05, unverified assumption): `kcc.ks.gov` is unreachable from
-- this development environment (confirmed via curl/nslookup/WebFetch, see
-- task doc §11's KS-05 entry) — the exact CSV/export column layout below
-- is a best-effort assumption based on the source's documented behavior
-- ("continuously updated", "active-only and all-operator downloads"), not
-- a confirmed live schema. raw_data is stored as a jsonb blob precisely so
-- a real-schema surprise doesn't require a migration to absorb.
CREATE TABLE IF NOT EXISTS ks_operator_raw_snapshot (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    ingestion_run_id uuid NOT NULL REFERENCES ks_ingestion_run(id) ON DELETE CASCADE,
    -- Which of KCC's two documented downloads this record came from.
    -- 'all' is expected to be a superset of 'active'; the reconciliation
    -- report (materialize::kcc_operators) checks that expectation rather
    -- than assuming it holds.
    list_source text NOT NULL,
    -- Nullable: a raw row with no parseable license number can't be
    -- resolved to an identity and is retained here with a reason, never
    -- silently dropped (same discipline as ks_well_raw_snapshot's
    -- kansas_membership_reason).
    kcc_license_number text,
    rejection_reason text,
    -- Pulled out of raw_data at parse time (src/parse/kcc_operators/csv_source.rs)
    -- using its own header-matching, rather than re-guessed from the jsonb
    -- blob by the materializer — same "extract once, near the parser"
    -- pattern as ks_well_raw_snapshot.source_kid alongside raw_data.
    reported_company_name text,
    source_city text,
    source_updated_at timestamptz,
    raw_data jsonb NOT NULL,
    -- Per-record content hash (KS-05's `source_record_hash`), computed
    -- over raw_data — lets the materializer detect an unchanged record
    -- without re-comparing every field.
    source_record_hash text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT ks_operator_raw_snapshot_list_source_check CHECK (list_source IN ('active', 'all')),
    CONSTRAINT ks_operator_raw_snapshot_identity_check
        CHECK (kcc_license_number IS NOT NULL OR rejection_reason IS NOT NULL)
);

CREATE INDEX IF NOT EXISTS ks_operator_raw_snapshot_run_idx ON ks_operator_raw_snapshot (ingestion_run_id);
CREATE INDEX IF NOT EXISTS ks_operator_raw_snapshot_license_idx
    ON ks_operator_raw_snapshot (ingestion_run_id, kcc_license_number) WHERE kcc_license_number IS NOT NULL;
CREATE INDEX IF NOT EXISTS ks_operator_raw_snapshot_list_source_idx
    ON ks_operator_raw_snapshot (ingestion_run_id, list_source);
