-- KS-06: staging snapshot for the KCC drilling-intent daily importer.
--
-- Rollback (plain single-file migrations, no paired .down.sql):
--   DROP TABLE IF EXISTS ks_intent_raw_snapshot;
--
-- Same raw-first discipline as ks_well_raw_snapshot/ks_operator_raw_snapshot:
-- one immutable row per record captured during a run, scoped to that run.
--
-- NOTE (KS-06, unverified assumption): `kcc.ks.gov` is unreachable from
-- this development environment (see docs/ops/ks-06-kcc-intents-unverified.md).
-- The source's "Parse:" field list in the task doc does not include an
-- explicit per-record identifier (no intent/permit number column), so
-- `source_filename` is treated as the natural identity key — the
-- assumption is that each intent's own C-1/KSONA document has a distinct
-- filename. `reported_lease_or_well_name` is captured even though it
-- isn't in the task doc's literal field list, because match tier 2
-- ("exact operator license plus exact PLSS and well/lease name")
-- requires it — documented as an inferred-necessary addition.
CREATE TABLE IF NOT EXISTS ks_intent_raw_snapshot (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    ingestion_run_id uuid NOT NULL REFERENCES ks_ingestion_run(id) ON DELETE CASCADE,
    source_filename text,
    rejection_reason text,
    permit_date date,
    kcc_district text,
    county text,
    section smallint,
    township smallint,
    range smallint,
    range_direction text,
    operator_license text,
    source_document_url text,
    reported_lease_or_well_name text,
    -- Never populated: surface-owner names, contact people, addresses, or
    -- phone numbers from C-1/KSONA attachments. This importer never even
    -- fetches attachment contents (task doc §3.7 item 2 / KS-06's own
    -- explicit privacy requirement) — raw_data is the search-result row
    -- only, not the linked document's contents.
    raw_data jsonb NOT NULL,
    source_record_hash text NOT NULL,
    observed_at timestamptz NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT ks_intent_raw_snapshot_identity_check
        CHECK (source_filename IS NOT NULL OR rejection_reason IS NOT NULL)
);

CREATE INDEX IF NOT EXISTS ks_intent_raw_snapshot_run_idx ON ks_intent_raw_snapshot (ingestion_run_id);
CREATE INDEX IF NOT EXISTS ks_intent_raw_snapshot_filename_idx
    ON ks_intent_raw_snapshot (source_filename) WHERE source_filename IS NOT NULL;
