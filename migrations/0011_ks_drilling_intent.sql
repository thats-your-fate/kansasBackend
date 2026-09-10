-- KS-06: canonical drilling-intent entity. Permanent — this is the whole
-- point of "store each daily source snapshot so the platform retains
-- records after they disappear from the 365-day window": once a row
-- lands here it is never deleted just because the source's own rolling
-- window later drops it.
--
-- Rollback (plain single-file migrations, no paired .down.sql):
--   DROP TABLE IF EXISTS ks_drilling_intent;

CREATE TABLE IF NOT EXISTS ks_drilling_intent (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    -- Natural identity (see 0010's note: the source exposes no explicit
    -- intent/permit number in the task doc's field list).
    source_filename text NOT NULL,
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

    -- Well-linking (task doc §3.1's SourceIdentityConfidence tiers,
    -- applied here per KS-06's own 4-tier order):
    --   exact_permit_or_api   -> tier 1
    --   license_plss_name     -> tier 2
    --   probabilistic         -> tier 3 (confidence required)
    --   unresolved            -> tier 4 (no candidate at all)
    matched_well_id uuid REFERENCES ks_well(internal_well_id),
    match_method text NOT NULL,
    match_confidence numeric,
    matched_fields jsonb,

    -- Event-adjacent timestamps (task doc: "Keep occurred_at, published_at
    -- when available, and observed_at as separate timestamps"). occurred_at
    -- is derived from permit_date (the closest confirmed "when this
    -- happened" signal); published_at is left NULL unless the source
    -- exposes a distinct posted-date (unconfirmed — see the unverified
    -- notice), never backfilled from occurred_at.
    occurred_at timestamptz,
    published_at timestamptz,
    first_observed_at timestamptz NOT NULL,
    last_observed_at timestamptz NOT NULL,

    source_ingestion_run_id uuid NOT NULL REFERENCES ks_ingestion_run(id),
    source_record_hash text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT ks_drilling_intent_source_filename_unique UNIQUE (source_filename),
    CONSTRAINT ks_drilling_intent_match_method_check
        CHECK (match_method IN ('exact_permit_or_api', 'license_plss_name', 'probabilistic', 'unresolved'))
);

CREATE INDEX IF NOT EXISTS ks_drilling_intent_well_idx ON ks_drilling_intent (matched_well_id) WHERE matched_well_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS ks_drilling_intent_unresolved_idx ON ks_drilling_intent (match_method) WHERE match_method = 'unresolved';
CREATE INDEX IF NOT EXISTS ks_drilling_intent_operator_license_idx ON ks_drilling_intent (operator_license);
