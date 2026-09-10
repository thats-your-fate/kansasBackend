-- KS-05: canonical KCC operator entity.
--
-- Rollback (plain single-file migrations, no paired .down.sql):
--   DROP TABLE IF EXISTS ks_operator;
--   (roll back 0009_ks_operator_alias.sql first — it REFERENCES this table)
--
-- Hard data-use control (task doc §3.7 / KS-05 acceptance criteria): this
-- table has NO street-address, phone, email, or contact-person column —
-- not "a column that the app happens not to select," an absent column.
-- KCC's own raw values for those fields (if any) live only in
-- ks_operator_raw_snapshot.raw_data, which no public-facing code path may
-- read (enforced by materialize::kcc_operators only ever projecting the
-- columns below out of raw_data, never the whole blob). source_city is
-- the one address-adjacent field KS-05 asked for, and only "if already
-- permitted by product policy" — no such policy decision has been made
-- yet, so it is stored here for lineage/a future decision but is not
-- read by any public-facing struct today (see
-- materialize::kcc_operators::PublicOperator, and its
-- privacy::assert_no_forbidden_fields-backed test).
CREATE TABLE IF NOT EXISTS ks_operator (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    -- Identity per KS-05: exact lookup by KCC license, never fuzzy.
    kcc_license_number text NOT NULL,
    -- Most-recently-observed spelling, verbatim from the source (evidence,
    -- not a cleaned-up display name).
    reported_company_name text NOT NULL,
    -- Deterministic normalization only (case/whitespace/punctuation) --
    -- see normalize::kcc_operators::normalize_company_name. Never the
    -- product of fuzzy/similarity matching; that's exactly what KS-05
    -- says not to do ("preventing aggressive mergers of similarly named
    -- companies").
    normalized_company_name text NOT NULL,
    active_status text NOT NULL,
    -- KCC's own reported update timestamp for this record, if the source
    -- exposes one; NULL when it doesn't (not fabricated from retrieval time).
    source_updated_at timestamptz,
    source_city text,
    source_record_hash text NOT NULL,
    source_ingestion_run_id uuid NOT NULL REFERENCES ks_ingestion_run(id),
    first_seen_at timestamptz NOT NULL DEFAULT now(),
    last_seen_in_all_list_at timestamptz,
    last_seen_in_active_list_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT ks_operator_license_unique UNIQUE (kcc_license_number),
    CONSTRAINT ks_operator_active_status_check
        CHECK (active_status IN ('active', 'inactive', 'unknown'))
);

CREATE INDEX IF NOT EXISTS ks_operator_normalized_name_idx ON ks_operator (normalized_company_name);
CREATE INDEX IF NOT EXISTS ks_operator_active_status_idx ON ks_operator (active_status);
