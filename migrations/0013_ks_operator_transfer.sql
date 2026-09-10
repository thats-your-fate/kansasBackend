-- KS-07: canonical approved-operator-transfer entity. Permanent — "the
-- KCC page exposes only the previous 90 days, making daily source
-- retention essential" (task doc). Immutable once written: a transfer
-- record is never rewritten to reflect a later re-interpretation, only
-- its well-links (below) can gain new candidate rows over time.
--
-- Rollback (plain single-file migrations, no paired .down.sql):
--   DROP TABLE IF EXISTS ks_operator_transfer_well;  -- first (FK)
--   DROP TABLE IF EXISTS ks_operator_transfer;

CREATE TABLE IF NOT EXISTS ks_operator_transfer (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    source_file text NOT NULL,
    past_operator_name text,
    past_operator_license text,
    new_operator_name text,
    new_operator_license text,
    -- Three genuinely distinct dates (task doc: "effective and approval
    -- dates remain distinct") — never collapsed into one "date" column.
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
    source_ingestion_run_id uuid NOT NULL REFERENCES ks_ingestion_run(id),
    source_record_hash text NOT NULL,
    first_observed_at timestamptz NOT NULL,
    last_observed_at timestamptz NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now()
);

-- Natural identity: the source exposes no explicit transfer ID either
-- (see 0012's note). A transfer is uniquely identified by the
-- combination of who transferred to whom, over what, effective when —
-- the daily-idempotent-import acceptance criterion depends on this being
-- stable across re-ingesting the same source content. A plain UNIQUE
-- constraint on these columns would NOT provide that: Postgres treats
-- NULL as distinct from NULL, so any transfer missing even one identity
-- field (a real possibility — county, a license, or the lease label
-- could all be blank on a given source row) would never conflict on
-- re-import and would duplicate every day instead of upserting. Wrapping
-- every column in COALESCE to a sentinel makes NULL a comparable value
-- like any other. `materialize::kcc_transfers` INSERTs must use the
-- exact same COALESCE expressions in its `ON CONFLICT` clause for
-- Postgres to recognize this index as the conflict target.
CREATE UNIQUE INDEX IF NOT EXISTS ks_operator_transfer_identity_idx ON ks_operator_transfer (
    (COALESCE(past_operator_license, '')),
    (COALESCE(new_operator_license, '')),
    (COALESCE(effective_date, '0001-01-01'::date)),
    (COALESCE(lease_or_well_label, '')),
    (COALESCE(county, ''))
);

CREATE INDEX IF NOT EXISTS ks_operator_transfer_new_license_idx ON ks_operator_transfer (new_operator_license);
CREATE INDEX IF NOT EXISTS ks_operator_transfer_past_license_idx ON ks_operator_transfer (past_operator_license);

-- One transfer may link to multiple wells (task doc acceptance criteria)
-- — a join table, not a single nullable FK on ks_operator_transfer.
-- `match_method = 'fuzzy_candidate'` rows are exactly the "fuzzy lease-name
-- matching may generate candidates but must not directly overwrite a
-- well's current operator" case: they are visible here for the internal
-- unmatched/candidate dashboard, but nothing in this codebase ever reads
-- a fuzzy_candidate row to justify writing to ks_well or any future
-- operator-history table.
CREATE TABLE IF NOT EXISTS ks_operator_transfer_well (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    transfer_id uuid NOT NULL REFERENCES ks_operator_transfer(id) ON DELETE CASCADE,
    well_id uuid NOT NULL REFERENCES ks_well(internal_well_id),
    match_method text NOT NULL,
    match_confidence numeric,
    matched_fields jsonb,
    created_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT ks_operator_transfer_well_unique UNIQUE (transfer_id, well_id),
    CONSTRAINT ks_operator_transfer_well_match_method_check
        CHECK (match_method IN ('kcc_dor_identifier', 'exact_plss', 'exact_lease_name', 'exact_operator_license', 'fuzzy_candidate'))
);

CREATE INDEX IF NOT EXISTS ks_operator_transfer_well_transfer_idx ON ks_operator_transfer_well (transfer_id);
CREATE INDEX IF NOT EXISTS ks_operator_transfer_well_well_idx ON ks_operator_transfer_well (well_id);
