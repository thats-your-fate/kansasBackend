-- KS-10: canonical monthly (and annual-summary/starting-cumulative)
-- lease production. `lease_id` is nullable and every descriptive field is
-- denormalized onto this row directly — a production record is still
-- valuable and reportable even when its LEASE_KID doesn't (yet, or ever)
-- resolve to a materialized `ks_lease` row (KS-09 dependency notwithstanding,
-- production data shouldn't be silently dropped just because the lease
-- dimension is incomplete for that lease).
--
-- Correction-safe reruns (task doc acceptance criteria): identity is
-- `(lease_kid, product, year, month)` — re-ingesting the same
-- lease/product/period upserts in place rather than duplicating. "Preserve
-- previous versions or snapshot lineage" is satisfied by the permanent,
-- append-only `ks_lease_production_raw_snapshot` (KS-10 never deletes or
-- overwrites a raw snapshot row) — this canonical table always reflects
-- the latest observed value, the same "raw permanent, canonical current"
-- split already used by every other Kansas entity table in this repo.
--
-- Rollback (plain single-file migrations, no paired .down.sql):
--   DROP TABLE IF EXISTS ks_lease_production_month;
CREATE TABLE IF NOT EXISTS ks_lease_production_month (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    lease_id uuid REFERENCES ks_lease(id),
    lease_kid text NOT NULL,
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
    source_crs text NOT NULL DEFAULT 'NAD27',
    year integer NOT NULL,
    -- Raw KGS value: -1 | 0 | 1-12. Never discard or reinterpret — see
    -- `record_kind` below for how downstream queries must actually use it.
    month smallint NOT NULL,
    -- constants::ProductionRecordKind, derived via classify_production_month.
    -- Task doc: "Do not duplicate month 0 or -1 into monthly charts" — this
    -- column is how that's enforced: any monthly-chart query must filter
    -- `record_kind = 'monthly'` explicitly; 'yearly_total'/'starting_cumulative'
    -- rows are stored (never dropped) but are structurally distinguishable
    -- from real monthly facts, not just conventionally so.
    record_kind text NOT NULL,
    -- 'oil' | 'gas'. Units are implied and fixed by product, never mixed:
    -- oil is barrels, gas is MCF (task doc).
    product text NOT NULL,
    -- KGS's own "WELLS" column — explicitly documented by KGS as limited
    -- quality (task doc), same caveat as KS-09's contributing-well counts.
    -- Context only, never a division key.
    reported_contributing_wells integer,
    production_volume double precision NOT NULL,
    source_url text,
    source_ingestion_run_id uuid NOT NULL REFERENCES ks_ingestion_run(id),
    source_record_hash text NOT NULL,
    first_observed_at timestamptz NOT NULL DEFAULT now(),
    last_observed_at timestamptz NOT NULL DEFAULT now(),
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT ks_lease_production_month_identity_unique UNIQUE (lease_kid, product, year, month),
    CONSTRAINT ks_lease_production_month_product_check CHECK (product IN ('oil', 'gas')),
    CONSTRAINT ks_lease_production_month_record_kind_check CHECK (record_kind IN ('monthly', 'yearly_total', 'starting_cumulative')),
    CONSTRAINT ks_lease_production_month_month_check CHECK (month BETWEEN -1 AND 12)
);

CREATE INDEX IF NOT EXISTS ks_lease_production_month_lease_idx ON ks_lease_production_month (lease_id) WHERE lease_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS ks_lease_production_month_lease_kid_idx ON ks_lease_production_month (lease_kid);
CREATE INDEX IF NOT EXISTS ks_lease_production_month_period_idx ON ks_lease_production_month (product, year, month);
CREATE INDEX IF NOT EXISTS ks_lease_production_month_county_idx ON ks_lease_production_month (county, product, year);
CREATE INDEX IF NOT EXISTS ks_lease_production_month_field_idx ON ks_lease_production_month (field_name, product, year);
-- Fast path for "monthly chart" queries — a real, indexed subset rather
-- than relying on callers to remember the filter every time.
CREATE INDEX IF NOT EXISTS ks_lease_production_month_monthly_only_idx
    ON ks_lease_production_month (product, year, month) WHERE record_kind = 'monthly';
