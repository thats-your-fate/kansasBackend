-- KS-08: canonical Kansas field entity. Identity is the ArcGIS layer's
-- own `FIELD_NAME` (the Oil and Gas Nomenclature Committee's official
-- reported name) — exact string, never normalized-and-merged. The
-- active/abandoned .xlsx files carry no FIELD_KID at all (confirmed live
-- 2026-09-05), so they can only ever be joined onto a field by exact
-- FIELD_NAME match, which is also why the task doc's "use exact reported
-- names and explicit alias evidence" instruction is enforced structurally
-- here, not just as a convention.
--
-- Rollback (plain single-file migrations, no paired .down.sql):
--   DROP TABLE IF EXISTS ks_field_annual_production;  -- first (FK)
--   DROP TABLE IF EXISTS ks_field_source_alias;        -- first (FK)
--   DROP TABLE IF EXISTS ks_field;

CREATE TABLE IF NOT EXISTS ks_field (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    official_field_name text NOT NULL,
    -- Deterministic normalization only (case/whitespace/trailing
    -- punctuation) — same discipline as
    -- normalize::kcc_operators::normalize_company_name. Used for lookup
    -- convenience, never to decide two names are "the same field."
    normalized_search_name text NOT NULL,
    state text NOT NULL DEFAULT 'KS',
    active_status text NOT NULL,
    field_type text,
    -- Merged from ArcGIS's FORMATIONS and/or the .xlsx files' PROD_ZONES
    -- when either is present; NULL when neither source reports one for
    -- this field, never fabricated.
    producing_zones text,

    -- Source geometry preserved alongside a derived display geometry
    -- (same "additive, never destructive" pattern as ks_well's
    -- source_latitude/display_geometry_4326, task doc §3.3/§3.8).
    source_geometry_geojson jsonb,
    display_geometry geometry(MultiPolygon, 4326),
    -- Geometry validity check (task doc acceptance criteria) — computed
    -- via PostGIS ST_IsValid/ST_IsValidReason at materialize time.
    -- Invalid geometry is stored as-is with this flag set, never silently
    -- repaired (ST_MakeValid would alter KGS's own reported boundary).
    geometry_is_valid boolean,
    geometry_validity_reason text,

    field_kid text,
    approx_acres double precision,
    cumulative_oil double precision,
    cumulative_gas double precision,
    max_oil_wells integer,
    max_gas_wells integer,
    last_oil_production_date text,
    last_gas_production_date text,

    source_ingestion_run_id uuid NOT NULL REFERENCES ks_ingestion_run(id),
    source_record_hash text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT ks_field_name_unique UNIQUE (official_field_name),
    CONSTRAINT ks_field_active_status_check CHECK (active_status IN ('active', 'abandoned', 'unknown'))
);

CREATE INDEX IF NOT EXISTS ks_field_normalized_name_idx ON ks_field (normalized_search_name);
CREATE INDEX IF NOT EXISTS ks_field_active_status_idx ON ks_field (active_status);
CREATE INDEX IF NOT EXISTS ks_field_display_geometry_idx ON ks_field USING GIST (display_geometry);

-- Source-alias table (task doc: "source aliases") — mirrors
-- ks_well_source_alias's shape/rationale exactly: FIELD_KID is not
-- documented as permanent by KGS any more than KID/LEASE_KID are, so it
-- lives here as an alias, never as this table's primary key.
CREATE TABLE IF NOT EXISTS ks_field_source_alias (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    field_id uuid NOT NULL REFERENCES ks_field(id) ON DELETE CASCADE,
    alias_type text NOT NULL,
    alias_value text NOT NULL,
    first_seen_at timestamptz NOT NULL DEFAULT now(),
    last_seen_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT ks_field_source_alias_type_check CHECK (alias_type IN ('field_kid')),
    CONSTRAINT ks_field_source_alias_unique UNIQUE (field_id, alias_type, alias_value)
);

CREATE INDEX IF NOT EXISTS ks_field_source_alias_lookup_idx ON ks_field_source_alias (alias_type, alias_value);

-- Per-year field-level production, from the active-file .xlsx sources.
-- `oil_wells`/`gas_wells` are the field file's OWN reported contributing-
-- well counts for that year — kept deliberately separate from
-- `SELECT count(*) FROM ks_well WHERE field_name = ...` so the two can be
-- reconciled against each other (task doc acceptance criteria: "field-
-- level production can be reconciled separately from well counts"),
-- exactly the same discipline as lease production's own contributing-
-- well-count column (task doc §3.4).
CREATE TABLE IF NOT EXISTS ks_field_annual_production (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    field_id uuid NOT NULL REFERENCES ks_field(id) ON DELETE CASCADE,
    year integer NOT NULL,
    oil_production double precision,
    oil_wells integer,
    gas_production double precision,
    gas_wells integer,
    source_ingestion_run_id uuid NOT NULL REFERENCES ks_ingestion_run(id),
    created_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT ks_field_annual_production_unique UNIQUE (field_id, year)
);

CREATE INDEX IF NOT EXISTS ks_field_annual_production_field_idx ON ks_field_annual_production (field_id);
