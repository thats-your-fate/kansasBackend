-- KS-04: canonical Kansas well entity, materialized from a `completed`
-- ks_ingestion_run's `ks_well_raw_snapshot` rows (KS-03). See task doc
-- §6.1 for the full field-by-field rationale.
--
-- Rollback (plain single-file migrations, no paired .down.sql):
--   DROP TABLE IF EXISTS ks_well;  -- run after 0006's rollback (FK)

CREATE TABLE IF NOT EXISTS ks_well (
    internal_well_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    state_code text NOT NULL DEFAULT 'KS',

    -- Identity (task doc §3.1) — api14 is the preferred identity when
    -- valid; api_raw/api_base/api_suffix/api_validation_status per KS-04.
    api_raw text,
    api14 text,
    api_base text,
    api_suffix text,
    api_validation_status text NOT NULL,
    well_identity_match_method text NOT NULL,

    -- KGS KID and KCC permit are source aliases, not identity (§3.1) —
    -- kept here as plain denormalized fields for convenient display, but
    -- `ks_well_source_alias` (0006) is the authoritative alias record.
    kgs_kid text,
    kcc_permit text,

    field_name text,
    lease_name text,
    well_name text,

    -- Raw status fields, always preserved, never overwritten by the
    -- derived value (task doc §3.5).
    well_class_raw text,
    status_raw text,
    status2_raw text,
    lifecycle_status text NOT NULL,

    -- KGS's own operator fields — evidence, not an authoritative
    -- relationship (task doc §3.2). The authoritative, versioned
    -- relationship lives in ks_well_operator_history once KS-05/KS-08
    -- build it.
    operator_name_kgs_original text,
    operator_name_kgs_reported_current text,

    county text,
    township smallint,
    township_direction text,
    range smallint,
    range_direction text,
    section smallint,
    subdivision_1_largest text,
    subdivision_2 text,
    subdivision_3 text,
    subdivision_4_smallest text,
    spot text,
    feet_north_from_reference integer,
    feet_east_from_reference integer,
    reference_corner text,

    permit_date date,
    permit_date_raw text,
    spud_date date,
    spud_date_raw text,
    completion_date date,
    completion_date_raw text,
    plug_date date,
    plug_date_raw text,

    total_depth integer,
    producing_formation text,
    formation_at_total_depth text,
    ip_oil double precision,
    ip_gas double precision,
    ip_water double precision,
    oil_kid text,
    oil_dor_id text,
    gas_kid text,
    gas_dor_id text,
    comments text,

    -- Source coordinates preserved alongside a derived WGS84 display
    -- geometry (task doc §3.3) — additive, never a replacement.
    source_latitude double precision,
    source_longitude double precision,
    source_crs text NOT NULL DEFAULT 'NAD27',
    display_geometry_4326 geometry(Point, 4326),
    location_source text,
    location_confidence text,
    location_warning text,

    -- Horizontal badge, no geometry claim (task doc §3.6).
    horizontal_or_directional boolean NOT NULL DEFAULT false,
    trajectory_available boolean NOT NULL DEFAULT false,

    -- Exact source lineage, visible from every row (KS-04 acceptance
    -- criteria) — which run, which source format, which raw object.
    source_format text NOT NULL,
    source_ingestion_run_id uuid NOT NULL REFERENCES ks_ingestion_run(id),
    source_object_id bigint,

    -- Tombstone-after-two-consecutive-misses (KS-04 acceptance criteria):
    -- a well absent from one new full snapshot is not deleted; only after
    -- two consecutive absences is it tombstoned. Reappearing resets the
    -- counter and un-tombstones.
    consecutive_missing_snapshot_count integer NOT NULL DEFAULT 0,
    is_tombstoned boolean NOT NULL DEFAULT false,
    tombstoned_at timestamptz,

    first_seen_at timestamptz NOT NULL DEFAULT now(),
    last_seen_at timestamptz NOT NULL DEFAULT now(),
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),

    CONSTRAINT ks_well_api_validation_status_check
        CHECK (api_validation_status IN ('valid', 'valid_short_form', 'malformed', 'missing')),
    CONSTRAINT ks_well_identity_match_method_check
        CHECK (well_identity_match_method IN ('api14', 'kid_fallback')),
    CONSTRAINT ks_well_source_format_check CHECK (source_format IN ('arcgis', 'zip')),
    CONSTRAINT ks_well_missing_count_check CHECK (consecutive_missing_snapshot_count >= 0)
);

-- api14 is the preferred identity key when present; a well without one
-- (identity resolved by KID fallback) has no api14-based uniqueness
-- constraint to satisfy — KID-fallback uniqueness is enforced instead
-- via ks_well_source_alias (0006), not here, since the same KID could in
-- principle appear in raw_data on a row that also has a distinct api14
-- (denormalized field, not authoritative identity).
CREATE UNIQUE INDEX IF NOT EXISTS ks_well_api14_unique ON ks_well (api14) WHERE api14 IS NOT NULL;
CREATE INDEX IF NOT EXISTS ks_well_kid_idx ON ks_well (kgs_kid) WHERE kgs_kid IS NOT NULL;
CREATE INDEX IF NOT EXISTS ks_well_county_idx ON ks_well (county);
CREATE INDEX IF NOT EXISTS ks_well_lifecycle_status_idx ON ks_well (lifecycle_status);
CREATE INDEX IF NOT EXISTS ks_well_unknown_status_idx
    ON ks_well (well_class_raw, status_raw, status2_raw) WHERE lifecycle_status = 'unknown';
CREATE INDEX IF NOT EXISTS ks_well_geometry_idx ON ks_well USING GIST (display_geometry_4326);
CREATE INDEX IF NOT EXISTS ks_well_source_run_idx ON ks_well (source_ingestion_run_id);
