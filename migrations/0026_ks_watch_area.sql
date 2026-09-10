-- KS-14: watch areas. This repo has no auth/account system yet (an
-- explicitly deferred item since KS-001 — see
-- docs/architecture/kansas-source-boundary.md's "explicitly deferred"
-- section), so a watch area here is not yet tied to a user account; it
-- exists so the matching engine (src/watch.rs) and its acceptance
-- criteria (radius/county/operator/field/well watch tests) have a real
-- table to query against. `owner_reference` is a free-text placeholder
-- for whatever identifies an owner once a real account system exists
-- (a central Future Wells Co user id, most likely, given KS-11/12/13's
-- established pattern of account-level concepts living centrally) —
-- deliberately not a foreign key to anything, since nothing to reference
-- exists yet.
--
-- Rollback (plain single-file migrations, no paired .down.sql):
--   DROP TABLE IF EXISTS ks_watch_area;
CREATE TABLE IF NOT EXISTS ks_watch_area (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    owner_reference text,
    watch_type text NOT NULL,
    county text,
    operator_id uuid REFERENCES ks_operator(id),
    field_id uuid REFERENCES ks_field(id),
    well_id uuid REFERENCES ks_well(internal_well_id),
    center_latitude double precision,
    center_longitude double precision,
    radius_miles double precision,
    is_active boolean NOT NULL DEFAULT true,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT ks_watch_area_type_check CHECK (watch_type IN ('county', 'operator', 'field', 'well', 'radius')),
    CONSTRAINT ks_watch_area_county_fields_check CHECK (watch_type != 'county' OR county IS NOT NULL),
    CONSTRAINT ks_watch_area_operator_fields_check CHECK (watch_type != 'operator' OR operator_id IS NOT NULL),
    CONSTRAINT ks_watch_area_field_fields_check CHECK (watch_type != 'field' OR field_id IS NOT NULL),
    CONSTRAINT ks_watch_area_well_fields_check CHECK (watch_type != 'well' OR well_id IS NOT NULL),
    CONSTRAINT ks_watch_area_radius_fields_check
        CHECK (watch_type != 'radius' OR (center_latitude IS NOT NULL AND center_longitude IS NOT NULL AND radius_miles IS NOT NULL AND radius_miles > 0))
);

CREATE INDEX IF NOT EXISTS ks_watch_area_active_type_idx ON ks_watch_area (watch_type) WHERE is_active;
CREATE INDEX IF NOT EXISTS ks_watch_area_county_idx ON ks_watch_area (county) WHERE watch_type = 'county' AND is_active;
CREATE INDEX IF NOT EXISTS ks_watch_area_operator_idx ON ks_watch_area (operator_id) WHERE watch_type = 'operator' AND is_active;
CREATE INDEX IF NOT EXISTS ks_watch_area_field_idx ON ks_watch_area (field_id) WHERE watch_type = 'field' AND is_active;
CREATE INDEX IF NOT EXISTS ks_watch_area_well_idx ON ks_watch_area (well_id) WHERE watch_type = 'well' AND is_active;
