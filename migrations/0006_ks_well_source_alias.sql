-- KS-04: source-alias table (task doc §3.1 / §6.1a). KGS's own KID is
-- documented as "not necessarily permanent," so it (and KCC permit, and
-- the raw API string) live here as aliases pointing at a well, never as
-- the well's primary key.
--
-- Rollback (plain single-file migrations, no paired .down.sql):
--   DROP TABLE IF EXISTS ks_well_source_alias;  -- run before 0005's rollback (FK)

CREATE TABLE IF NOT EXISTS ks_well_source_alias (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    well_id uuid NOT NULL REFERENCES ks_well(internal_well_id) ON DELETE CASCADE,
    alias_type text NOT NULL,
    alias_value text NOT NULL,
    first_seen_at timestamptz NOT NULL DEFAULT now(),
    last_seen_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT ks_well_source_alias_type_check CHECK (alias_type IN ('kgs_kid', 'kcc_permit', 'api_raw')),
    CONSTRAINT ks_well_source_alias_unique UNIQUE (well_id, alias_type, alias_value)
);

CREATE INDEX IF NOT EXISTS ks_well_source_alias_lookup_idx ON ks_well_source_alias (alias_type, alias_value);
