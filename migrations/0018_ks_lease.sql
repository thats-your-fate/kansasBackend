-- KS-09: canonical Kansas lease dimension + well-to-lease relationships.
--
-- Rollback (plain single-file migrations, no paired .down.sql):
--   DROP TABLE IF EXISTS ks_well_lease_link;  -- first (FK)
--   DROP TABLE IF EXISTS ks_lease_source_alias;
--   DROP TABLE IF EXISTS ks_lease;

-- Internal UUID is the stable key (task doc: "Use an internal UUID as
-- the stable lease key... Do not use LEASE_KID as the permanent database
-- primary key; KGS describes it as not necessarily permanent" — the
-- identical discipline KS-04 already applies to well KID/KCC permit).
CREATE TABLE IF NOT EXISTS ks_lease (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    lease_kid text,
    dor_code text,
    -- 'oil' | 'gas' | 'unknown' — derived from PRODUCES, which is
    -- confirmed live to sometimes be the literal string "NULL" (not a
    -- blank), handled explicitly rather than silently misparsed.
    product text NOT NULL,
    reported_lease_name text,
    operator text,
    -- Confirmed live: the real all-leases file has no FIELD column at
    -- all (see 0017's note) — nullable, always NULL until a source that
    -- actually provides one is wired in. Kept as a real column now
    -- (rather than added later) so the task doc's requested shape is on
    -- record even though no data populates it yet.
    field_name text,
    county text,
    township smallint,
    township_direction text,
    range smallint,
    range_direction text,
    section smallint,
    spot text,
    -- NAD27, per KGS's own lease-format documentation ("generally not as
    -- well defined as the location created for well data") — same
    -- preserve-source-CRS discipline as ks_well.source_crs (task doc §3.3).
    source_latitude double precision,
    source_longitude double precision,
    source_crs text NOT NULL DEFAULT 'NAD27',
    cumulative_production double precision,
    -- The file's own last-reported-activity window — NOT a definitive
    -- lease start/end date. A lease still producing today can show
    -- year_stop = the current year simply because that's the most recent
    -- reported production, not because it ceased.
    year_start integer,
    year_stop integer,
    source_document_url text,
    source_ingestion_run_id uuid NOT NULL REFERENCES ks_ingestion_run(id),
    source_record_hash text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT ks_lease_product_check CHECK (product IN ('oil', 'gas', 'unknown'))
);

CREATE UNIQUE INDEX IF NOT EXISTS ks_lease_kid_unique ON ks_lease (lease_kid) WHERE lease_kid IS NOT NULL;
CREATE INDEX IF NOT EXISTS ks_lease_reported_name_idx ON ks_lease (reported_lease_name);
CREATE INDEX IF NOT EXISTS ks_lease_county_idx ON ks_lease (county);

-- Source-alias table (task doc: "Store source aliases: LEASE_KID,
-- DOR_CODE...") — same shape/rationale as ks_well_source_alias/
-- ks_field_source_alias: LEASE_KID and DOR_CODE are undocumented-
-- permanence identifiers, never a primary key, always tracked here so a
-- future KID/DOR_CODE change survives ("source aliases survive changes,"
-- task doc acceptance criteria).
CREATE TABLE IF NOT EXISTS ks_lease_source_alias (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    lease_id uuid NOT NULL REFERENCES ks_lease(id) ON DELETE CASCADE,
    alias_type text NOT NULL,
    alias_value text NOT NULL,
    first_seen_at timestamptz NOT NULL DEFAULT now(),
    last_seen_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT ks_lease_source_alias_type_check CHECK (alias_type IN ('lease_kid', 'dor_code')),
    CONSTRAINT ks_lease_source_alias_unique UNIQUE (lease_id, alias_type, alias_value)
);

CREATE INDEX IF NOT EXISTS ks_lease_source_alias_lookup_idx ON ks_lease_source_alias (alias_type, alias_value);

-- Well-to-lease relationships. A physical well may link to an oil lease
-- record and a gas lease record independently (task doc) — this is a
-- join table, not a single FK on ks_well, and `product` is part of the
-- link's own identity so the same (well, lease) pair could in principle
-- appear under both products without conflict (in practice a given
-- lease row is only ever one product, so this mostly guards against
-- accidental duplicate inserts of the same link).
--
-- Hard requirement (task doc): "Never infer surface, mineral, royalty,
-- or working-interest ownership from these records." Nothing in this
-- schema or the materializer that populates it computes or stores any
-- ownership/title concept — this table is purely an identity
-- cross-reference.
CREATE TABLE IF NOT EXISTS ks_well_lease_link (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    well_id uuid NOT NULL REFERENCES ks_well(internal_well_id),
    lease_id uuid NOT NULL REFERENCES ks_lease(id),
    product text NOT NULL,
    -- Which raw field on the well/lease side supplied the evidence.
    link_source text NOT NULL,
    -- The general strategy: exact_identifier (well's OIL_KID/OIL_DOR_ID/
    -- GAS_KID/GAS_DOR_ID against the lease's own lease_kid/dor_code,
    -- scoped by product so oil and gas identifiers can never collide),
    -- exact_api_number (an API number listed in the lease's own
    -- production-data API_NUMBER field matches a well's api14 exactly),
    -- or name_match (fallback only — task doc acceptance criteria:
    -- "exact ID-based links take precedence over name matching").
    link_method text NOT NULL,
    confidence numeric NOT NULL,
    valid_from date,
    valid_to date,
    source_ingestion_run_id uuid NOT NULL REFERENCES ks_ingestion_run(id),
    created_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT ks_well_lease_link_product_check CHECK (product IN ('oil', 'gas')),
    CONSTRAINT ks_well_lease_link_method_check CHECK (link_method IN ('exact_identifier', 'exact_api_number', 'name_match')),
    CONSTRAINT ks_well_lease_link_unique UNIQUE (well_id, lease_id, product, link_method)
);

CREATE INDEX IF NOT EXISTS ks_well_lease_link_well_idx ON ks_well_lease_link (well_id);
CREATE INDEX IF NOT EXISTS ks_well_lease_link_lease_idx ON ks_well_lease_link (lease_id);
