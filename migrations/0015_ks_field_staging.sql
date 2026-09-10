-- KS-08: staging snapshot for the KGS field importer. Unlike KS-03/05/06/07,
-- this source is confirmed live (KGS is reachable, unlike kcc.ks.gov) —
-- verified 2026-09-05 against the real ArcGIS layer (item
-- 390ee1b9eade40bca42ee3534a813b20, 8,089 real features, fields
-- STATUS in {'Active','Abandoned'}) and the real field-production/
-- abandoned-field .xlsx files listed on the KGS field index page.
--
-- One ingestion_run covers all three source components together
-- (ArcGIS geometry/identity + the six active-file .xlsx enrichment files
-- + the one abandoned-file .xlsx), distinguished by `source_component` —
-- the same "one run, multiple fetches" shape KS-05 used for its
-- active/all operator downloads.
--
-- Rollback (plain single-file migrations, no paired .down.sql):
--   DROP TABLE IF EXISTS ks_field_raw_snapshot;
CREATE TABLE IF NOT EXISTS ks_field_raw_snapshot (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    ingestion_run_id uuid NOT NULL REFERENCES ks_ingestion_run(id) ON DELETE CASCADE,
    source_component text NOT NULL,
    -- ArcGIS-only.
    source_object_id bigint,
    field_kid text,
    -- Present from every component — the one field every source agrees
    -- on. Always the verbatim reported name; matching across components
    -- is exact-string only (task doc: "do not merge two official field
    -- names merely because punctuation or directional words look
    -- similar" — this importer never fuzzy-matches field names, unlike
    -- KS-06/KS-07's well-linking, which fuzzy-matches lease names).
    field_name text NOT NULL,
    status text,
    field_type text,
    -- ArcGIS's FORMATIONS or the .xlsx files' PROD_ZONES — same concept,
    -- different source, reconciled at materialize time.
    producing_zones text,
    -- active_file-only: lifetime totals, distinct from ArcGIS's own
    -- CUMM_OIL/CUMM_GAS (different source, same concept — the materializer
    -- prefers ArcGIS's since it's the identity/geometry source of truth).
    total_oil_prod double precision,
    total_gas_prod double precision,
    -- active_file-only: {"<year>": {"oil_prod":, "oil_wells":, "gas_prod":, "gas_wells":}, ...},
    -- pre-parsed by parse::kgs_fields::xlsx_source so the materializer
    -- never re-derives year columns from a header-name pattern.
    parsed_annual jsonb,
    -- ArcGIS-only: a raw GeoJSON geometry object
    -- (`{"type":"Polygon","coordinates":[...]}`), passed straight to
    -- `ST_GeomFromGeoJSON` at materialize time, never hand-parsed here.
    raw_geometry jsonb,
    raw_data jsonb NOT NULL,
    source_record_hash text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT ks_field_raw_snapshot_component_check
        CHECK (source_component IN ('arcgis', 'active_file', 'abandoned_file'))
);

CREATE INDEX IF NOT EXISTS ks_field_raw_snapshot_run_idx ON ks_field_raw_snapshot (ingestion_run_id);
CREATE INDEX IF NOT EXISTS ks_field_raw_snapshot_name_idx ON ks_field_raw_snapshot (ingestion_run_id, field_name);
