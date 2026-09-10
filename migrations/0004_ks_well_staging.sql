-- KS-03: staging snapshot for the KGS statewide well importer.
--
-- Rollback (plain single-file migrations, no paired .down.sql):
--   ALTER TABLE ks_ingestion_run DROP COLUMN IF EXISTS manifest;
--   DROP TABLE IF EXISTS ks_well_raw_snapshot;

-- "Manifest with counts/checksums" (KS-03 acceptance criteria), mirroring
-- futureBackend's own import_runs.manifest jsonb precedent (see task doc
-- §11/§10.1). Holds source-metadata-before/after, object-ID range
-- coverage, and per-batch checksums — the acceptance-criteria detail that
-- doesn't fit a typed column.
ALTER TABLE ks_ingestion_run
    ADD COLUMN IF NOT EXISTS manifest jsonb NOT NULL DEFAULT '{}'::jsonb;

-- One row per raw record captured during a run. Immutable — never
-- updated or deleted after insert, including excluded (non-Kansas) rows,
-- which are retained with a reason rather than dropped (task doc §3: "no
-- ingestion, parsing, normalization... code" caveat now updated by KS-03;
-- "raw-first ingestion" fixed architecture decision applies here exactly).
--
-- This table is scoped per ingestion_run: a materializer (KS-04+) must
-- only read rows whose run has reached `completed` status
-- (ks_ingestion_run.status), never a `running`/`failed`/`rejected` one —
-- that is the "atomically publish only after QA passes" guarantee, built
-- on the state machine `src/source/ingestion_run.rs` already enforces.
CREATE TABLE IF NOT EXISTS ks_well_raw_snapshot (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    ingestion_run_id uuid NOT NULL REFERENCES ks_ingestion_run(id) ON DELETE CASCADE,
    -- ArcGIS OBJECTID; NULL when this row came from the ZIP fallback,
    -- which has no equivalent stable per-request identifier.
    source_object_id bigint,
    -- KID is present (as an integer or string, depending on source) in
    -- both the ArcGIS layer and the ZIP export; kept as its own column
    -- for fast identity cross-checks even though it's also in raw_data.
    source_kid text,
    source_format text NOT NULL,
    raw_data jsonb NOT NULL,
    -- CRS varies by source_format, confirmed at KS-04 by reading
    -- kgs.ku.edu/PRS/file_format.html in full: for 'arcgis' rows this is
    -- true WGS84 (outSR=4326 was requested); for 'zip' rows this is the
    -- ZIP's plain LATITUDE/LONGITUDE columns, which KGS's own
    -- documentation confirms are NAD 1927 (NAD27), NOT reprojected. Never
    -- named "wgs84" — that would misstate the zip path's actual CRS.
    -- KS-04 only builds ks_well.display_geometry_4326 from 'arcgis' rows;
    -- 'zip' rows keep their NAD27 coordinates in source_latitude/
    -- source_longitude with no display-geometry claim, until a real
    -- NAD27->WGS84 transform is implemented.
    geometry_source_x double precision,
    geometry_source_y double precision,
    kansas_membership_status text NOT NULL DEFAULT 'included',
    kansas_membership_reason text,
    created_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT ks_well_raw_snapshot_format_check CHECK (source_format IN ('arcgis', 'zip')),
    CONSTRAINT ks_well_raw_snapshot_membership_check
        CHECK (kansas_membership_status IN ('included', 'excluded_other_state')),
    CONSTRAINT ks_well_raw_snapshot_membership_reason_check
        CHECK (kansas_membership_status = 'included' OR kansas_membership_reason IS NOT NULL)
);

CREATE INDEX IF NOT EXISTS ks_well_raw_snapshot_run_idx ON ks_well_raw_snapshot (ingestion_run_id);
CREATE INDEX IF NOT EXISTS ks_well_raw_snapshot_object_id_idx
    ON ks_well_raw_snapshot (ingestion_run_id, source_object_id) WHERE source_object_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS ks_well_raw_snapshot_kid_idx
    ON ks_well_raw_snapshot (source_kid) WHERE source_kid IS NOT NULL;
CREATE INDEX IF NOT EXISTS ks_well_raw_snapshot_membership_idx
    ON ks_well_raw_snapshot (ingestion_run_id, kansas_membership_status);
