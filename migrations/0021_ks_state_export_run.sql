-- KS-12/KS-13: local record of `state-export-v1` bundles this backend has
-- produced for the shared Future Wells Co platform, and whether
-- `futureBackend` has acknowledged importing each one. Deliberately much
-- simpler than a full change-log/CDC system (see
-- src/state_export/exporter.rs's module doc): this repo ships baseline/full
-- exports only for now, so there is no per-row change log to maintain,
-- just a record of each export attempt for `state-sync-v1`'s
-- status/pending-acks/acknowledge commands to work against.
--
-- Rollback (plain single-file migrations, no paired .down.sql):
--   DROP TABLE IF EXISTS ks_state_export_run;
CREATE TABLE IF NOT EXISTS ks_state_export_run (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    export_id text NOT NULL,
    consumer_code text NOT NULL,
    export_mode text NOT NULL,
    package_path text NOT NULL,
    manifest_checksum text NOT NULL,
    records_by_entity jsonb NOT NULL,
    total_records bigint NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    acknowledged_at timestamptz,
    CONSTRAINT ks_state_export_run_export_id_unique UNIQUE (export_id),
    CONSTRAINT ks_state_export_run_mode_check CHECK (export_mode IN ('baseline', 'full', 'incremental'))
);

CREATE INDEX IF NOT EXISTS ks_state_export_run_consumer_idx ON ks_state_export_run (consumer_code, created_at DESC);
CREATE INDEX IF NOT EXISTS ks_state_export_run_pending_ack_idx ON ks_state_export_run (consumer_code) WHERE acknowledged_at IS NULL;
