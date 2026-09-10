-- KS-15: raw staging for well-document metadata, same "permanent,
-- append-only raw + upserted canonical" split used by every other
-- Kansas source in this repo (KS-03 through KS-10).
--
-- Rollback (plain single-file migrations, no paired .down.sql):
--   DROP TABLE IF EXISTS ks_well_document_raw_snapshot;
CREATE TABLE IF NOT EXISTS ks_well_document_raw_snapshot (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    ingestion_run_id uuid NOT NULL REFERENCES ks_ingestion_run(id),
    document_source text NOT NULL,
    source_document_id text,
    raw_data jsonb NOT NULL,
    source_record_hash text NOT NULL,
    rejection_reason text,
    observed_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT ks_well_document_raw_snapshot_source_check CHECK (document_source IN ('KGS_DOCUMENTS', 'KCC_KOLAR_DOCUMENTS'))
);

CREATE INDEX IF NOT EXISTS ks_well_document_raw_snapshot_run_idx ON ks_well_document_raw_snapshot (ingestion_run_id, id);
