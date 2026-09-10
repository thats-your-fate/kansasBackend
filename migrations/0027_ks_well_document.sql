-- KS-15: Kansas well-document metadata index. Metadata-first by design
-- (task doc: "Do not mass-download every document during the first well
-- import. Index metadata and use a controlled queue for documents
-- required by product views or downstream extraction.") — this table
-- never stores a document body; `content_hash` is only ever populated
-- once (and if) a specific document is deliberately queued and
-- downloaded, tracked by `parser_status`.
--
-- Natural identity: `(document_source, source_document_id)` — the same
-- source-alias discipline already used for wells/fields/leases (never a
-- reused-KID-style value as the sole primary key; here the surrogate
-- `id` is the primary key and the natural pair is a unique index).
--
-- Rollback (plain single-file migrations, no paired .down.sql):
--   DROP TABLE IF EXISTS ks_well_document;
CREATE TABLE IF NOT EXISTS ks_well_document (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    document_source text NOT NULL,
    source_document_id text NOT NULL,
    document_type text NOT NULL,
    document_type_raw text,

    -- Well relationship is evidence, not a guaranteed resolution — a
    -- document metadata record is still valuable even when it can't (yet,
    -- or ever) be tied to a materialized ks_well row (task doc: "source
    -- links remain available when extraction fails" — the same principle
    -- applies to relationship resolution).
    well_id uuid REFERENCES ks_well(internal_well_id),
    api_number_raw text,
    kgs_kid text,

    source_url text NOT NULL,
    filing_date date,
    filing_date_raw text,

    -- Task doc: "Respect confidentiality, login pages, robots/access
    -- restrictions, and unavailable records. Do not attempt to acquire
    -- documents from login-only libraries." `public_access_state` is set
    -- from the source's own observed signal at metadata-index time
    -- (never guessed), and the `parser_status` CHECK below structurally
    -- prevents a restricted/login-only/unavailable record from ever
    -- reaching a "queued for download" state.
    public_access_state text NOT NULL,
    parser_status text NOT NULL DEFAULT 'not_queued',
    content_hash text,
    content_object_storage_key text,
    parser_failure_reason text,

    first_observed_at timestamptz NOT NULL DEFAULT now(),
    last_observed_at timestamptz NOT NULL DEFAULT now(),
    source_ingestion_run_id uuid NOT NULL REFERENCES ks_ingestion_run(id),
    source_record_hash text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),

    CONSTRAINT ks_well_document_source_check CHECK (document_source IN ('KGS_DOCUMENTS', 'KCC_KOLAR_DOCUMENTS')),
    CONSTRAINT ks_well_document_type_check CHECK (document_type IN (
        'c1', 'aco1', 't1', 'cp1', 'cp4', 'c5', 'cg1', 'g2', 'aco16',
        'u1', 'u3c', 'u7', 'u8', 'u9',
        'wireline_log', 'las', 'driller_report', 'geologist_report',
        'directional_survey', 'plat', 'other'
    )),
    CONSTRAINT ks_well_document_access_state_check CHECK (public_access_state IN ('public', 'restricted', 'login_required', 'unavailable')),
    CONSTRAINT ks_well_document_parser_status_check CHECK (parser_status IN ('not_queued', 'queued', 'extracted', 'failed', 'skipped_restricted')),
    -- Structural enforcement, not just convention: a document can only be
    -- queued/extracted if it was actually observed as publicly
    -- accessible. Downloading is never even reachable for a restricted/
    -- login-only/unavailable record.
    CONSTRAINT ks_well_document_no_bypass_check CHECK (public_access_state = 'public' OR parser_status IN ('not_queued', 'skipped_restricted'))
);

CREATE UNIQUE INDEX IF NOT EXISTS ks_well_document_identity_unique ON ks_well_document (document_source, source_document_id);
CREATE INDEX IF NOT EXISTS ks_well_document_well_idx ON ks_well_document (well_id) WHERE well_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS ks_well_document_api_idx ON ks_well_document (api_number_raw) WHERE api_number_raw IS NOT NULL;
CREATE INDEX IF NOT EXISTS ks_well_document_kid_idx ON ks_well_document (kgs_kid) WHERE kgs_kid IS NOT NULL;
CREATE INDEX IF NOT EXISTS ks_well_document_type_idx ON ks_well_document (document_type);
CREATE INDEX IF NOT EXISTS ks_well_document_unresolved_idx ON ks_well_document (well_id) WHERE well_id IS NULL;
CREATE INDEX IF NOT EXISTS ks_well_document_queue_idx ON ks_well_document (parser_status) WHERE parser_status = 'queued';
