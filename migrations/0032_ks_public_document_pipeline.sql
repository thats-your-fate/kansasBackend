-- Public KGS PDFs: immutable bytes, current metadata and explicit event evidence.
-- No login-only library content or private extracted page text is published.
ALTER TABLE ks_well_document ADD COLUMN IF NOT EXISTS effective_date date;
ALTER TABLE ks_well_document ADD COLUMN IF NOT EXISTS effective_date_basis text;
ALTER TABLE ks_well_document ADD COLUMN IF NOT EXISTS stored_at timestamptz;
ALTER TABLE ks_well_document ADD COLUMN IF NOT EXISTS page_count integer;

CREATE TABLE ks_document_version (
 id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
 document_id uuid NOT NULL REFERENCES ks_well_document(id),
 content_hash text NOT NULL CHECK (content_hash ~ '^[a-f0-9]{64}$'),
 object_storage_key text NOT NULL CHECK (object_storage_key LIKE 'kansas/documents/sha256/%'),
 size_bytes bigint NOT NULL CHECK (size_bytes BETWEEN 1 AND 52428800),
 page_count integer NOT NULL CHECK (page_count > 0),
 source_url text NOT NULL,
 metadata_hash text NOT NULL,
 source_received_on date,
 effective_date date,
 effective_date_basis text,
 source_metadata jsonb NOT NULL,
 stored_at timestamptz NOT NULL DEFAULT now(),
 UNIQUE(document_id,content_hash,metadata_hash)
);
ALTER TABLE ks_well_document ADD COLUMN current_version_id uuid REFERENCES ks_document_version(id);
CREATE TABLE ks_document_event_link (
 document_id uuid NOT NULL REFERENCES ks_well_document(id),
 event_id uuid NOT NULL REFERENCES ks_event(id),
 version_id uuid NOT NULL REFERENCES ks_document_version(id),
 PRIMARY KEY(document_id,event_id)
);
CREATE TABLE ks_document_scan (
 well_id uuid PRIMARY KEY REFERENCES ks_well(internal_well_id),
 checked_at timestamptz NOT NULL DEFAULT now(),
 page_hash text,
 document_count integer,
 error text,
 failures integer NOT NULL DEFAULT 0
);
CREATE INDEX ks_document_scan_checked_idx ON ks_document_scan(checked_at);
CREATE INDEX ks_document_filing_public_idx ON ks_well_document(well_id,filing_date DESC)
 WHERE public_access_state='public' AND content_object_storage_key IS NOT NULL;
