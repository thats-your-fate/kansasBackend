-- Failed/rejected runs retain their provenance but must not prevent retrying the same file.
ALTER TABLE ks_ingestion_run DROP CONSTRAINT ks_ingestion_run_source_checksum_unique;
CREATE UNIQUE INDEX ks_ingestion_run_active_checksum_unique ON ks_ingestion_run(source_id,payload_checksum)
WHERE status IN ('running','completed');
