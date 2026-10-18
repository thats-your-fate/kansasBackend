-- Bulk archive reads do not need UUID ordering. A run-only B-tree keeps equal
-- run keys in heap-TID order and avoids millions of random heap fetches through
-- the (ingestion_run_id,id) pagination index. Retain that index for live paging.
-- Rollback: DROP INDEX IF EXISTS ks_production_raw_archive_scan_idx;
CREATE INDEX IF NOT EXISTS ks_production_raw_archive_scan_idx
ON ks_lease_production_raw_snapshot(ingestion_run_id);
