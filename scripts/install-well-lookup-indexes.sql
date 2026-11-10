-- Run with psql -X -v ON_ERROR_STOP=1. Concurrent builds keep readers/writers available.
SET lock_timeout = '5s';
SET statement_timeout = '5min';
CREATE INDEX CONCURRENTLY IF NOT EXISTS ks_well_public_api_raw_idx
 ON ks_well(api_raw) WHERE is_tombstoned = false;
CREATE INDEX CONCURRENTLY IF NOT EXISTS ks_well_public_api_compact_idx
 ON ks_well((regexp_replace(api_raw, '[^0-9A-Za-z]+', '', 'g'))) WHERE is_tombstoned = false;
CREATE INDEX CONCURRENTLY IF NOT EXISTS ks_well_public_uuid_text_idx
 ON ks_well((internal_well_id::text)) WHERE is_tombstoned = false;
CREATE INDEX CONCURRENTLY IF NOT EXISTS ks_well_public_county_slug_idx
 ON ks_well((trim(both '-' from lower(regexp_replace(regexp_replace(btrim(county), '&', ' and ', 'g'), '[^a-zA-Z0-9]+', '-', 'g'))))) WHERE is_tombstoned = false;
CREATE INDEX CONCURRENTLY IF NOT EXISTS ks_well_public_operator_slug_idx
 ON ks_well((trim(both '-' from lower(regexp_replace(regexp_replace(btrim(COALESCE(NULLIF(btrim(operator_name_kgs_reported_current), ''), NULLIF(btrim(operator_name_kgs_original), ''))), '&', ' and ', 'g'), '[^a-zA-Z0-9]+', '-', 'g'))))) WHERE is_tombstoned = false;
CREATE INDEX CONCURRENTLY IF NOT EXISTS ks_well_public_field_slug_idx
 ON ks_well((trim(both '-' from lower(regexp_replace(regexp_replace(btrim(field_name), '&', ' and ', 'g'), '[^a-zA-Z0-9]+', '-', 'g'))))) WHERE is_tombstoned = false;
SELECT indexrelid::regclass, indisvalid, pg_relation_size(indexrelid) FROM pg_index
 WHERE indexrelid::regclass::text LIKE 'ks_well_public_%_idx';
