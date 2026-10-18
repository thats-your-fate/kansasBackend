INSERT INTO ks_lease(lease_kid,dor_code,product,reported_lease_name,operator,county,township,township_direction,range,range_direction,section,spot,source_latitude,source_longitude,cumulative_production,year_start,year_stop,source_document_url,source_ingestion_run_id,source_record_hash)
SELECT lease_kid,dor_code,CASE upper(produces_raw) WHEN 'OIL' THEN 'oil' WHEN 'GAS' THEN 'gas' ELSE 'unknown' END,
 reported_lease_name,operator,county,township,township_direction,range,range_direction,section,spot,source_latitude,source_longitude,production,year_start,year_stop,source_url,ingestion_run_id,source_record_hash
FROM ks_lease_raw_snapshot WHERE ingestion_run_id=$1 AND rejection_reason IS NULL AND lease_kid IS NOT NULL
ON CONFLICT(lease_kid) WHERE lease_kid IS NOT NULL DO UPDATE SET
 dor_code=EXCLUDED.dor_code,product=EXCLUDED.product,reported_lease_name=EXCLUDED.reported_lease_name,
 operator=EXCLUDED.operator,county=EXCLUDED.county,township=EXCLUDED.township,township_direction=EXCLUDED.township_direction,
 range=EXCLUDED.range,range_direction=EXCLUDED.range_direction,section=EXCLUDED.section,spot=EXCLUDED.spot,
 source_latitude=EXCLUDED.source_latitude,source_longitude=EXCLUDED.source_longitude,cumulative_production=EXCLUDED.cumulative_production,
 year_start=EXCLUDED.year_start,year_stop=EXCLUDED.year_stop,source_document_url=EXCLUDED.source_document_url,
 source_ingestion_run_id=EXCLUDED.source_ingestion_run_id,source_record_hash=EXCLUDED.source_record_hash,updated_at=now();
-- NEXT STATEMENT
INSERT INTO ks_lease_source_alias(lease_id,alias_type,alias_value)
SELECT l.id,v.kind,v.value FROM ks_lease l
CROSS JOIN LATERAL (VALUES ('lease_kid',l.lease_kid),('dor_code',l.dor_code)) v(kind,value)
WHERE l.source_ingestion_run_id=$1 AND nullif(btrim(v.value),'') IS NOT NULL
ON CONFLICT(lease_id,alias_type,alias_value) DO UPDATE SET last_seen_at=now();
-- NEXT STATEMENT
DELETE FROM ks_well_lease_link WHERE lease_id IN (SELECT id FROM ks_lease WHERE source_ingestion_run_id=$1);
-- NEXT STATEMENT
WITH well_ids AS (
 SELECT internal_well_id AS well_id,'oil'::text AS product,oil_kid AS kid,oil_dor_id AS dor FROM ks_well WHERE NOT is_tombstoned
 UNION ALL SELECT internal_well_id,'gas',gas_kid,gas_dor_id FROM ks_well WHERE NOT is_tombstoned
), unique_dor AS (
 SELECT product,dor_code FROM ks_lease WHERE source_ingestion_run_id=$1 AND dor_code IS NOT NULL
 GROUP BY product,dor_code HAVING count(*)=1
)
INSERT INTO ks_well_lease_link(well_id,lease_id,product,link_source,link_method,confidence,source_ingestion_run_id)
SELECT DISTINCT w.well_id,l.id,l.product,'KGS_PRODUCT_KID','exact_identifier',1,$1
FROM well_ids w JOIN ks_lease l ON l.lease_kid=w.kid AND l.product=w.product AND l.source_ingestion_run_id=$1
UNION ALL
SELECT DISTINCT w.well_id,l.id,l.product,'KGS_UNIQUE_PRODUCT_DOR_ID','exact_identifier',1,$1
FROM well_ids w
JOIN unique_dor d ON d.product=w.product AND d.dor_code=w.dor
JOIN ks_lease l ON l.source_ingestion_run_id=$1 AND l.product=d.product AND l.dor_code=d.dor_code
WHERE NOT EXISTS(SELECT 1 FROM ks_lease k WHERE k.source_ingestion_run_id=$1 AND k.product=w.product AND k.lease_kid=w.kid)
ON CONFLICT(well_id,lease_id,product,link_method) DO NOTHING;
-- NEXT STATEMENT
WITH api_tokens AS (
 SELECT l.id,l.product,regexp_replace(trim(t.token),'-','','g') AS digits
 FROM ks_lease l JOIN ks_lease_raw_snapshot r ON r.lease_kid=l.lease_kid AND r.ingestion_run_id=$1
 CROSS JOIN LATERAL regexp_split_to_table(r.api_number_raw,',') t(token)
 WHERE l.source_ingestion_run_id=$1 AND l.product IN ('oil','gas') AND trim(t.token) ~ '^[0-9-]+$'
)
INSERT INTO ks_well_lease_link(well_id,lease_id,product,link_source,link_method,confidence,source_ingestion_run_id)
SELECT DISTINCT w.internal_well_id,a.id,a.product,'KGS_LEASE_API_NUMBER','exact_api_number',1,$1
FROM api_tokens a JOIN ks_well w ON w.api14=CASE length(a.digits) WHEN 10 THEN a.digits||'0000' WHEN 14 THEN a.digits END
WHERE NOT w.is_tombstoned
  AND NOT EXISTS(SELECT 1 FROM ks_well_lease_link e WHERE e.well_id=w.internal_well_id AND e.lease_id=a.id AND e.product=a.product)
ON CONFLICT(well_id,lease_id,product,link_method) DO NOTHING;
