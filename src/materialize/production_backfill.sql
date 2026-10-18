WITH eligible AS MATERIALIZED (
 SELECT l.id AS lease_id,r.lease_kid,r.reported_lease_name,r.dor_code,r.api_number_raw,r.field_name,
 r.producing_zone,r.operator,r.county,r.township,r.township_direction,r.range,r.range_direction,r.section,
 r.spot,r.source_latitude,r.source_longitude,r.year,r.month,
 CASE r.month WHEN 0 THEN 'yearly_total' WHEN -1 THEN 'starting_cumulative' ELSE 'monthly' END AS record_kind,
 CASE upper(btrim(r.product_raw)) WHEN 'O' THEN 'oil' WHEN 'OIL' THEN 'oil' ELSE 'gas' END AS product,
 r.reported_contributing_wells,r.production_volume,r.source_url,r.source_record_hash
 FROM ks_lease_production_raw_snapshot r LEFT JOIN ks_lease l ON l.lease_kid=r.lease_kid
 WHERE r.ingestion_run_id=$1 AND r.rejection_reason IS NULL AND r.lease_kid IS NOT NULL
   AND r.month BETWEEN -1 AND 12 AND upper(btrim(r.product_raw)) IN ('O','OIL','G','GAS')
), upserted AS (
 INSERT INTO ks_lease_production_month(lease_id,lease_kid,reported_lease_name,dor_code,api_number_raw,
 field_name,producing_zone,operator,county,township,township_direction,range,range_direction,section,spot,
 source_latitude,source_longitude,year,month,record_kind,product,reported_contributing_wells,production_volume,
 source_url,source_ingestion_run_id,source_record_hash)
 SELECT lease_id,lease_kid,reported_lease_name,dor_code,api_number_raw,field_name,producing_zone,operator,county,
 township,township_direction,range,range_direction,section,spot,source_latitude,source_longitude,year,month,
 record_kind,product,reported_contributing_wells,production_volume,source_url,$1,source_record_hash FROM eligible
 ON CONFLICT(lease_kid,product,year,month) DO UPDATE SET
 lease_id=coalesce(EXCLUDED.lease_id,ks_lease_production_month.lease_id),reported_lease_name=EXCLUDED.reported_lease_name,
 dor_code=EXCLUDED.dor_code,api_number_raw=EXCLUDED.api_number_raw,field_name=EXCLUDED.field_name,
 producing_zone=EXCLUDED.producing_zone,operator=EXCLUDED.operator,county=EXCLUDED.county,township=EXCLUDED.township,
 township_direction=EXCLUDED.township_direction,range=EXCLUDED.range,range_direction=EXCLUDED.range_direction,
 section=EXCLUDED.section,spot=EXCLUDED.spot,source_latitude=EXCLUDED.source_latitude,source_longitude=EXCLUDED.source_longitude,
 record_kind=EXCLUDED.record_kind,reported_contributing_wells=EXCLUDED.reported_contributing_wells,
 production_volume=EXCLUDED.production_volume,source_url=EXCLUDED.source_url,source_ingestion_run_id=EXCLUDED.source_ingestion_run_id,
 source_record_hash=EXCLUDED.source_record_hash,updated_at=now()
 WHERE ks_lease_production_month.source_record_hash IS DISTINCT FROM EXCLUDED.source_record_hash
    OR ks_lease_production_month.source_ingestion_run_id IS DISTINCT FROM EXCLUDED.source_ingestion_run_id
    OR (EXCLUDED.lease_id IS NOT NULL AND ks_lease_production_month.lease_id IS DISTINCT FROM EXCLUDED.lease_id)
 RETURNING (xmax=0) AS was_inserted
)
SELECT count(*) FILTER(WHERE was_inserted)::bigint AS created,
 (SELECT count(*) FROM eligible)-count(*) FILTER(WHERE was_inserted) AS updated,
 (SELECT count(*) FROM ks_lease_production_raw_snapshot WHERE ingestion_run_id=$1 AND rejection_reason IS NULL
   AND lease_kid IS NOT NULL AND month NOT BETWEEN -1 AND 12) AS skipped_unmapped_month,
 (SELECT count(*) FROM ks_lease_production_raw_snapshot WHERE ingestion_run_id=$1 AND rejection_reason IS NULL
   AND lease_kid IS NOT NULL AND month BETWEEN -1 AND 12
   AND (product_raw IS NULL OR upper(btrim(product_raw)) NOT IN ('O','OIL','G','GAS'))) AS skipped_product_mismatch
FROM upserted;
