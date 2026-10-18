INSERT INTO ks_event(event_type,dedup_key,occurred_at,observed_at,payload,source_ingestion_run_id,related_well_id,related_county)
SELECT v.kind, w.internal_well_id::text || ':' || v.day::text,
       v.day::timestamp AT TIME ZONE 'America/Chicago', now(),
       jsonb_build_object('historical_backfill',true,'source','KGS well master',
           'title',v.label,'summary','Historical date recorded by KGS; imported as source history, not newly observed activity.'),
       w.source_ingestion_run_id,w.internal_well_id,w.county
FROM ks_well w JOIN ks_ingestion_run r ON r.id=w.source_ingestion_run_id
CROSS JOIN LATERAL (VALUES
    ('SPUD_RECORDED',w.spud_date,'Spud date recorded by KGS (historical)'),
    ('COMPLETION_RECORDED',w.completion_date,'Completion date recorded by KGS (historical)'),
    ('PLUGGING_RECORDED',w.plug_date,'Plugging date recorded by KGS (historical)')
) v(kind,day,label)
WHERE r.status='completed' AND r.source_url LIKE 'https://www.kgs.ku.edu/%'
  AND r.parser_version !~* '(^|[-_])(test|fixture)'
  AND v.day BETWEEN '1850-01-01'::date AND CURRENT_DATE
ON CONFLICT(event_type,dedup_key) DO NOTHING
