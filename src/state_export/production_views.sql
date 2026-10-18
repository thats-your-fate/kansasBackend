-- Session-local views used only by --exclude-test-ingestion exports.
-- Public source tables and the Kansas API remain unchanged.
CREATE TEMP VIEW ks_sync_real_ingestion_runs AS
SELECT id FROM public.ks_ingestion_run
WHERE coalesce(parser_version, '') !~* '(^|[-_])(test|fixture)'
  AND coalesce(source_url, '') !~* '(example[.]invalid|test[-_]?fixture|127[.]0[.]0[.]1|localhost)';

DO $$
DECLARE table_name text;
        projection text;
BEGIN
    FOREACH table_name IN ARRAY ARRAY[
        'ks_well', 'ks_operator', 'ks_field', 'ks_lease',
        'ks_lease_production_month', 'ks_event', 'ks_drilling_intent',
        'ks_operator_transfer'
    ] LOOP
        projection := 't.*';
        IF table_name = 'ks_well' THEN
            -- ZIP coordinates are NAD27; export true WGS84 map points without
            -- changing the source coordinates or the live Kansas tables.
            SELECT string_agg(
                CASE WHEN attname = 'display_geometry_4326' THEN
                    'COALESCE(t.display_geometry_4326, CASE '
                    'WHEN t.source_latitude BETWEEN -90 AND 90 AND t.source_longitude BETWEEN -180 AND 180 '
                    'AND upper(t.source_crs) IN (''NAD27'',''EPSG:4267'',''NAD83'',''EPSG:4269'',''WGS84'',''EPSG:4326'') '
                    'THEN ST_Transform(ST_SetSRID(ST_MakePoint(t.source_longitude,t.source_latitude), '
                    'CASE upper(t.source_crs) WHEN ''NAD27'' THEN 4267 WHEN ''EPSG:4267'' THEN 4267 '
                    'WHEN ''NAD83'' THEN 4269 WHEN ''EPSG:4269'' THEN 4269 ELSE 4326 END),4326) '
                    'END) AS display_geometry_4326'
                ELSE format('t.%I', attname) END,
                ', ' ORDER BY attnum)
            INTO projection
            FROM pg_attribute
            WHERE attrelid = 'public.ks_well'::regclass AND attnum > 0 AND NOT attisdropped;
        END IF;
        EXECUTE format(
            'CREATE TEMP VIEW %I AS SELECT %s FROM public.%I t '
            'WHERE t.source_ingestion_run_id IN (SELECT id FROM pg_temp.ks_sync_real_ingestion_runs)',
            table_name, projection, table_name);
    END LOOP;
END $$;

CREATE TEMP VIEW ks_well_source_alias AS
SELECT a.* FROM public.ks_well_source_alias a
WHERE a.well_id IN (SELECT internal_well_id FROM pg_temp.ks_well);

CREATE TEMP VIEW ks_operator_transfer_well AS
SELECT t.* FROM public.ks_operator_transfer_well t
WHERE t.well_id IN (SELECT internal_well_id FROM pg_temp.ks_well)
  AND t.transfer_id IN (SELECT id FROM pg_temp.ks_operator_transfer);

CREATE TEMP VIEW ks_well_lease_link AS
SELECT link.* FROM public.ks_well_lease_link link
WHERE link.well_id IN (SELECT internal_well_id FROM pg_temp.ks_well)
  AND link.lease_id IN (SELECT id FROM pg_temp.ks_lease)
  AND link.source_ingestion_run_id IN (SELECT id FROM pg_temp.ks_sync_real_ingestion_runs);
