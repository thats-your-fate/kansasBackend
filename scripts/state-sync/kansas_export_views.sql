-- Export-only views: leave the live Kansas tables intact while excluding
-- integration fixtures identified by their ingestion provenance.
BEGIN;
CREATE SCHEMA IF NOT EXISTS ks_export_v1;
CREATE OR REPLACE VIEW ks_export_v1.real_ingestion_runs AS
SELECT id FROM public.ks_ingestion_run
WHERE coalesce(parser_version, '') !~* '(^|[-_])(test|fixture)'
  AND coalesce(source_url, '') !~* '(example[.]invalid|test[-_]?fixture)';

DO $$
DECLARE table_name text;
BEGIN
    FOREACH table_name IN ARRAY ARRAY[
        'ks_well', 'ks_operator', 'ks_field', 'ks_lease',
        'ks_lease_production_month', 'ks_event', 'ks_drilling_intent',
        'ks_operator_transfer'
    ] LOOP
        EXECUTE format(
            'CREATE OR REPLACE VIEW ks_export_v1.%I AS SELECT t.* FROM public.%I t '
            'WHERE t.source_ingestion_run_id IN (SELECT id FROM ks_export_v1.real_ingestion_runs)',
            table_name, table_name);
    END LOOP;
END $$;

CREATE OR REPLACE VIEW ks_export_v1.ks_well_source_alias AS
SELECT a.* FROM public.ks_well_source_alias a
WHERE a.well_id IN (SELECT internal_well_id FROM ks_export_v1.ks_well);

CREATE OR REPLACE VIEW ks_export_v1.ks_operator_transfer_well AS
SELECT t.* FROM public.ks_operator_transfer_well t
WHERE t.well_id IN (SELECT internal_well_id FROM ks_export_v1.ks_well)
  AND t.transfer_id IN (SELECT id FROM ks_export_v1.ks_operator_transfer);
COMMIT;
