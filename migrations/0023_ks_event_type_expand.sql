-- KS-14: expands ks_event (KS-06/KS-07) to the full event-type set the
-- task doc requires, plus three explicitly reserved-but-not-yet-emitted
-- types (CONSERVATION_DOCKET_FILED, CONSERVATION_ORDER_ISSUED,
-- UIC_REPORT_UPDATED) — reserved so a future importer can start emitting
-- them without a schema migration, and so no other event type can
-- accidentally collide with those names in the meantime.
--
-- Also adds typed relationship columns (task doc: "Connect events to
-- watch areas, saved counties, operators, fields and wells") so
-- watch-area matching is a plain join/filter, not JSON-payload parsing.
-- `related_well_id` covers the common single-well case; a transfer that
-- links multiple wells uses `ks_event_well_link` (migration 0024)
-- instead, since a single nullable FK can't represent "many."
--
-- Rollback (plain single-file migrations, no paired .down.sql):
--   ALTER TABLE ks_event DROP COLUMN IF EXISTS related_well_id, ... (each column);
--   ALTER TABLE ks_event DROP CONSTRAINT ks_event_type_check;
--   ALTER TABLE ks_event ADD CONSTRAINT ks_event_type_check CHECK (event_type IN ('INTENT_POSTED', 'OPERATOR_TRANSFER_APPROVED'));
ALTER TABLE ks_event DROP CONSTRAINT IF EXISTS ks_event_type_check;
ALTER TABLE ks_event ADD CONSTRAINT ks_event_type_check CHECK (event_type IN (
    'INTENT_POSTED',
    'INTENT_EXPIRED',
    'SPUD_RECORDED',
    'COMPLETION_RECORDED',
    'WELL_STATUS_CHANGED',
    'PLUGGING_RECORDED',
    'OPERATOR_TRANSFER_APPROVED',
    'FIRST_LEASE_PRODUCTION_REPORTED',
    'LEASE_PRODUCTION_UPDATED',
    'DOCUMENT_ADDED',
    -- Reserved, not yet emitted by any importer in this repo (KS-14).
    'CONSERVATION_DOCKET_FILED',
    'CONSERVATION_ORDER_ISSUED',
    'UIC_REPORT_UPDATED'
));

ALTER TABLE ks_event ADD COLUMN IF NOT EXISTS related_well_id uuid REFERENCES ks_well(internal_well_id);
ALTER TABLE ks_event ADD COLUMN IF NOT EXISTS related_operator_id uuid REFERENCES ks_operator(id);
ALTER TABLE ks_event ADD COLUMN IF NOT EXISTS related_field_id uuid REFERENCES ks_field(id);
ALTER TABLE ks_event ADD COLUMN IF NOT EXISTS related_lease_id uuid REFERENCES ks_lease(id);
ALTER TABLE ks_event ADD COLUMN IF NOT EXISTS related_county text;

CREATE INDEX IF NOT EXISTS ks_event_related_well_idx ON ks_event (related_well_id) WHERE related_well_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS ks_event_related_operator_idx ON ks_event (related_operator_id) WHERE related_operator_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS ks_event_related_field_idx ON ks_event (related_field_id) WHERE related_field_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS ks_event_related_lease_idx ON ks_event (related_lease_id) WHERE related_lease_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS ks_event_related_county_idx ON ks_event (related_county) WHERE related_county IS NOT NULL;
