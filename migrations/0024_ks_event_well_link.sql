-- KS-14: multi-well event links. An `OPERATOR_TRANSFER_APPROVED` event
-- may cover several wells at once (KS-07's `ks_operator_transfer_well`
-- join table already models this on the transfer side) — this is the
-- event-side equivalent, so a well watch can match an event even when
-- `ks_event.related_well_id` (the common single-well case) is NULL.
--
-- Rollback (plain single-file migrations, no paired .down.sql):
--   DROP TABLE IF EXISTS ks_event_well_link;
CREATE TABLE IF NOT EXISTS ks_event_well_link (
    event_id uuid NOT NULL REFERENCES ks_event(id) ON DELETE CASCADE,
    well_id uuid NOT NULL REFERENCES ks_well(internal_well_id),
    PRIMARY KEY (event_id, well_id)
);

CREATE INDEX IF NOT EXISTS ks_event_well_link_well_idx ON ks_event_well_link (well_id);
