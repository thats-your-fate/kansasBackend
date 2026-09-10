-- KS-14: INTENT_EXPIRED tracking. KCC's intents page has no explicit
-- permit-expiration date (confirmed absent from the field list at
-- KS-06) — the only honest, source-grounded signal available is that an
-- intent falls out of the rolling 365-day window without a well ever
-- having been spudded for it. `consecutive_missing_snapshot_count`
-- mirrors `ks_well`'s own tombstone-after-two-misses discipline (0005),
-- applied here to avoid flagging expiration off one flaky/incomplete
-- daily page.
--
-- Rollback (plain single-file migrations, no paired .down.sql):
--   ALTER TABLE ks_drilling_intent DROP COLUMN IF EXISTS consecutive_missing_snapshot_count, is_expired, expired_at;
ALTER TABLE ks_drilling_intent ADD COLUMN IF NOT EXISTS consecutive_missing_snapshot_count integer NOT NULL DEFAULT 0;
ALTER TABLE ks_drilling_intent ADD COLUMN IF NOT EXISTS is_expired boolean NOT NULL DEFAULT false;
ALTER TABLE ks_drilling_intent ADD COLUMN IF NOT EXISTS expired_at timestamptz;

ALTER TABLE ks_drilling_intent ADD CONSTRAINT ks_drilling_intent_missing_count_check CHECK (consecutive_missing_snapshot_count >= 0);
