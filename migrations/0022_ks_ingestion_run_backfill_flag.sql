-- KS-14: marks an ingestion run as a historical baseline/backfill rather
-- than ongoing live monitoring. Task doc: "Do not emit hundreds of
-- thousands of 'new well' events from the initial historical baseline.
-- Mark baseline imports as backfills and publish only events that occur
-- or are first observed after the live-monitoring cutoff, unless a
-- deliberate historical timeline job is being run." Defaults to `false`
-- so every existing call site (KS-03 through KS-10) keeps today's
-- behavior — event-emitting materializers must opt a run INTO backfill
-- suppression explicitly, not the other way around, since only wells and
-- production are large enough to need it by default; intents/transfers
-- are naturally small rolling windows.
--
-- Rollback (plain single-file migrations, no paired .down.sql):
--   ALTER TABLE ks_ingestion_run DROP COLUMN IF EXISTS is_baseline_backfill;
ALTER TABLE ks_ingestion_run ADD COLUMN IF NOT EXISTS is_baseline_backfill boolean NOT NULL DEFAULT false;

-- The "unless a deliberate historical timeline job is being run" escape
-- hatch — an operator explicitly opts a specific backfill run INTO
-- publishing its historical events anyway (e.g. building a public
-- activity timeline from day one), rather than the suppressed default.
-- Meaningless (and ignored) on a non-backfill run, since suppression
-- itself never applies there — see `constants::should_emit_event`.
ALTER TABLE ks_ingestion_run ADD COLUMN IF NOT EXISTS force_historical_events boolean NOT NULL DEFAULT false;
