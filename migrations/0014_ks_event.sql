-- KS-06/KS-07: shared event log. Both tickets ask for the same shape
-- (generate a typed event with occurred_at/published_at/observed_at, with
-- deduplication) so this is one table, not two.
--
-- Scope note: the task-doc-level architecture instruction (§10) says to
-- reuse `futureBackend`'s shared event system where one exists, the same
-- way KS-02 reused its `jurisdictions`/`data_sources` tables rather than
-- inventing Kansas-local equivalents. That investigation was not possible
-- from this session (no access to the `futureBackend` checkout), so this
-- table is Kansas-local for now, deliberately mirroring the KS-02
-- precedent of keeping something Kansas-internal when the shared
-- equivalent can't be confirmed — see docs/architecture/kansas-source-boundary.md
-- item 37. Revisit and migrate into `futureBackend`'s event system if one
-- is confirmed to exist there.
--
-- Rollback (plain single-file migrations, no paired .down.sql):
--   DROP TABLE IF EXISTS ks_event;
CREATE TABLE IF NOT EXISTS ks_event (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    event_type text NOT NULL,
    -- Natural per-event identity for deduplication (task doc acceptance
    -- criteria: "event deduplication"). For INTENT_POSTED this is the
    -- intent's id; for OPERATOR_TRANSFER_APPROVED, the transfer's id.
    dedup_key text NOT NULL,
    occurred_at timestamptz,
    published_at timestamptz,
    observed_at timestamptz NOT NULL,
    payload jsonb NOT NULL,
    source_ingestion_run_id uuid NOT NULL REFERENCES ks_ingestion_run(id),
    created_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT ks_event_dedup_unique UNIQUE (event_type, dedup_key),
    CONSTRAINT ks_event_type_check CHECK (event_type IN ('INTENT_POSTED', 'OPERATOR_TRANSFER_APPROVED'))
);

CREATE INDEX IF NOT EXISTS ks_event_type_occurred_idx ON ks_event (event_type, occurred_at DESC);
