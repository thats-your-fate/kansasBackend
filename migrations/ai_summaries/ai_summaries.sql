-- Experimental AI well summaries (superadmin only, drafts until reviewed).
-- Idempotent and kept out of the numbered sqlx migrations so an older release
-- without this file can still start against the same database.

CREATE TABLE IF NOT EXISTS ks_ai_summary_runs (
    id BIGSERIAL PRIMARY KEY,
    status TEXT NOT NULL CHECK (status IN (
        'running', 'cancelling', 'cancelled', 'completed', 'failed', 'interrupted'
    )),
    endpoint_url TEXT NOT NULL,
    model TEXT NOT NULL,
    prompt_version TEXT NOT NULL,
    activity_since DATE NOT NULL,
    well_limit INTEGER NOT NULL CHECK (well_limit > 0),
    concurrency INTEGER NOT NULL CHECK (concurrency > 0),
    force_regenerate BOOLEAN NOT NULL DEFAULT false,
    selected_count INTEGER NOT NULL DEFAULT 0,
    succeeded_count INTEGER NOT NULL DEFAULT 0,
    failed_count INTEGER NOT NULL DEFAULT 0,
    skipped_count INTEGER NOT NULL DEFAULT 0,
    prompt_tokens BIGINT NOT NULL DEFAULT 0,
    completion_tokens BIGINT NOT NULL DEFAULT 0,
    created_by TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    started_at TIMESTAMPTZ,
    finished_at TIMESTAMPTZ,
    last_error TEXT
);

CREATE TABLE IF NOT EXISTS ks_ai_summary_run_items (
    run_id BIGINT NOT NULL REFERENCES ks_ai_summary_runs(id) ON DELETE CASCADE,
    well_id UUID NOT NULL,
    lookup_api TEXT NOT NULL,
    latest_activity_date DATE,
    status TEXT NOT NULL DEFAULT 'pending' CHECK (status IN (
        'pending', 'running', 'succeeded', 'failed', 'skipped'
    )),
    attempts INTEGER NOT NULL DEFAULT 0,
    error TEXT,
    latency_ms INTEGER,
    summary_id BIGINT,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (run_id, well_id)
);
CREATE INDEX IF NOT EXISTS idx_ks_ai_summary_run_items_status
    ON ks_ai_summary_run_items(run_id, status);

CREATE TABLE IF NOT EXISTS ks_well_ai_summaries (
    id BIGSERIAL PRIMARY KEY,
    well_id UUID NOT NULL,
    lookup_api TEXT NOT NULL,
    run_id BIGINT REFERENCES ks_ai_summary_runs(id) ON DELETE SET NULL,
    model TEXT NOT NULL,
    prompt_version TEXT NOT NULL,
    input_sha256 TEXT NOT NULL,
    input_json JSONB NOT NULL,
    summary TEXT NOT NULL,
    finish_reason TEXT,
    prompt_tokens INTEGER,
    completion_tokens INTEGER,
    latency_ms INTEGER,
    review_status TEXT NOT NULL DEFAULT 'draft'
        CHECK (review_status IN ('draft', 'approved', 'rejected')),
    reviewed_by TEXT,
    reviewed_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (well_id, model, prompt_version, input_sha256)
);
CREATE INDEX IF NOT EXISTS idx_ks_well_ai_summaries_well
    ON ks_well_ai_summaries(well_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_ks_well_ai_summaries_run
    ON ks_well_ai_summaries(run_id, id DESC);
CREATE INDEX IF NOT EXISTS idx_ks_well_ai_summaries_approved_api
    ON ks_well_ai_summaries(lookup_api, reviewed_at DESC NULLS LAST, id DESC)
    WHERE review_status = 'approved';

CREATE TABLE IF NOT EXISTS ks_ai_summary_log (
    id BIGSERIAL PRIMARY KEY,
    run_id BIGINT NOT NULL REFERENCES ks_ai_summary_runs(id) ON DELETE CASCADE,
    logged_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    level TEXT NOT NULL CHECK (level IN ('info', 'warn', 'error')),
    lookup_api TEXT,
    message TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_ks_ai_summary_log_run
    ON ks_ai_summary_log(run_id, id);

ALTER TABLE ks_well_ai_summaries
    ADD COLUMN IF NOT EXISTS source_fingerprint TEXT;
