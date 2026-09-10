//! KS-02: ingestion-run lineage and the idempotency/partial-replace
//! guarantees the source registry requires. No canonical entity tables
//! exist yet (KS-005+ add those) — this module is exercised end-to-end by
//! its own tests today, ahead of any real parser using it, so the
//! contract is settled before KS-005 needs to build against it.

use chrono::{DateTime, NaiveDate, Utc};
use sqlx::PgPool;
use uuid::Uuid;

#[derive(Debug, thiserror::Error)]
pub enum IngestionRunError {
    #[error("database error: {0}")]
    Database(#[from] sqlx::Error),
    #[error("run {run_id} is not in a state that can transition to {target}")]
    InvalidTransition { run_id: Uuid, target: &'static str },
}

/// Describes a downloaded payload immediately after its checksum is known
/// — the point at which a caller must check `find_completed_run_by_checksum`
/// before doing any parse/materialize work.
pub struct DownloadedPayload<'a> {
    pub source_id: Uuid,
    pub source_url: &'a str,
    pub retrieved_at: DateTime<Utc>,
    pub source_modified_at: Option<DateTime<Utc>>,
    pub payload_checksum: &'a str,
    pub schema_fingerprint: Option<&'a str>,
    pub parser_version: &'a str,
}

/// KS-14: `(is_baseline_backfill, force_historical_events)` for a run —
/// every event-emitting materializer reads this once per run and passes
/// it to `constants::should_emit_event` at each event-emission site,
/// rather than each materializer re-deriving it independently.
pub async fn event_policy(pool: &PgPool, run_id: Uuid) -> Result<(bool, bool), sqlx::Error> {
    sqlx::query_as(
        "SELECT is_baseline_backfill, force_historical_events FROM ks_ingestion_run WHERE id = $1",
    )
    .bind(run_id)
    .fetch_one(pool)
    .await
}

/// Marks a run as a historical baseline/backfill (task doc: "Mark
/// baseline imports as backfills"). Callers set this immediately after
/// `start_run`, before the snapshot's own parse/stage work — a deliberate
/// choice at import time, not something inferred after the fact.
pub async fn mark_as_backfill(pool: &PgPool, run_id: Uuid) -> Result<(), sqlx::Error> {
    sqlx::query("UPDATE ks_ingestion_run SET is_baseline_backfill = true WHERE id = $1")
        .bind(run_id)
        .execute(pool)
        .await?;
    Ok(())
}

/// The task doc's "deliberate historical timeline job" escape hatch: an
/// operator explicitly opts a backfill run INTO publishing its historical
/// events (e.g. seeding a public activity timeline from day one) rather
/// than the suppressed default. Meaningless on a run that isn't also
/// marked as a backfill.
pub async fn mark_as_force_historical(pool: &PgPool, run_id: Uuid) -> Result<(), sqlx::Error> {
    sqlx::query("UPDATE ks_ingestion_run SET force_historical_events = true WHERE id = $1")
        .bind(run_id)
        .execute(pool)
        .await?;
    Ok(())
}

/// KS-17: "source HTTP/content-type changes" monitoring needs a real
/// observed value to compare against. Optional and separate from
/// `start_run` deliberately — threading a new required field through
/// `DownloadedPayload` would touch every existing importer's call site
/// for a column most of them have no cheap way to populate yet (some
/// don't go through `reqwest` directly, e.g. the ArcGIS/ZIP paths use
/// their own response handling). Callers that already hold the raw HTTP
/// response call this once, after `start_run`; callers that don't are
/// simply not part of this check's coverage yet — `monitoring::
/// content_type_report` says so explicitly rather than fabricating "no
/// change" for a source that has never recorded one.
pub async fn record_response_content_type(
    pool: &PgPool,
    run_id: Uuid,
    content_type: &str,
) -> Result<(), sqlx::Error> {
    sqlx::query("UPDATE ks_ingestion_run SET response_content_type = $2 WHERE id = $1")
        .bind(run_id)
        .bind(content_type)
        .execute(pool)
        .await?;
    Ok(())
}

/// The idempotency check KS-02 requires: "identical snapshots are
/// idempotent." Returns the existing run's id if a `completed` run with
/// this exact (source, checksum) pair already exists — callers must skip
/// parsing/materializing entirely in that case, not just skip re-download.
pub async fn find_completed_run_by_checksum(
    pool: &PgPool,
    source_id: Uuid,
    payload_checksum: &str,
) -> Result<Option<Uuid>, sqlx::Error> {
    sqlx::query_scalar::<_, Uuid>(
        r#"
        SELECT id FROM ks_ingestion_run
        WHERE source_id = $1 AND payload_checksum = $2 AND status = 'completed'
        "#,
    )
    .bind(source_id)
    .bind(payload_checksum)
    .fetch_optional(pool)
    .await
}

/// Starts a new run in `running` status. The checksum is already known at
/// this point (computed from the downloaded payload) — `running` is the
/// initial state, not `pending`, because there is nothing to do between
/// "payload downloaded" and "validating/materializing it."
///
/// The `(source_id, payload_checksum)` unique constraint means calling
/// this twice for the same unchanged payload fails with a database
/// uniqueness error rather than silently duplicating history — callers
/// must call `find_completed_run_by_checksum` first.
pub async fn start_run(pool: &PgPool, payload: DownloadedPayload<'_>) -> Result<Uuid, sqlx::Error> {
    sqlx::query_scalar::<_, Uuid>(
        r#"
        INSERT INTO ks_ingestion_run (
            source_id, source_url, retrieved_at, source_modified_at,
            payload_checksum, schema_fingerprint, parser_version, status
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, 'running')
        RETURNING id
        "#,
    )
    .bind(payload.source_id)
    .bind(payload.source_url)
    .bind(payload.retrieved_at)
    .bind(payload.source_modified_at)
    .bind(payload.payload_checksum)
    .bind(payload.schema_fingerprint)
    .bind(payload.parser_version)
    .fetch_one(pool)
    .await
}

/// Transitions a `running` run to `completed`. This is the ONLY function
/// that can mark a run `completed`, and only succeeds if the row is
/// currently `running` — enforced by the `WHERE` clause, not just
/// documented, so a run can never jump `pending -> completed` or be
/// completed twice. Future materializers (KS-005+) must only treat a
/// `completed` run's data as authoritative for canonical tables — a
/// `failed` run's partial writes (if any exist at the parse/staging layer)
/// must never be promoted.
pub async fn complete_run(
    pool: &PgPool,
    run_id: Uuid,
    row_count: i64,
    rejected_row_count: i64,
    coverage_start: Option<NaiveDate>,
    coverage_end: Option<NaiveDate>,
    manifest: serde_json::Value,
) -> Result<(), IngestionRunError> {
    let result = sqlx::query(
        r#"
        UPDATE ks_ingestion_run
        SET status = 'completed', row_count = $2, rejected_row_count = $3,
            coverage_start = $4, coverage_end = $5, manifest = $6,
            completed_at = now(), updated_at = now()
        WHERE id = $1 AND status = 'running'
        "#,
    )
    .bind(run_id)
    .bind(row_count)
    .bind(rejected_row_count)
    .bind(coverage_start)
    .bind(coverage_end)
    .bind(manifest)
    .execute(pool)
    .await?;

    if result.rows_affected() == 0 {
        return Err(IngestionRunError::InvalidTransition {
            run_id,
            target: "completed",
        });
    }
    Ok(())
}

/// Transitions a `running` run to `failed`. Same not-just-documented
/// guarantee as `complete_run`: only a `running` row can fail this way.
/// Use this for execution errors (network failure, parse crash) — use
/// `reject_run` for a run that executed successfully but failed its own
/// QA gate (count tolerance exceeded, source changed mid-import).
pub async fn fail_run(
    pool: &PgPool,
    run_id: Uuid,
    failure_message: &str,
) -> Result<(), IngestionRunError> {
    let result = sqlx::query(
        r#"
        UPDATE ks_ingestion_run
        SET status = 'failed', failure_message = $2, completed_at = now(), updated_at = now()
        WHERE id = $1 AND status = 'running'
        "#,
    )
    .bind(run_id)
    .bind(failure_message)
    .execute(pool)
    .await?;

    if result.rows_affected() == 0 {
        return Err(IngestionRunError::InvalidTransition {
            run_id,
            target: "failed",
        });
    }
    Ok(())
}

/// Transitions a `running` run to `rejected` — the run executed (data was
/// fetched and staged) but failed its own QA gate before publish: the
/// source changed mid-import, the extracted count fell outside the
/// configured tolerance, or an object-ID range was found missing. Staged
/// rows in `ks_well_raw_snapshot` for a `rejected` run must never be read
/// by a materializer, exactly like a `failed` run's.
pub async fn reject_run(
    pool: &PgPool,
    run_id: Uuid,
    reason: &str,
    manifest: serde_json::Value,
) -> Result<(), IngestionRunError> {
    let result = sqlx::query(
        r#"
        UPDATE ks_ingestion_run
        SET status = 'rejected', failure_message = $2, manifest = $3,
            completed_at = now(), updated_at = now()
        WHERE id = $1 AND status = 'running'
        "#,
    )
    .bind(run_id)
    .bind(reason)
    .bind(manifest)
    .execute(pool)
    .await?;

    if result.rows_affected() == 0 {
        return Err(IngestionRunError::InvalidTransition {
            run_id,
            target: "rejected",
        });
    }
    Ok(())
}

/// Records one rejected/invalid raw record against a run. `row_count` on
/// the run captures the aggregate; this table is for debugging a specific
/// rejection.
pub async fn record_rejection(
    pool: &PgPool,
    run_id: Uuid,
    row_number: Option<i64>,
    raw_data: Option<serde_json::Value>,
    reason: &str,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        r#"
        INSERT INTO ks_source_rejection (ingestion_run_id, row_number, raw_data, reason)
        VALUES ($1, $2, $3, $4)
        "#,
    )
    .bind(run_id)
    .bind(row_number)
    .bind(raw_data)
    .bind(reason)
    .execute(pool)
    .await?;
    Ok(())
}
