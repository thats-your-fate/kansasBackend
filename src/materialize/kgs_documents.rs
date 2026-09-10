//! KS-15: materializes `ks_well_document_raw_snapshot` rows into the
//! canonical, idempotent `ks_well_document` metadata index, resolves each
//! document's well relationship by KID/API (evidence, not a guarantee —
//! a document is still valuable metadata even unresolved), and emits a
//! deduplicated `DOCUMENT_ADDED` event per newly-indexed document
//! (subject to KS-14's baseline-backfill suppression). Never downloads a
//! document body — see `enqueue_for_download`/`next_queued_documents` for
//! the task doc's "controlled queue" requirement, and this module's own
//! `public_access_state` handling for why a restricted/login-required
//! document can never reach the queue at all.

use sqlx::PgPool;
use uuid::Uuid;

use crate::constants::{self, should_emit_event};
use crate::event::{self, NewEvent};
use crate::source::ingestion_run;

#[derive(Debug, thiserror::Error)]
pub enum MaterializeError {
    #[error("database error: {0}")]
    Database(#[from] sqlx::Error),
    #[error("ingestion run {0} not found")]
    RunNotFound(Uuid),
    #[error("ingestion run {0} has status '{1}', not 'completed' — refusing to materialize from a non-published run")]
    RunNotCompleted(Uuid, String),
    #[error("document {0} is not public — cannot be queued for download")]
    NotPublic(Uuid),
}

#[derive(Debug, Default, Clone, PartialEq)]
pub struct MaterializeOutcome {
    pub created: i64,
    pub updated: i64,
    pub events_emitted: i64,
    pub unresolved: i64,
}

#[derive(Debug, sqlx::FromRow)]
struct RawSnapshotRow {
    document_source: String,
    source_document_id: Option<String>,
    raw_data: serde_json::Value,
    source_record_hash: String,
}

pub async fn materialize_documents_from_run(
    pool: &PgPool,
    ingestion_run_id: Uuid,
) -> Result<MaterializeOutcome, MaterializeError> {
    let status: Option<String> =
        sqlx::query_scalar("SELECT status FROM ks_ingestion_run WHERE id = $1")
            .bind(ingestion_run_id)
            .fetch_optional(pool)
            .await?;
    let status = status.ok_or(MaterializeError::RunNotFound(ingestion_run_id))?;
    if status != "completed" {
        return Err(MaterializeError::RunNotCompleted(ingestion_run_id, status));
    }

    let rows: Vec<RawSnapshotRow> = sqlx::query_as(
        r#"
        SELECT document_source, source_document_id, raw_data, source_record_hash
        FROM ks_well_document_raw_snapshot
        WHERE ingestion_run_id = $1 AND rejection_reason IS NULL AND source_document_id IS NOT NULL
        "#,
    )
    .bind(ingestion_run_id)
    .fetch_all(pool)
    .await?;

    let (is_baseline_backfill, force_historical) =
        ingestion_run::event_policy(pool, ingestion_run_id).await?;
    let cutoff = constants::live_monitoring_cutoff();

    let mut outcome = MaterializeOutcome::default();

    for row in rows {
        let source_document_id = row
            .source_document_id
            .expect("filtered by WHERE source_document_id IS NOT NULL");
        let document_type_raw = row
            .raw_data
            .get("document_type_raw")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_owned();
        let document_type =
            crate::normalize::kgs_documents::classify_document_type(&document_type_raw);
        let kgs_kid = row
            .raw_data
            .get("kgs_kid")
            .and_then(|v| v.as_str())
            .map(str::to_owned);
        let api_number_raw = row
            .raw_data
            .get("api_number_raw")
            .and_then(|v| v.as_str())
            .map(str::to_owned);
        let source_url = row
            .raw_data
            .get("source_url")
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .to_owned();
        let filing_date_raw = row
            .raw_data
            .get("filing_date_raw")
            .and_then(|v| v.as_str())
            .map(str::to_owned);
        let filing_date = filing_date_raw.as_deref().and_then(parse_kgs_document_date);
        let access_state = row
            .raw_data
            .get("access_state")
            .and_then(|v| v.as_str())
            .unwrap_or("unavailable")
            .to_owned();
        let parser_status = if access_state == "public" {
            "not_queued"
        } else {
            "skipped_restricted"
        };

        let well_id: Option<Uuid> = if let Some(kid) = kgs_kid.as_deref() {
            sqlx::query_scalar("SELECT internal_well_id FROM ks_well WHERE kgs_kid = $1")
                .bind(kid)
                .fetch_optional(pool)
                .await?
        } else {
            None
        };
        if well_id.is_none() {
            outcome.unresolved += 1;
        }

        let (document_id, is_new): (Uuid, bool) = sqlx::query_as(
            r#"
            INSERT INTO ks_well_document (
                document_source, source_document_id, document_type, document_type_raw,
                well_id, api_number_raw, kgs_kid, source_url, filing_date, filing_date_raw,
                public_access_state, parser_status, source_ingestion_run_id, source_record_hash
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
            ON CONFLICT (document_source, source_document_id) DO UPDATE SET
                document_type = EXCLUDED.document_type,
                document_type_raw = EXCLUDED.document_type_raw,
                well_id = COALESCE(EXCLUDED.well_id, ks_well_document.well_id),
                api_number_raw = EXCLUDED.api_number_raw,
                kgs_kid = EXCLUDED.kgs_kid,
                source_url = EXCLUDED.source_url,
                filing_date = EXCLUDED.filing_date,
                filing_date_raw = EXCLUDED.filing_date_raw,
                public_access_state = EXCLUDED.public_access_state,
                source_ingestion_run_id = EXCLUDED.source_ingestion_run_id,
                source_record_hash = EXCLUDED.source_record_hash,
                last_observed_at = now(),
                updated_at = now()
            RETURNING id, (xmax = 0)
            "#,
        )
        .bind(&row.document_source)
        .bind(&source_document_id)
        .bind(document_type.as_db_str())
        .bind(&document_type_raw)
        .bind(well_id)
        .bind(&api_number_raw)
        .bind(&kgs_kid)
        .bind(&source_url)
        .bind(filing_date)
        .bind(&filing_date_raw)
        .bind(&access_state)
        .bind(parser_status)
        .bind(ingestion_run_id)
        .bind(&row.source_record_hash)
        .fetch_one(pool)
        .await?;

        if is_new {
            outcome.created += 1;
            let occurred_at = filing_date
                .and_then(|d| d.and_hms_opt(0, 0, 0))
                .map(|dt| dt.and_utc());
            if should_emit_event(is_baseline_backfill, occurred_at, cutoff, force_historical) {
                let inserted = event::record_event(
                    pool,
                    NewEvent {
                        event_type: event::DOCUMENT_ADDED,
                        dedup_key: &document_id.to_string(),
                        occurred_at,
                        published_at: None,
                        observed_at: chrono::Utc::now(),
                        payload: serde_json::json!({
                            "document_id": document_id,
                            "document_type": document_type.as_db_str(),
                            "document_source": row.document_source,
                        }),
                        source_ingestion_run_id: ingestion_run_id,
                        related_well_id: well_id,
                        related_operator_id: None,
                        related_field_id: None,
                        related_lease_id: None,
                        related_county: None,
                        related_well_ids: &[],
                    },
                )
                .await?;
                if inserted {
                    outcome.events_emitted += 1;
                }
            }
        } else {
            outcome.updated += 1;
        }
    }

    Ok(outcome)
}

/// KGS's real well-detail page renders dates like `Apr-15-1999`
/// (confirmed live, KS-15 discovery) — a different shape than the
/// `MM/DD/YYYY` convention `normalize::parse_us_date` handles for
/// KCC/ArcGIS sources.
fn parse_kgs_document_date(raw: &str) -> Option<chrono::NaiveDate> {
    chrono::NaiveDate::parse_from_str(raw.trim(), "%b-%d-%Y").ok()
}

#[derive(Debug, Clone, sqlx::FromRow)]
pub struct UnresolvedDocument {
    pub id: Uuid,
    pub document_source: String,
    pub source_document_id: String,
    pub kgs_kid: Option<String>,
    pub api_number_raw: Option<String>,
    pub first_observed_at: chrono::DateTime<chrono::Utc>,
}

/// "Source links remain available when extraction fails" / unresolved
/// well relationships — a queryable report, same pattern as every other
/// unresolved-match report in this repo.
pub async fn unresolved_documents_report(
    pool: &PgPool,
    limit: i64,
) -> Result<Vec<UnresolvedDocument>, sqlx::Error> {
    sqlx::query_as(
        r#"
        SELECT id, document_source, source_document_id, kgs_kid, api_number_raw, first_observed_at
        FROM ks_well_document
        WHERE well_id IS NULL
        ORDER BY first_observed_at DESC
        LIMIT $1
        "#,
    )
    .bind(limit)
    .fetch_all(pool)
    .await
}

#[derive(Debug, Clone, sqlx::FromRow)]
pub struct QueuedDocument {
    pub id: Uuid,
    pub document_source: String,
    pub source_document_id: String,
    pub source_url: String,
    pub document_type: String,
}

/// KS-15's "controlled queue for documents required by product views or
/// downstream extraction" — a document is only ever placed here
/// explicitly (never automatically at metadata-index time), and only if
/// it was observed as `public` (enforced by the DB CHECK constraint in
/// migration `0027`, not just this function's own guard).
pub async fn enqueue_for_download(
    pool: &PgPool,
    document_id: Uuid,
) -> Result<(), MaterializeError> {
    let updated = sqlx::query("UPDATE ks_well_document SET parser_status = 'queued', updated_at = now() WHERE id = $1 AND public_access_state = 'public' AND parser_status = 'not_queued'")
        .bind(document_id)
        .execute(pool)
        .await?;
    if updated.rows_affected() == 0 {
        return Err(MaterializeError::NotPublic(document_id));
    }
    Ok(())
}

pub async fn next_queued_documents(
    pool: &PgPool,
    limit: i64,
) -> Result<Vec<QueuedDocument>, sqlx::Error> {
    sqlx::query_as("SELECT id, document_source, source_document_id, source_url, document_type FROM ks_well_document WHERE parser_status = 'queued' ORDER BY first_observed_at LIMIT $1")
        .bind(limit)
        .fetch_all(pool)
        .await
}

/// Records a successful download+extraction. `content_hash` makes a
/// re-download of unchanged content detectable without re-parsing.
pub async fn mark_downloaded(
    pool: &PgPool,
    document_id: Uuid,
    content_hash: &str,
    object_storage_key: Option<&str>,
) -> Result<(), sqlx::Error> {
    sqlx::query("UPDATE ks_well_document SET parser_status = 'extracted', content_hash = $2, content_object_storage_key = $3, updated_at = now() WHERE id = $1")
        .bind(document_id)
        .bind(content_hash)
        .bind(object_storage_key)
        .execute(pool)
        .await?;
    Ok(())
}

/// Task doc: "source links remain available when extraction fails" —
/// `source_url`/metadata are untouched; only `parser_status`/the failure
/// reason change, so the document is still fully visible and its
/// original source link still usable even though extraction didn't work.
pub async fn mark_extraction_failed(
    pool: &PgPool,
    document_id: Uuid,
    reason: &str,
) -> Result<(), sqlx::Error> {
    sqlx::query("UPDATE ks_well_document SET parser_status = 'failed', parser_failure_reason = $2, updated_at = now() WHERE id = $1")
        .bind(document_id)
        .bind(reason)
        .execute(pool)
        .await?;
    Ok(())
}
