//! KS-06: the KCC daily drilling-intent snapshot importer.
//!
//! **Unverified against the live service** (task doc §11's KS-06 entry,
//! `docs/ops/ks-06-kcc-intents-unverified.md`): `kcc.ks.gov` is confirmed
//! unreachable from this development environment. This module parses the
//! search-results page as an HTML table (`source::html_table`) — a
//! best-effort assumption, not a confirmed shape.

pub mod table_source;

use chrono::Utc;
use sha2::{Digest, Sha256};
use sqlx::PgPool;
use uuid::Uuid;

use crate::constants;
use crate::source::html_table::parse_first_table;
use crate::source::ingestion_run::{self, DownloadedPayload, IngestionRunError};
use crate::source::registry;
use crate::source::retry::{with_retry, RetryPolicy};

use table_source::{row_to_raw_intent, RawIntentRow};

const PARSER_VERSION: &str = "kcc_intents_html_table-v1";

#[derive(Debug, thiserror::Error)]
pub enum ImportError {
    #[error("http request failed: {0}")]
    Request(#[from] reqwest::Error),
    #[error("database error: {0}")]
    Database(#[from] sqlx::Error),
    #[error("ingestion run error: {0}")]
    IngestionRun(#[from] IngestionRunError),
    #[error("source '{0}' is not registered in ks_source")]
    SourceNotFound(&'static str),
}

#[derive(Debug, Clone, Copy, Default)]
pub struct ImportOptions {
    pub retry_policy: RetryPolicy,
}

#[derive(Debug, Clone, PartialEq)]
pub enum ImportStatus {
    Completed,
    UnchangedSkipped,
    Rejected,
}

#[derive(Debug, Clone)]
pub struct ImportOutcome {
    pub run_id: Uuid,
    pub status: ImportStatus,
    pub staged_count: i64,
    pub rejected_count: i64,
    pub manifest: serde_json::Value,
}

/// Fraction of parsed rows that may fail to yield a usable identity
/// before the whole run is rejected as a likely partial/corrupted page
/// load (task doc: "partial-result safeguards").
const MAX_REJECTED_ROW_RATIO: f64 = 0.5;

pub async fn run_intent_import(
    pool: &PgPool,
    client: &reqwest::Client,
    intents_page_url: &str,
    options: ImportOptions,
) -> Result<ImportOutcome, ImportError> {
    let source = registry::find_source_by_code(pool, constants::KCC_INTENTS)
        .await?
        .ok_or(ImportError::SourceNotFound(constants::KCC_INTENTS))?;

    let page_html = with_retry(options.retry_policy, || {
        fetch_text(client, intents_page_url)
    })
    .await?;
    let checksum = checksum_bytes(page_html.as_bytes());

    if let Some(existing_run_id) =
        ingestion_run::find_completed_run_by_checksum(pool, source.id, &checksum).await?
    {
        return Ok(ImportOutcome {
            run_id: existing_run_id,
            status: ImportStatus::UnchangedSkipped,
            staged_count: 0,
            rejected_count: 0,
            manifest: serde_json::json!({"note": "identical page checksum already completed; skipped re-parse"}),
        });
    }

    let run_id = ingestion_run::start_run(
        pool,
        DownloadedPayload {
            source_id: source.id,
            source_url: intents_page_url,
            retrieved_at: Utc::now(),
            source_modified_at: None,
            payload_checksum: &checksum,
            schema_fingerprint: None,
            parser_version: PARSER_VERSION,
        },
    )
    .await?;

    let table_rows = match parse_first_table(&page_html, intents_page_url) {
        Ok(rows) => rows,
        Err(err) => {
            let manifest = serde_json::json!({"table_parse_error": err.to_string()});
            let reason = format!("could not find a results table on the page: {err}");
            ingestion_run::reject_run(pool, run_id, &reason, manifest.clone()).await?;
            return Ok(ImportOutcome {
                run_id,
                status: ImportStatus::Rejected,
                staged_count: 0,
                rejected_count: 0,
                manifest,
            });
        }
    };

    if table_rows.is_empty() {
        let manifest = serde_json::json!({"row_count": 0});
        let reason = "zero intent rows parsed — likely the page isn't available yet or the format changed (zero-result safeguard)";
        ingestion_run::reject_run(pool, run_id, reason, manifest.clone()).await?;
        return Ok(ImportOutcome {
            run_id,
            status: ImportStatus::Rejected,
            staged_count: 0,
            rejected_count: 0,
            manifest,
        });
    }

    let observed_at = Utc::now();
    let raw_rows: Vec<RawIntentRow> = table_rows.iter().map(row_to_raw_intent).collect();
    let rejected_count = raw_rows
        .iter()
        .filter(|r| r.rejection_reason.is_some())
        .count();
    let staged_count = raw_rows.len() - rejected_count;

    let rejected_ratio = rejected_count as f64 / raw_rows.len() as f64;
    if rejected_ratio > MAX_REJECTED_ROW_RATIO {
        let manifest = serde_json::json!({
            "row_count": raw_rows.len(),
            "rejected_count": rejected_count,
            "rejected_ratio": rejected_ratio,
        });
        let reason = format!(
            "{rejected_count} of {} rows ({:.0}%) failed to parse an identity — likely a partial/incomplete page load (partial-result safeguard)",
            raw_rows.len(),
            rejected_ratio * 100.0
        );
        ingestion_run::reject_run(pool, run_id, &reason, manifest.clone()).await?;
        return Ok(ImportOutcome {
            run_id,
            status: ImportStatus::Rejected,
            staged_count: 0,
            rejected_count: rejected_count as i64,
            manifest,
        });
    }

    persist_rows(pool, run_id, &raw_rows, observed_at).await?;

    let manifest = serde_json::json!({
        "row_count": raw_rows.len(),
        "staged_count": staged_count,
        "rejected_count": rejected_count,
    });
    ingestion_run::complete_run(
        pool,
        run_id,
        staged_count as i64,
        rejected_count as i64,
        None,
        None,
        manifest.clone(),
    )
    .await?;

    Ok(ImportOutcome {
        run_id,
        status: ImportStatus::Completed,
        staged_count: staged_count as i64,
        rejected_count: rejected_count as i64,
        manifest,
    })
}

async fn persist_rows(
    pool: &PgPool,
    run_id: Uuid,
    rows: &[RawIntentRow],
    observed_at: chrono::DateTime<Utc>,
) -> Result<(), sqlx::Error> {
    let mut tx = pool.begin().await?;
    for row in rows {
        let hash = checksum_bytes(row.raw_data.to_string().as_bytes());
        sqlx::query(
            r#"
            INSERT INTO ks_intent_raw_snapshot (
                ingestion_run_id, source_filename, rejection_reason, permit_date, kcc_district, county,
                section, township, range, range_direction, operator_license, source_document_url,
                reported_lease_or_well_name, raw_data, source_record_hash, observed_at
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16)
            "#,
        )
        .bind(run_id)
        .bind(&row.source_filename)
        .bind(row.rejection_reason)
        .bind(row.permit_date_raw.as_deref().and_then(|d| crate::normalize::parse_us_date(Some(d))))
        .bind(&row.kcc_district)
        .bind(&row.county)
        .bind(row.section)
        .bind(row.township)
        .bind(row.range)
        .bind(&row.range_direction)
        .bind(&row.operator_license)
        .bind(&row.source_document_url)
        .bind(&row.reported_lease_or_well_name)
        .bind(&row.raw_data)
        .bind(&hash)
        .bind(observed_at)
        .execute(&mut *tx)
        .await?;
    }
    tx.commit().await
}

async fn fetch_text(client: &reqwest::Client, url: &str) -> Result<String, reqwest::Error> {
    client
        .get(url)
        .send()
        .await?
        .error_for_status()?
        .text()
        .await
}

fn checksum_bytes(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    format!("{:x}", hasher.finalize())
}
