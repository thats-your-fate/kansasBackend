//! KS-05: the KCC operator-license importer. Discovers the current
//! "active operators" and "all operators" download links from the KCC
//! operator page's anchors (`source::kcc_html::discover_operator_links`)
//! rather than embedding an undocumented filename, downloads each, and
//! stages every parsed row.
//!
//! **Unverified against the live service** (task doc §11's KS-05 entry):
//! `kcc.ks.gov` is confirmed unreachable from this development
//! environment (curl/nslookup/WebFetch DNS failures, checked twice across
//! two sessions). This module is built and tested against a hand-built
//! fixture page/CSV modeled on KCC's documented conventions, not the real
//! service — unlike KS-03/KS-04, which were verified live. Confirm
//! against a real fetch before production use.

pub mod csv_source;

use chrono::Utc;
use sha2::{Digest, Sha256};
use sqlx::PgPool;
use uuid::Uuid;

use crate::constants;
use crate::source::ingestion_run::{self, DownloadedPayload, IngestionRunError};
use crate::source::kcc_html::{self, DiscoveryError};
use crate::source::registry;
use crate::source::retry::{with_retry, RetryPolicy};

use csv_source::{parse_operator_csv, CsvParseError};

const PARSER_VERSION: &str = "kcc_operators_csv-v1";

#[derive(Debug, thiserror::Error)]
pub enum ImportError {
    #[error("http request failed: {0}")]
    Request(#[from] reqwest::Error),
    #[error("csv parse error: {0}")]
    Csv(#[from] CsvParseError),
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
    pub active_count: i64,
    pub all_count: i64,
    pub manifest: serde_json::Value,
}

pub async fn run_operator_import(
    pool: &PgPool,
    client: &reqwest::Client,
    operator_page_url: &str,
    options: ImportOptions,
) -> Result<ImportOutcome, ImportError> {
    let source = registry::find_source_by_code(pool, constants::KCC_OPERATORS)
        .await?
        .ok_or(ImportError::SourceNotFound(constants::KCC_OPERATORS))?;

    let page_html = with_retry(options.retry_policy, || {
        fetch_text(client, operator_page_url)
    })
    .await?;

    let links = match kcc_html::discover_operator_links(&page_html, operator_page_url) {
        Ok(links) => links,
        Err(err) => {
            return reject_on_discovery_failure(pool, source.id, operator_page_url, &page_html, err)
                .await
        }
    };

    let active_bytes = match &links.active_operators_url {
        Some(url) => Some(with_retry(options.retry_policy, || fetch_bytes(client, url)).await?),
        None => None,
    };
    let all_bytes = match &links.all_operators_url {
        Some(url) => Some(with_retry(options.retry_policy, || fetch_bytes(client, url)).await?),
        None => None,
    };

    let checksum = checksum_payloads(active_bytes.as_deref(), all_bytes.as_deref());

    if let Some(existing_run_id) =
        ingestion_run::find_completed_run_by_checksum(pool, source.id, &checksum).await?
    {
        return Ok(ImportOutcome {
            run_id: existing_run_id,
            status: ImportStatus::UnchangedSkipped,
            active_count: 0,
            all_count: 0,
            manifest: serde_json::json!({"note": "identical active+all payload checksum already completed; skipped re-parse"}),
        });
    }

    let run_id = ingestion_run::start_run(
        pool,
        DownloadedPayload {
            source_id: source.id,
            source_url: operator_page_url,
            retrieved_at: Utc::now(),
            source_modified_at: None,
            payload_checksum: &checksum,
            schema_fingerprint: None,
            parser_version: PARSER_VERSION,
        },
    )
    .await?;

    let active_rows = match &active_bytes {
        Some(bytes) => match parse_operator_csv(bytes) {
            Ok(rows) => rows,
            Err(err) => {
                let _ = ingestion_run::fail_run(
                    pool,
                    run_id,
                    &format!("active-operators csv parse failed: {err}"),
                )
                .await;
                return Err(err.into());
            }
        },
        None => Vec::new(),
    };
    let all_rows = match &all_bytes {
        Some(bytes) => match parse_operator_csv(bytes) {
            Ok(rows) => rows,
            Err(err) => {
                let _ = ingestion_run::fail_run(
                    pool,
                    run_id,
                    &format!("all-operators csv parse failed: {err}"),
                )
                .await;
                return Err(err.into());
            }
        },
        None => Vec::new(),
    };

    let active_count = active_rows.len() as i64;
    let all_count = all_rows.len() as i64;

    // KS-05 acceptance criteria: "source page changes generate alerts
    // rather than silently returning zero operators." Both lists came
    // back empty (a link was found, but the file it points to parsed to
    // nothing) — reject rather than publish a canonical wipe.
    if active_count == 0 && all_count == 0 {
        let manifest = serde_json::json!({
            "active_operators_url": links.active_operators_url,
            "all_operators_url": links.all_operators_url,
            "active_count": 0,
            "all_count": 0,
        });
        let reason =
            "zero operator records parsed from either download — likely a format or schema change";
        ingestion_run::reject_run(pool, run_id, reason, manifest.clone()).await?;
        return Ok(ImportOutcome {
            run_id,
            status: ImportStatus::Rejected,
            active_count: 0,
            all_count: 0,
            manifest,
        });
    }

    persist_rows(pool, run_id, "active", &active_rows).await?;
    persist_rows(pool, run_id, "all", &all_rows).await?;

    let reconciliation = reconcile(&active_rows, &all_rows);

    let manifest = serde_json::json!({
        "active_operators_url": links.active_operators_url,
        "all_operators_url": links.all_operators_url,
        "active_count": active_count,
        "all_count": all_count,
        "reconciliation": reconciliation,
    });

    ingestion_run::complete_run(
        pool,
        run_id,
        active_count + all_count,
        0,
        None,
        None,
        manifest.clone(),
    )
    .await?;

    Ok(ImportOutcome {
        run_id,
        status: ImportStatus::Completed,
        active_count,
        all_count,
        manifest,
    })
}

async fn reject_on_discovery_failure(
    pool: &PgPool,
    source_id: Uuid,
    operator_page_url: &str,
    page_html: &str,
    err: DiscoveryError,
) -> Result<ImportOutcome, ImportError> {
    let checksum = checksum_payloads(Some(page_html.as_bytes()), None);
    let run_id = ingestion_run::start_run(
        pool,
        DownloadedPayload {
            source_id,
            source_url: operator_page_url,
            retrieved_at: Utc::now(),
            source_modified_at: None,
            payload_checksum: &checksum,
            schema_fingerprint: None,
            parser_version: PARSER_VERSION,
        },
    )
    .await?;
    let manifest = serde_json::json!({"discovery_error": err.to_string()});
    let reason = format!("operator page link discovery failed: {err}");
    ingestion_run::reject_run(pool, run_id, &reason, manifest.clone()).await?;
    Ok(ImportOutcome {
        run_id,
        status: ImportStatus::Rejected,
        active_count: 0,
        all_count: 0,
        manifest,
    })
}

/// Cross-checks the two lists against each other: KCC's all-operators
/// download is expected to be a superset of active-operators. A license
/// present only in the active list is a genuine anomaly worth surfacing,
/// not silently absorbed.
fn reconcile(
    active_rows: &[csv_source::RawOperatorRow],
    all_rows: &[csv_source::RawOperatorRow],
) -> serde_json::Value {
    use std::collections::HashSet;
    let active_licenses: HashSet<&str> = active_rows
        .iter()
        .filter_map(|r| r.kcc_license_number.as_deref())
        .collect();
    let all_licenses: HashSet<&str> = all_rows
        .iter()
        .filter_map(|r| r.kcc_license_number.as_deref())
        .collect();

    let active_not_in_all: Vec<&str> = active_licenses.difference(&all_licenses).copied().collect();
    let all_not_in_active: usize = all_licenses.difference(&active_licenses).count();

    serde_json::json!({
        "active_license_count": active_licenses.len(),
        "all_license_count": all_licenses.len(),
        "active_not_in_all_count": active_not_in_all.len(),
        "active_not_in_all_sample": active_not_in_all.iter().take(20).collect::<Vec<_>>(),
        "all_not_in_active_count": all_not_in_active,
    })
}

async fn persist_rows(
    pool: &PgPool,
    run_id: Uuid,
    list_source: &'static str,
    rows: &[csv_source::RawOperatorRow],
) -> Result<(), sqlx::Error> {
    let mut tx = pool.begin().await?;
    for row in rows {
        let hash = checksum_payloads(Some(row.raw_data.to_string().as_bytes()), None);
        let rejection_reason = if row.kcc_license_number.is_none() {
            Some("no parseable license number in this row")
        } else {
            None
        };
        sqlx::query(
            r#"
            INSERT INTO ks_operator_raw_snapshot (
                ingestion_run_id, list_source, kcc_license_number, rejection_reason,
                reported_company_name, source_city, source_updated_at,
                raw_data, source_record_hash
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
            "#,
        )
        .bind(run_id)
        .bind(list_source)
        .bind(&row.kcc_license_number)
        .bind(rejection_reason)
        .bind(&row.reported_company_name)
        .bind(&row.source_city)
        .bind(row.source_updated_at)
        .bind(&row.raw_data)
        .bind(&hash)
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

async fn fetch_bytes(client: &reqwest::Client, url: &str) -> Result<Vec<u8>, reqwest::Error> {
    Ok(client
        .get(url)
        .send()
        .await?
        .error_for_status()?
        .bytes()
        .await?
        .to_vec())
}

fn checksum_payloads(a: Option<&[u8]>, b: Option<&[u8]>) -> String {
    let mut hasher = Sha256::new();
    if let Some(a) = a {
        hasher.update(a);
    }
    hasher.update(b"|");
    if let Some(b) = b {
        hasher.update(b);
    }
    format!("{:x}", hasher.finalize())
}
