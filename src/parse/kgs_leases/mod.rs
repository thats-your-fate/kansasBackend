//! KS-09: the KGS all-leases archive importer. Confirmed live 2026-09-05
//! against the real `KGS_ALL_LEASES_ZIP` (98,639 real rows) — same
//! confidence level as KS-03/KS-04/KS-08, unlike KS-05/06/07.

pub mod zip_source;

use chrono::Utc;
use sqlx::PgPool;
use uuid::Uuid;

use crate::constants;
use crate::source::ingestion_run::{self, DownloadedPayload, IngestionRunError};
use crate::source::registry;
use crate::source::retry::RetryPolicy;

use zip_source::{download_zip, parse_lease_zip, LeaseZipError, RawLeaseRow};

const PARSER_VERSION: &str = "kgs_leases_zip-v1";

#[derive(Debug, thiserror::Error)]
pub enum ImportError {
    #[error("download failed: {0}")]
    Download(#[from] reqwest::Error),
    #[error("zip download error: {0}")]
    ZipDownload(#[from] crate::parse::kgs_wells::zip_source::ZipImportError),
    #[error("zip parse error: {0}")]
    Zip(#[from] LeaseZipError),
    #[error("database error: {0}")]
    Database(#[from] sqlx::Error),
    #[error("ingestion run error: {0}")]
    IngestionRun(#[from] IngestionRunError),
    #[error("source '{0}' is not registered in ks_source")]
    SourceNotFound(&'static str),
    #[error("blocking parse task panicked: {0}")]
    JoinError(#[from] tokio::task::JoinError),
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

pub async fn run_lease_import(
    pool: &PgPool,
    client: &reqwest::Client,
    zip_url: &str,
    dest_path: &std::path::Path,
    retry_policy: RetryPolicy,
) -> Result<ImportOutcome, ImportError> {
    let source = registry::find_source_by_code(pool, constants::KGS_LEASES)
        .await?
        .ok_or(ImportError::SourceNotFound(constants::KGS_LEASES))?;

    let downloaded = download_zip(client, zip_url, dest_path, retry_policy).await?;

    if let Some(existing_run_id) =
        ingestion_run::find_completed_run_by_checksum(pool, source.id, &downloaded.sha256).await?
    {
        return Ok(ImportOutcome {
            run_id: existing_run_id,
            status: ImportStatus::UnchangedSkipped,
            staged_count: 0,
            rejected_count: 0,
            manifest: serde_json::json!({"note": "identical ZIP checksum already completed; skipped re-parse"}),
        });
    }

    let source_modified_at = downloaded
        .last_modified
        .as_deref()
        .and_then(|value| chrono::DateTime::parse_from_rfc2822(value).ok())
        .map(|dt| dt.with_timezone(&Utc));

    let run_id = ingestion_run::start_run(
        pool,
        DownloadedPayload {
            source_id: source.id,
            source_url: zip_url,
            retrieved_at: Utc::now(),
            source_modified_at,
            payload_checksum: &downloaded.sha256,
            schema_fingerprint: None,
            parser_version: PARSER_VERSION,
        },
    )
    .await?;

    let parse_path = downloaded.path.clone();
    let rows = match tokio::task::spawn_blocking(move || parse_lease_zip(&parse_path)).await {
        Ok(Ok(rows)) => rows,
        Ok(Err(err)) => {
            let _ =
                ingestion_run::fail_run(pool, run_id, &format!("lease zip parse failed: {err}"))
                    .await;
            return Err(err.into());
        }
        Err(join_err) => {
            let _ = ingestion_run::fail_run(
                pool,
                run_id,
                &format!("lease zip parse task panicked: {join_err}"),
            )
            .await;
            return Err(join_err.into());
        }
    };

    if rows.is_empty() {
        let manifest = serde_json::json!({"row_count": 0});
        let reason =
            "zero lease rows parsed — likely the archive format changed (zero-result safeguard)";
        ingestion_run::reject_run(pool, run_id, reason, manifest.clone()).await?;
        return Ok(ImportOutcome {
            run_id,
            status: ImportStatus::Rejected,
            staged_count: 0,
            rejected_count: 0,
            manifest,
        });
    }

    let rejected_count = rows.iter().filter(|r| r.rejection_reason.is_some()).count();
    let staged_count = rows.len() - rejected_count;

    persist_rows(pool, run_id, &rows).await?;

    let manifest = serde_json::json!({
        "content_length": downloaded.content_length,
        "last_modified": downloaded.last_modified,
        "row_count": rows.len(),
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
    rows: &[RawLeaseRow],
) -> Result<(), sqlx::Error> {
    use sha2::{Digest, Sha256};
    let mut tx = pool.begin().await?;
    for row in rows {
        let mut hasher = Sha256::new();
        hasher.update(row.raw_data.to_string().as_bytes());
        let hash = format!("{:x}", hasher.finalize());

        sqlx::query(
            r#"
            INSERT INTO ks_lease_raw_snapshot (
                ingestion_run_id, lease_kid, rejection_reason, reported_lease_name, dor_code, api_number_raw,
                operator, county, township, township_direction, range, range_direction, section, spot,
                source_latitude, source_longitude, produces_raw, production, year_start, year_stop,
                source_url, raw_data, source_record_hash
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23)
            "#,
        )
        .bind(run_id)
        .bind(&row.lease_kid)
        .bind(row.rejection_reason)
        .bind(&row.reported_lease_name)
        .bind(&row.dor_code)
        .bind(&row.api_number_raw)
        .bind(&row.operator)
        .bind(&row.county)
        .bind(row.township)
        .bind(&row.township_direction)
        .bind(row.range)
        .bind(&row.range_direction)
        .bind(row.section)
        .bind(&row.spot)
        .bind(row.source_latitude)
        .bind(row.source_longitude)
        .bind(&row.produces_raw)
        .bind(row.production)
        .bind(row.year_start)
        .bind(row.year_stop)
        .bind(&row.source_url)
        .bind(&row.raw_data)
        .bind(&hash)
        .execute(&mut *tx)
        .await?;
    }
    tx.commit().await
}
