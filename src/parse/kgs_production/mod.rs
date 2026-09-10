//! KS-10: the KGS monthly lease-production importer. Confirmed live
//! 2026-09-05 against the real decade archives — same confidence level
//! as KS-08/KS-09, unlike KS-05/06/07. Real row counts are large (the
//! 2020-present oil file alone has ~670K rows), so staging uses batched
//! multi-row `INSERT`s (`sqlx::QueryBuilder::push_values`) rather than
//! one statement per row — the scaling problem KS-08/KS-09 documented and
//! deliberately left unfixed would make this importer impractical at
//! real scale otherwise.

pub mod zip_source;

use chrono::Utc;
use sqlx::PgPool;
use uuid::Uuid;

use crate::constants::{self, classify_production_month, is_production_year_ingestible};
use crate::normalize::kgs_production::{parse_product, ProductionProduct};
use crate::source::ingestion_run::{self, DownloadedPayload, IngestionRunError};
use crate::source::registry;
use crate::source::retry::RetryPolicy;

use zip_source::{stream_production_csv, ProductionZipError, RawProductionRow};

const PARSER_VERSION: &str = "kgs_production_zip-v1";
const DEFAULT_BATCH_SIZE: usize = 1000;

#[derive(Debug, thiserror::Error)]
pub enum ImportError {
    #[error("download failed: {0}")]
    Download(#[from] reqwest::Error),
    #[error("zip download error: {0}")]
    ZipDownload(#[from] crate::parse::kgs_wells::zip_source::ZipImportError),
    #[error("zip parse error: {0}")]
    Zip(#[from] ProductionZipError),
    #[error("database error: {0}")]
    Database(#[from] sqlx::Error),
    #[error("ingestion run error: {0}")]
    IngestionRun(#[from] IngestionRunError),
    #[error("source '{0}' is not registered in ks_source")]
    SourceNotFound(&'static str),
    #[error("'{0}' is not a recognized oil/gas production filename")]
    UnrecognizedFilename(String),
    #[error("blocking parse task panicked: {0}")]
    JoinError(#[from] tokio::task::JoinError),
}

#[derive(Debug, Clone, Copy)]
pub struct ImportOptions {
    pub retry_policy: RetryPolicy,
    pub batch_size: usize,
    pub legal_review_override_enabled: bool,
}

impl Default for ImportOptions {
    fn default() -> Self {
        Self {
            retry_policy: RetryPolicy::default(),
            batch_size: DEFAULT_BATCH_SIZE,
            legal_review_override_enabled: false,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum ImportStatus {
    Completed,
    UnchangedSkipped,
    Rejected,
}

#[derive(Debug, Clone)]
pub struct FileImportOutcome {
    pub source_file: &'static str,
    pub run_id: Uuid,
    pub status: ImportStatus,
    pub staged_count: i64,
    pub rejected_count: i64,
    pub pre_1990_blocked_count: i64,
    pub manifest: serde_json::Value,
}

fn product_from_filename(filename: &str) -> Result<ProductionProduct, ImportError> {
    if filename.starts_with("oil_") {
        Ok(ProductionProduct::Oil)
    } else if filename.starts_with("gas_") {
        Ok(ProductionProduct::Gas)
    } else {
        Err(ImportError::UnrecognizedFilename(filename.to_string()))
    }
}

/// Runs all 8 configured decade files (task doc: 2020-present, 2010-2019,
/// 2000-2009, 1990-1999, oil and gas each). A single file's failure
/// (network error, malformed archive) does not abort the rest — each
/// entry in the returned `Vec` is independent, matching migration
/// `0019`'s "a bad file doesn't block the other seven" design note.
pub async fn run_all_production_files(
    pool: &PgPool,
    client: &reqwest::Client,
    archive_base: &str,
    dest_dir: &std::path::Path,
    options: ImportOptions,
) -> Vec<(&'static str, Result<FileImportOutcome, ImportError>)> {
    let urls = constants::production_file_urls(archive_base);
    let mut results = Vec::with_capacity(constants::KGS_PRODUCTION_FILES.len());
    for (filename, url) in constants::KGS_PRODUCTION_FILES.iter().zip(urls.iter()) {
        let dest = dest_dir.join(filename);
        let outcome = run_single_file_import(pool, client, url, filename, &dest, options).await;
        results.push((*filename, outcome));
    }
    results
}

pub async fn run_single_file_import(
    pool: &PgPool,
    client: &reqwest::Client,
    file_url: &str,
    filename: &'static str,
    dest_path: &std::path::Path,
    options: ImportOptions,
) -> Result<FileImportOutcome, ImportError> {
    let product = product_from_filename(filename)?;
    let source_code = match product {
        ProductionProduct::Oil => constants::KGS_PRODUCTION_OIL,
        ProductionProduct::Gas => constants::KGS_PRODUCTION_GAS,
    };
    let source = registry::find_source_by_code(pool, source_code)
        .await?
        .ok_or(ImportError::SourceNotFound(source_code))?;

    let downloaded = crate::parse::kgs_wells::zip_source::download_zip(
        client,
        file_url,
        dest_path,
        options.retry_policy,
    )
    .await?;

    if let Some(existing_run_id) =
        ingestion_run::find_completed_run_by_checksum(pool, source.id, &downloaded.sha256).await?
    {
        return Ok(FileImportOutcome {
            source_file: filename,
            run_id: existing_run_id,
            status: ImportStatus::UnchangedSkipped,
            staged_count: 0,
            rejected_count: 0,
            pre_1990_blocked_count: 0,
            manifest: serde_json::json!({"note": "identical ZIP checksum already completed; skipped re-parse"}),
        });
    }

    let source_modified_at = downloaded
        .last_modified
        .as_deref()
        .and_then(|v| chrono::DateTime::parse_from_rfc2822(v).ok())
        .map(|dt| dt.with_timezone(&Utc));

    let run_id = ingestion_run::start_run(
        pool,
        DownloadedPayload {
            source_id: source.id,
            source_url: file_url,
            retrieved_at: Utc::now(),
            source_modified_at,
            payload_checksum: &downloaded.sha256,
            schema_fingerprint: None,
            parser_version: PARSER_VERSION,
        },
    )
    .await?;

    let (tx, mut rx) = tokio::sync::mpsc::channel::<Vec<RawProductionRow>>(4);
    let parse_path = downloaded.path.clone();
    let batch_size = options.batch_size;
    let parse_handle = tokio::task::spawn_blocking(move || {
        stream_production_csv(&parse_path, batch_size, |batch| {
            tx.blocking_send(batch).map_err(|_| {
                ProductionZipError::Io(std::io::Error::other("staging receiver dropped"))
            })
        })
    });

    let mut staged_count: i64 = 0;
    let mut rejected_count: i64 = 0;
    let mut pre_1990_blocked_count: i64 = 0;

    while let Some(mut batch) = rx.recv().await {
        for row in &mut batch {
            classify_row(
                row,
                product,
                options.legal_review_override_enabled,
                &mut pre_1990_blocked_count,
            );
        }
        let (staged, rejected) = persist_batch(pool, run_id, filename, &batch).await?;
        staged_count += staged;
        rejected_count += rejected;
    }

    match parse_handle.await {
        Ok(Ok(())) => {}
        Ok(Err(err)) => {
            let _ = ingestion_run::fail_run(
                pool,
                run_id,
                &format!("production file parse failed: {err}"),
            )
            .await;
            return Err(err.into());
        }
        Err(join_err) => {
            let _ = ingestion_run::fail_run(
                pool,
                run_id,
                &format!("production parse task panicked: {join_err}"),
            )
            .await;
            return Err(join_err.into());
        }
    }

    if staged_count == 0 {
        let manifest =
            serde_json::json!({"row_count": rejected_count, "rejected_count": rejected_count});
        let reason = "zero production rows staged — likely the archive format changed (zero-result safeguard)";
        ingestion_run::reject_run(pool, run_id, reason, manifest.clone()).await?;
        return Ok(FileImportOutcome {
            source_file: filename,
            run_id,
            status: ImportStatus::Rejected,
            staged_count: 0,
            rejected_count,
            pre_1990_blocked_count,
            manifest,
        });
    }

    let manifest = serde_json::json!({
        "content_length": downloaded.content_length,
        "last_modified": downloaded.last_modified,
        "staged_count": staged_count,
        "rejected_count": rejected_count,
        "pre_1990_blocked_count": pre_1990_blocked_count,
    });
    ingestion_run::complete_run(
        pool,
        run_id,
        staged_count,
        rejected_count,
        None,
        None,
        manifest.clone(),
    )
    .await?;

    Ok(FileImportOutcome {
        source_file: filename,
        run_id,
        status: ImportStatus::Completed,
        staged_count,
        rejected_count,
        pre_1990_blocked_count,
        manifest,
    })
}

/// Applies every staging-time business rule beyond "has a LEASE_KID"
/// (already checked by the parser): pre-1990 legal-review block (task
/// doc's explicit hard requirement), unparseable `MONTH-YEAR`, and an
/// out-of-range month value. Mutates `row.rejection_reason` in place —
/// all reasons are `'static` string literals, so this stays a plain field
/// assignment rather than needing an owned `String`.
fn classify_row(
    row: &mut RawProductionRow,
    expected_product: ProductionProduct,
    legal_review_override_enabled: bool,
    pre_1990_blocked_count: &mut i64,
) {
    if row.rejection_reason.is_some() {
        return;
    }
    let Some(year) = row.year else {
        row.rejection_reason = Some("could not parse MONTH-YEAR");
        return;
    };
    let Some(month) = row.month else {
        row.rejection_reason = Some("could not parse MONTH-YEAR");
        return;
    };
    if !is_production_year_ingestible(year, legal_review_override_enabled) {
        row.rejection_reason = Some("pre-1990 production blocked (legal review required)");
        *pre_1990_blocked_count += 1;
        return;
    }
    if classify_production_month(month).is_none() {
        row.rejection_reason = Some("month value outside the documented -1|0|1-12 range");
        return;
    }
    match parse_product(row.product_raw.as_deref()) {
        Some(p) if p == expected_product => {}
        _ => row.rejection_reason = Some("PRODUCT column did not match the expected oil/gas file"),
    }
}

async fn persist_batch(
    pool: &PgPool,
    run_id: Uuid,
    source_file: &'static str,
    rows: &[RawProductionRow],
) -> Result<(i64, i64), sqlx::Error> {
    if rows.is_empty() {
        return Ok((0, 0));
    }
    let mut staged = 0i64;
    let mut rejected = 0i64;

    let mut qb = sqlx::QueryBuilder::new(
        "INSERT INTO ks_lease_production_raw_snapshot (
            ingestion_run_id, source_file, lease_kid, rejection_reason, reported_lease_name, dor_code,
            api_number_raw, field_name, producing_zone, operator, county, township, township_direction,
            range, range_direction, section, spot, source_latitude, source_longitude, month_year_raw,
            year, month, product_raw, reported_contributing_wells, production_volume, source_url,
            raw_data, source_record_hash
        ) ",
    );
    qb.push_values(rows.iter(), |mut b, row| {
        if row.rejection_reason.is_some() {
            rejected += 1;
        } else {
            staged += 1;
        }
        b.push_bind(run_id)
            .push_bind(source_file)
            .push_bind(&row.lease_kid)
            .push_bind(row.rejection_reason)
            .push_bind(&row.reported_lease_name)
            .push_bind(&row.dor_code)
            .push_bind(&row.api_number_raw)
            .push_bind(&row.field_name)
            .push_bind(&row.producing_zone)
            .push_bind(&row.operator)
            .push_bind(&row.county)
            .push_bind(row.township)
            .push_bind(&row.township_direction)
            .push_bind(row.range)
            .push_bind(&row.range_direction)
            .push_bind(row.section)
            .push_bind(&row.spot)
            .push_bind(row.source_latitude)
            .push_bind(row.source_longitude)
            .push_bind(&row.month_year_raw)
            .push_bind(row.year)
            .push_bind(row.month)
            .push_bind(&row.product_raw)
            .push_bind(row.reported_contributing_wells)
            .push_bind(row.production_volume)
            .push_bind(&row.source_url)
            .push_bind(&row.raw_data)
            .push_bind(&row.source_record_hash);
    });
    qb.build().execute(pool).await?;

    Ok((staged, rejected))
}
