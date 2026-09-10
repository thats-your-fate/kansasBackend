//! KS-08: the KGS field importer. Unlike KS-05/06/07, this source is
//! confirmed live — KGS is reachable, unlike `kcc.ks.gov` — verified
//! 2026-09-05 against the real ArcGIS `Oil_and_Gas_Fields` layer (item
//! `390ee1b9eade40bca42ee3534a813b20`, 8,089 real features) and the real
//! field-production/abandoned-field `.xlsx` files linked from
//! `https://www.kgs.ku.edu/Magellan/Field/index.html`.
//!
//! One `ks_ingestion_run` covers all three source components (ArcGIS
//! geometry/identity, six active-file `.xlsx` enrichment files, one
//! abandoned-file `.xlsx`) — the same "one run, multiple fetches" shape
//! KS-05 used for its active/all operator downloads.

pub mod xlsx_source;

use chrono::Utc;
use sha2::{Digest, Sha256};
use sqlx::PgPool;
use uuid::Uuid;

use crate::constants;
use crate::source::arcgis::{self, GeoJsonFeature};
use crate::source::ingestion_run::{self, DownloadedPayload, IngestionRunError};
use crate::source::registry;
use crate::source::retry::{with_retry, RetryPolicy};

use xlsx_source::{parse_field_xlsx, RawFieldFileRow, XlsxParseError};

const PARSER_VERSION: &str = "kgs_fields_arcgis_xlsx-v1";

#[derive(Debug, thiserror::Error)]
pub enum ImportError {
    #[error("http request failed: {0}")]
    Request(#[from] reqwest::Error),
    #[error("arcgis query error: {0}")]
    ArcGis(#[from] arcgis::ArcGisQueryError),
    #[error("xlsx parse error: {0}")]
    Xlsx(#[from] XlsxParseError),
    #[error("database error: {0}")]
    Database(#[from] sqlx::Error),
    #[error("ingestion run error: {0}")]
    IngestionRun(#[from] IngestionRunError),
    #[error("source '{0}' is not registered in ks_source")]
    SourceNotFound(&'static str),
}

#[derive(Debug, Clone, Copy)]
pub struct ImportOptions {
    pub retry_policy: RetryPolicy,
    pub count_tolerance_ratio: f64,
    /// Testing/ops escape hatch only — caps how many object IDs the
    /// ArcGIS half processes. Must stay `None` in production.
    pub limit_object_ids: Option<usize>,
}

impl Default for ImportOptions {
    fn default() -> Self {
        Self {
            retry_policy: RetryPolicy::default(),
            count_tolerance_ratio: 0.02,
            limit_object_ids: None,
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
pub struct ImportOutcome {
    pub run_id: Uuid,
    pub status: ImportStatus,
    pub arcgis_count: i64,
    pub active_file_row_count: i64,
    pub abandoned_file_row_count: i64,
    pub manifest: serde_json::Value,
}

#[allow(clippy::too_many_arguments)]
pub async fn run_field_import(
    pool: &PgPool,
    client: &reqwest::Client,
    arcgis_layer_url: &str,
    active_file_urls: &[String],
    abandoned_file_url: &str,
    options: ImportOptions,
) -> Result<ImportOutcome, ImportError> {
    let source = registry::find_source_by_code(pool, constants::KGS_FIELDS)
        .await?
        .ok_or(ImportError::SourceNotFound(constants::KGS_FIELDS))?;

    let metadata_before = with_retry(options.retry_policy, || {
        arcgis::fetch_layer_metadata(client, arcgis_layer_url)
    })
    .await?;
    let count_estimate = with_retry(options.retry_policy, || {
        arcgis::fetch_total_count(client, arcgis_layer_url)
    })
    .await?;
    let object_ids = arcgis::fetch_all_object_ids(
        client,
        arcgis_layer_url,
        options.retry_policy,
        options.limit_object_ids,
    )
    .await?;

    let mut active_bytes = Vec::new();
    for url in active_file_urls {
        active_bytes.push(with_retry(options.retry_policy, || fetch_bytes(client, url)).await?);
    }
    let abandoned_bytes = with_retry(options.retry_policy, || {
        fetch_bytes(client, abandoned_file_url)
    })
    .await?;

    let checksum = checksum_run_inputs(&object_ids, &active_bytes, &abandoned_bytes);

    if let Some(existing_run_id) =
        ingestion_run::find_completed_run_by_checksum(pool, source.id, &checksum).await?
    {
        return Ok(ImportOutcome {
            run_id: existing_run_id,
            status: ImportStatus::UnchangedSkipped,
            arcgis_count: 0,
            active_file_row_count: 0,
            abandoned_file_row_count: 0,
            manifest: serde_json::json!({"note": "identical object-IDs + file bytes already completed; skipped re-extraction"}),
        });
    }

    let source_modified_at = metadata_before
        .editing_info
        .as_ref()
        .and_then(|info| info.data_last_edit_date)
        .and_then(epoch_millis_to_datetime);
    let schema_fingerprint_before = metadata_before.schema_fingerprint();

    let run_id = ingestion_run::start_run(
        pool,
        DownloadedPayload {
            source_id: source.id,
            source_url: arcgis_layer_url,
            retrieved_at: Utc::now(),
            source_modified_at,
            payload_checksum: &checksum,
            schema_fingerprint: Some(&schema_fingerprint_before),
            parser_version: PARSER_VERSION,
        },
    )
    .await?;

    let batch_size = metadata_before.max_record_count.max(1) as usize;
    let mut arcgis_count: i64 = 0;
    for chunk in object_ids.chunks(batch_size) {
        let features = match with_retry(options.retry_policy, || {
            arcgis::fetch_features_batch_geojson(client, arcgis_layer_url, chunk)
        })
        .await
        {
            Ok(features) => features,
            Err(err) => {
                let _ = ingestion_run::fail_run(
                    pool,
                    run_id,
                    &format!("field batch fetch failed after retries: {err}"),
                )
                .await;
                return Err(err.into());
            }
        };
        persist_arcgis_batch(pool, run_id, &features).await?;
        arcgis_count += features.len() as i64;
    }

    let metadata_after = with_retry(options.retry_policy, || {
        arcgis::fetch_layer_metadata(client, arcgis_layer_url)
    })
    .await?;
    let count_after = with_retry(options.retry_policy, || {
        arcgis::fetch_total_count(client, arcgis_layer_url)
    })
    .await?;
    let schema_fingerprint_after = metadata_after.schema_fingerprint();
    let source_changed = schema_fingerprint_after != schema_fingerprint_before;

    let tolerance = (count_estimate as f64 * options.count_tolerance_ratio).ceil() as i64;
    let manifest_base = serde_json::json!({
        "arcgis_count_estimate": count_estimate,
        "arcgis_count_after": count_after,
        "arcgis_extracted": arcgis_count,
    });

    if source_changed {
        ingestion_run::reject_run(
            pool,
            run_id,
            "field layer schema changed during import",
            manifest_base.clone(),
        )
        .await?;
        return Ok(ImportOutcome {
            run_id,
            status: ImportStatus::Rejected,
            arcgis_count,
            active_file_row_count: 0,
            abandoned_file_row_count: 0,
            manifest: manifest_base,
        });
    }
    if options.limit_object_ids.is_none() && (arcgis_count - count_estimate).abs() > tolerance {
        let reason = format!("extracted count {arcgis_count} outside tolerance of layer estimate {count_estimate} (+/- {tolerance})");
        ingestion_run::reject_run(pool, run_id, &reason, manifest_base.clone()).await?;
        return Ok(ImportOutcome {
            run_id,
            status: ImportStatus::Rejected,
            arcgis_count,
            active_file_row_count: 0,
            abandoned_file_row_count: 0,
            manifest: manifest_base,
        });
    }

    let mut active_file_row_count: i64 = 0;
    for bytes in &active_bytes {
        let rows = match parse_field_xlsx(bytes) {
            Ok(rows) => rows,
            Err(err) => {
                let _ = ingestion_run::fail_run(
                    pool,
                    run_id,
                    &format!("active field file parse failed: {err}"),
                )
                .await;
                return Err(err.into());
            }
        };
        persist_file_rows(pool, run_id, "active_file", &rows).await?;
        active_file_row_count += rows.len() as i64;
    }

    let abandoned_rows = match parse_field_xlsx(&abandoned_bytes) {
        Ok(rows) => rows,
        Err(err) => {
            let _ = ingestion_run::fail_run(
                pool,
                run_id,
                &format!("abandoned field file parse failed: {err}"),
            )
            .await;
            return Err(err.into());
        }
    };
    persist_file_rows(pool, run_id, "abandoned_file", &abandoned_rows).await?;
    let abandoned_file_row_count = abandoned_rows.len() as i64;

    let manifest = serde_json::json!({
        "arcgis_count_estimate": count_estimate,
        "arcgis_count_after": count_after,
        "arcgis_extracted": arcgis_count,
        "active_file_row_count": active_file_row_count,
        "abandoned_file_row_count": abandoned_file_row_count,
    });
    ingestion_run::complete_run(
        pool,
        run_id,
        arcgis_count + active_file_row_count + abandoned_file_row_count,
        0,
        None,
        None,
        manifest.clone(),
    )
    .await?;

    Ok(ImportOutcome {
        run_id,
        status: ImportStatus::Completed,
        arcgis_count,
        active_file_row_count,
        abandoned_file_row_count,
        manifest,
    })
}

async fn persist_arcgis_batch(
    pool: &PgPool,
    run_id: Uuid,
    features: &[GeoJsonFeature],
) -> Result<(), sqlx::Error> {
    let mut tx = pool.begin().await?;
    for feature in features {
        let attrs = &feature.properties;
        let field_name = attrs
            .get("FIELD_NAME")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let object_id = attrs.get("OBJECTID").and_then(|v| v.as_i64());
        let field_kid = attrs.get("FIELD_KID").and_then(|v| v.as_str());
        let status = attrs.get("STATUS").and_then(|v| v.as_str());
        let field_type = attrs.get("FIELD_TYPE").and_then(|v| v.as_str());
        let formations = attrs.get("FORMATIONS").and_then(|v| v.as_str());
        let hash = checksum_bytes(serde_json::to_string(attrs).unwrap_or_default().as_bytes());

        sqlx::query(
            r#"
            INSERT INTO ks_field_raw_snapshot (
                ingestion_run_id, source_component, source_object_id, field_kid, field_name,
                status, field_type, producing_zones, raw_geometry, raw_data, source_record_hash
            )
            VALUES ($1, 'arcgis', $2, $3, $4, $5, $6, $7, $8, $9, $10)
            "#,
        )
        .bind(run_id)
        .bind(object_id)
        .bind(field_kid)
        .bind(&field_name)
        .bind(status)
        .bind(field_type)
        .bind(formations)
        .bind(&feature.geometry)
        .bind(serde_json::Value::Object(attrs.clone()))
        .bind(&hash)
        .execute(&mut *tx)
        .await?;
    }
    tx.commit().await
}

async fn persist_file_rows(
    pool: &PgPool,
    run_id: Uuid,
    component: &'static str,
    rows: &[RawFieldFileRow],
) -> Result<(), sqlx::Error> {
    let mut tx = pool.begin().await?;
    for row in rows {
        let hash = checksum_bytes(row.raw_data.to_string().as_bytes());
        let annual_json: serde_json::Map<String, serde_json::Value> = row
            .annual
            .iter()
            .map(|(year, (oil_prod, oil_wells, gas_prod, gas_wells))| {
                (year.to_string(), serde_json::json!({"oil_prod": oil_prod, "oil_wells": oil_wells, "gas_prod": gas_prod, "gas_wells": gas_wells}))
            })
            .collect();
        sqlx::query(
            r#"
            INSERT INTO ks_field_raw_snapshot (
                ingestion_run_id, source_component, field_name, producing_zones,
                total_oil_prod, total_gas_prod, parsed_annual, raw_data, source_record_hash
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
            "#,
        )
        .bind(run_id)
        .bind(component)
        .bind(&row.field_name)
        .bind(&row.producing_zones)
        .bind(row.total_oil_prod)
        .bind(row.total_gas_prod)
        .bind(serde_json::Value::Object(annual_json))
        .bind(&row.raw_data)
        .bind(&hash)
        .execute(&mut *tx)
        .await?;
    }
    tx.commit().await
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

fn checksum_run_inputs(
    object_ids: &[i64],
    active_bytes: &[Vec<u8>],
    abandoned_bytes: &[u8],
) -> String {
    let mut hasher = Sha256::new();
    for id in object_ids {
        hasher.update(id.to_le_bytes());
    }
    for bytes in active_bytes {
        hasher.update(bytes);
    }
    hasher.update(abandoned_bytes);
    format!("{:x}", hasher.finalize())
}

fn checksum_bytes(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    format!("{:x}", hasher.finalize())
}

fn epoch_millis_to_datetime(millis: i64) -> Option<chrono::DateTime<Utc>> {
    chrono::DateTime::from_timestamp_millis(millis)
}
