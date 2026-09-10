//! KS-03: the KGS statewide well snapshot importer. ArcGIS
//! (`KGS_WELLS_ARCGIS`) is preferred; the static ZIP (`KGS_WELLS_ZIP`) is
//! the fallback used only when ArcGIS itself is unreachable (item
//! resolution or initial metadata/count fetch fails after retries) — not
//! a mid-pipeline switch once an ArcGIS run has started staging data.

pub mod membership;
pub mod zip_source;

use chrono::{DateTime, Utc};
use sha2::{Digest, Sha256};
use sqlx::{PgPool, QueryBuilder};
use uuid::Uuid;

use crate::constants;
use crate::source::arcgis::{self, ArcGisFeature};
use crate::source::ingestion_run::{self, DownloadedPayload, IngestionRunError};
use crate::source::registry;
use crate::source::retry::{with_retry, RetryPolicy};

pub use membership::{classify_kansas_membership, MembershipClassification, MembershipStatus};

const PARSER_VERSION: &str = "kgs_wells_arcgis-v1";
const ZIP_PARSER_VERSION: &str = "kgs_wells_zip-v1";

#[derive(Debug, thiserror::Error)]
pub enum ImportError {
    #[error("arcgis query error: {0}")]
    ArcGis(#[from] arcgis::ArcGisQueryError),
    #[error("zip fallback error: {0}")]
    Zip(#[from] zip_source::ZipImportError),
    #[error("database error: {0}")]
    Database(#[from] sqlx::Error),
    #[error("ingestion run error: {0}")]
    IngestionRun(#[from] IngestionRunError),
    #[error("source '{0}' is not registered in ks_source")]
    SourceNotFound(&'static str),
    #[error("blocking parse task panicked: {0}")]
    JoinError(#[from] tokio::task::JoinError),
}

#[derive(Debug, Clone)]
pub struct ImportOptions {
    /// Fraction of the layer's estimated count the extracted total may
    /// deviate by before the run is rejected (KS-03: "configurable count
    /// tolerance rather than a hardcoded 519,092 expectation").
    pub count_tolerance_ratio: f64,
    pub retry_policy: RetryPolicy,
    /// Testing/ops escape hatch only: caps how many object IDs are
    /// processed. Must stay `None` in production — this exists so a live
    /// smoke check can run against the real 519k-record layer without
    /// pulling the whole thing.
    pub limit_object_ids: Option<usize>,
}

impl Default for ImportOptions {
    fn default() -> Self {
        Self {
            count_tolerance_ratio: 0.02,
            retry_policy: RetryPolicy::default(),
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
    pub included_count: i64,
    pub excluded_count: i64,
    pub manifest: serde_json::Value,
}

/// One raw record, already classified, ready to insert into
/// `ks_well_raw_snapshot`. `source_format`/`source_object_id` differ
/// between the ArcGIS and ZIP paths (see `zip_source.rs`).
#[derive(Debug, Clone)]
pub struct StagedRow {
    pub source_object_id: Option<i64>,
    pub source_kid: Option<String>,
    pub source_format: &'static str,
    pub raw_data: serde_json::Value,
    pub longitude: Option<f64>,
    pub latitude: Option<f64>,
    pub membership: MembershipClassification,
}

/// Runs the full ArcGIS snapshot import described in the KS-03 task doc:
/// resolve → record metadata before → list all object IDs → fetch in
/// batches with retry → stage → verify no missing IDs → re-read metadata
/// after (source-change detection) → compare count against a configurable
/// tolerance → publish (or reject) atomically via the `ks_ingestion_run`
/// state machine.
pub async fn run_arcgis_snapshot_import(
    pool: &PgPool,
    client: &reqwest::Client,
    layer_url: &str,
    options: ImportOptions,
) -> Result<ImportOutcome, ImportError> {
    let source = registry::find_source_by_code(pool, constants::KGS_WELLS_ARCGIS)
        .await?
        .ok_or(ImportError::SourceNotFound(constants::KGS_WELLS_ARCGIS))?;

    let metadata_before = with_retry(options.retry_policy, || {
        arcgis::fetch_layer_metadata(client, layer_url)
    })
    .await?;
    let count_estimate = with_retry(options.retry_policy, || {
        arcgis::fetch_total_count(client, layer_url)
    })
    .await?;

    let object_ids = arcgis::fetch_all_object_ids(
        client,
        layer_url,
        options.retry_policy,
        options.limit_object_ids,
    )
    .await?;
    let checksum = checksum_object_ids(&object_ids);

    if let Some(existing_run_id) =
        ingestion_run::find_completed_run_by_checksum(pool, source.id, &checksum).await?
    {
        return Ok(ImportOutcome {
            run_id: existing_run_id,
            status: ImportStatus::UnchangedSkipped,
            included_count: 0,
            excluded_count: 0,
            manifest: serde_json::json!({"note": "identical object-ID snapshot already completed; skipped re-extraction"}),
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
            source_url: layer_url,
            retrieved_at: Utc::now(),
            source_modified_at,
            payload_checksum: &checksum,
            schema_fingerprint: Some(&schema_fingerprint_before),
            parser_version: PARSER_VERSION,
        },
    )
    .await?;

    let batch_size = metadata_before.max_record_count.max(1) as usize;
    let mut included_count: i64 = 0;
    let mut excluded_count: i64 = 0;
    let mut extracted_object_ids: Vec<i64> = Vec::with_capacity(object_ids.len());

    for chunk in object_ids.chunks(batch_size) {
        let features = match with_retry(options.retry_policy, || {
            arcgis::fetch_features_batch(client, layer_url, chunk)
        })
        .await
        {
            Ok(features) => features,
            Err(err) => {
                let _ = ingestion_run::fail_run(
                    pool,
                    run_id,
                    &format!("batch fetch failed after retries: {err}"),
                )
                .await;
                return Err(err.into());
            }
        };

        let rows: Vec<StagedRow> = features.iter().map(feature_to_staged_row).collect();
        for row in &rows {
            match row.membership.status {
                MembershipStatus::Included => included_count += 1,
                MembershipStatus::ExcludedOtherState => excluded_count += 1,
            }
            if let Some(object_id) = row.source_object_id {
                extracted_object_ids.push(object_id);
            }
        }
        persist_batch(pool, run_id, &rows).await?;
    }

    extracted_object_ids.sort_unstable();
    let missing_object_ids: Vec<i64> = object_ids
        .iter()
        .copied()
        .filter(|id| extracted_object_ids.binary_search(id).is_err())
        .collect();

    if !missing_object_ids.is_empty() {
        let manifest = serde_json::json!({
            "missing_object_id_count": missing_object_ids.len(),
            "missing_object_id_sample": missing_object_ids.iter().take(50).collect::<Vec<_>>(),
        });
        let reason = format!(
            "{} of {} object IDs were never extracted",
            missing_object_ids.len(),
            object_ids.len()
        );
        ingestion_run::reject_run(pool, run_id, &reason, manifest.clone()).await?;
        return Ok(ImportOutcome {
            run_id,
            status: ImportStatus::Rejected,
            included_count,
            excluded_count,
            manifest,
        });
    }

    let metadata_after = with_retry(options.retry_policy, || {
        arcgis::fetch_layer_metadata(client, layer_url)
    })
    .await?;
    let count_after = with_retry(options.retry_policy, || {
        arcgis::fetch_total_count(client, layer_url)
    })
    .await?;
    let schema_fingerprint_after = metadata_after.schema_fingerprint();
    let data_last_edit_after = metadata_after
        .editing_info
        .as_ref()
        .and_then(|info| info.data_last_edit_date);
    let data_last_edit_before = metadata_before
        .editing_info
        .as_ref()
        .and_then(|info| info.data_last_edit_date);

    let source_changed = schema_fingerprint_after != schema_fingerprint_before
        || data_last_edit_after != data_last_edit_before;

    let extracted_total = included_count + excluded_count;
    let manifest = serde_json::json!({
        "source_metadata_before": {
            "schema_fingerprint": schema_fingerprint_before,
            "data_last_edit_date": data_last_edit_before,
            "count_estimate": count_estimate,
        },
        "source_metadata_after": {
            "schema_fingerprint": schema_fingerprint_after,
            "data_last_edit_date": data_last_edit_after,
            "count_estimate": count_after,
        },
        "object_id_count": object_ids.len(),
        "included_count": included_count,
        "excluded_count": excluded_count,
        "extracted_total": extracted_total,
        "count_tolerance_ratio": options.count_tolerance_ratio,
    });

    if source_changed {
        ingestion_run::reject_run(
            pool,
            run_id,
            "source layer metadata changed during import",
            manifest.clone(),
        )
        .await?;
        return Ok(ImportOutcome {
            run_id,
            status: ImportStatus::Rejected,
            included_count,
            excluded_count,
            manifest,
        });
    }

    let tolerance = (count_estimate as f64 * options.count_tolerance_ratio).ceil() as i64;
    if (extracted_total - count_estimate).abs() > tolerance {
        let reason = format!(
            "extracted count {extracted_total} outside tolerance of layer estimate {count_estimate} (+/- {tolerance})"
        );
        ingestion_run::reject_run(pool, run_id, &reason, manifest.clone()).await?;
        return Ok(ImportOutcome {
            run_id,
            status: ImportStatus::Rejected,
            included_count,
            excluded_count,
            manifest,
        });
    }

    ingestion_run::complete_run(
        pool,
        run_id,
        extracted_total,
        excluded_count,
        None,
        None,
        manifest.clone(),
    )
    .await?;
    Ok(ImportOutcome {
        run_id,
        status: ImportStatus::Completed,
        included_count,
        excluded_count,
        manifest,
    })
}

/// Runs the ZIP fallback import: download (with retry) → parse (in a
/// blocking task, since `zip`/`csv` are synchronous) → classify → stage →
/// publish. No layer-estimate/tolerance check applies here — a static
/// file's own row count is the ground truth, there's no separate estimate
/// to compare it against — but the same `ks_ingestion_run` state machine
/// (and its "never read a non-completed run's staged rows" guarantee)
/// applies identically to both paths.
pub async fn run_zip_snapshot_import(
    pool: &PgPool,
    client: &reqwest::Client,
    zip_url: &str,
    dest_path: &std::path::Path,
    retry_policy: RetryPolicy,
) -> Result<ImportOutcome, ImportError> {
    let source = registry::find_source_by_code(pool, constants::KGS_WELLS_ZIP)
        .await?
        .ok_or(ImportError::SourceNotFound(constants::KGS_WELLS_ZIP))?;

    let downloaded = zip_source::download_zip(client, zip_url, dest_path, retry_policy).await?;

    if let Some(existing_run_id) =
        ingestion_run::find_completed_run_by_checksum(pool, source.id, &downloaded.sha256).await?
    {
        return Ok(ImportOutcome {
            run_id: existing_run_id,
            status: ImportStatus::UnchangedSkipped,
            included_count: 0,
            excluded_count: 0,
            manifest: serde_json::json!({"note": "identical ZIP checksum already completed; skipped re-parse"}),
        });
    }

    let source_modified_at = downloaded
        .last_modified
        .as_deref()
        .and_then(|value| DateTime::parse_from_rfc2822(value).ok())
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
            parser_version: ZIP_PARSER_VERSION,
        },
    )
    .await?;

    let parse_path = downloaded.path.clone();
    let rows =
        match tokio::task::spawn_blocking(move || zip_source::parse_zip_wells(&parse_path)).await {
            Ok(Ok(rows)) => rows,
            Ok(Err(err)) => {
                let _ = ingestion_run::fail_run(pool, run_id, &format!("zip parse failed: {err}"))
                    .await;
                return Err(err.into());
            }
            Err(join_err) => {
                let _ = ingestion_run::fail_run(
                    pool,
                    run_id,
                    &format!("zip parse task panicked: {join_err}"),
                )
                .await;
                return Err(join_err.into());
            }
        };

    let mut included_count: i64 = 0;
    let mut excluded_count: i64 = 0;
    for chunk in rows.chunks(2000) {
        for row in chunk {
            match row.membership.status {
                MembershipStatus::Included => included_count += 1,
                MembershipStatus::ExcludedOtherState => excluded_count += 1,
            }
        }
        persist_batch(pool, run_id, chunk).await?;
    }

    let manifest = serde_json::json!({
        "content_length": downloaded.content_length,
        "last_modified": downloaded.last_modified,
        "row_count": rows.len(),
        "included_count": included_count,
        "excluded_count": excluded_count,
    });

    ingestion_run::complete_run(
        pool,
        run_id,
        included_count + excluded_count,
        excluded_count,
        None,
        None,
        manifest.clone(),
    )
    .await?;

    Ok(ImportOutcome {
        run_id,
        status: ImportStatus::Completed,
        included_count,
        excluded_count,
        manifest,
    })
}

fn checksum_object_ids(object_ids: &[i64]) -> String {
    let mut hasher = Sha256::new();
    for id in object_ids {
        hasher.update(id.to_le_bytes());
    }
    format!("{:x}", hasher.finalize())
}

fn epoch_millis_to_datetime(millis: i64) -> Option<DateTime<Utc>> {
    DateTime::from_timestamp_millis(millis)
}

fn feature_to_staged_row(feature: &ArcGisFeature) -> StagedRow {
    let attrs = &feature.attributes;
    let object_id = attrs.get("OBJECTID").and_then(|v| v.as_i64());
    let source_kid = attrs.get("KID").map(json_value_to_identity_string);
    let api_num_nodash = attrs.get("API_NUM_NODASH").and_then(|v| v.as_str());
    let county = attrs.get("COUNTY").and_then(|v| v.as_str());
    let latitude = feature.geometry.as_ref().map(|g| g.y);
    let longitude = feature.geometry.as_ref().map(|g| g.x);
    let wgs84_lat_lon = match (latitude, longitude) {
        (Some(lat), Some(lon)) => Some((lat, lon)),
        _ => None,
    };

    let membership = classify_kansas_membership(api_num_nodash, county, wgs84_lat_lon);

    StagedRow {
        source_object_id: object_id,
        source_kid,
        source_format: "arcgis",
        raw_data: serde_json::Value::Object(attrs.clone()),
        longitude,
        latitude,
        membership,
    }
}

/// KID (and similar identity-ish fields) arrive as either a JSON number or
/// a JSON string depending on source — normalize to a plain string
/// without the `.0` a naive `Value::to_string()` on a float would add.
fn json_value_to_identity_string(value: &serde_json::Value) -> String {
    match value {
        serde_json::Value::String(s) => s.clone(),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                i.to_string()
            } else {
                n.to_string()
            }
        }
        other => other.to_string(),
    }
}

async fn persist_batch(pool: &PgPool, run_id: Uuid, rows: &[StagedRow]) -> Result<(), sqlx::Error> {
    if rows.is_empty() {
        return Ok(());
    }

    let mut query = QueryBuilder::new(
        r#"
        INSERT INTO ks_well_raw_snapshot (
            ingestion_run_id, source_object_id, source_kid, source_format,
            raw_data, geometry_source_x, geometry_source_y,
            kansas_membership_status, kansas_membership_reason
        )
        "#,
    );

    query.push_values(rows, |mut row_values, row| {
        row_values
            .push_bind(run_id)
            .push_bind(row.source_object_id)
            .push_bind(&row.source_kid)
            .push_bind(row.source_format)
            .push_bind(&row.raw_data)
            .push_bind(row.longitude)
            .push_bind(row.latitude)
            .push_bind(row.membership.status.as_db_str())
            .push_bind(&row.membership.reason);
    });

    query.build().execute(pool).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn feature_to_staged_row_extracts_identity_and_classifies() {
        let feature: ArcGisFeature = serde_json::from_value(serde_json::json!({
            "attributes": {
                "OBJECTID": 150927528,
                "KID": 1002913220,
                "API_NUM_NODASH": "15059232820000",
                "COUNTY": "Franklin",
            },
            "geometry": {"x": -95.23272894759393, "y": 38.44608317373737},
        }))
        .unwrap();

        let row = feature_to_staged_row(&feature);
        assert_eq!(row.source_object_id, Some(150927528));
        assert_eq!(row.source_kid.as_deref(), Some("1002913220"));
        assert_eq!(row.source_format, "arcgis");
        assert_eq!(row.longitude, Some(-95.23272894759393));
        assert_eq!(row.latitude, Some(38.44608317373737));
        assert_eq!(row.membership.status, MembershipStatus::Included);
    }

    #[test]
    fn feature_to_staged_row_handles_missing_geometry_and_kid() {
        let feature: ArcGisFeature = serde_json::from_value(serde_json::json!({
            "attributes": {
                "OBJECTID": 1,
                "API_NUM_NODASH": "35000000000000",
                "COUNTY": "Not A County",
            },
            "geometry": null,
        }))
        .unwrap();

        let row = feature_to_staged_row(&feature);
        assert_eq!(row.source_kid, None);
        assert_eq!(row.longitude, None);
        assert_eq!(row.latitude, None);
        assert_eq!(row.membership.status, MembershipStatus::ExcludedOtherState);
    }

    #[test]
    fn checksum_is_stable_and_order_independent_after_sorting() {
        let a = checksum_object_ids(&[1, 2, 3]);
        let b = checksum_object_ids(&[1, 2, 3]);
        let c = checksum_object_ids(&[1, 2, 4]);
        assert_eq!(a, b);
        assert_ne!(a, c);
    }
}
