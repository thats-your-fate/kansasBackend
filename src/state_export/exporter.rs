//! KS-12/KS-13: the Kansas `state-export-v1` exporter.
//!
//! Scope, deliberately narrower than a full incremental/change-data-capture
//! exporter: this module supports **baseline/full exports only**. The
//! `state-export-v1` contract explicitly allows this ("Baseline exports may
//! be large and may disable row-level change logging while still producing
//! `import_entity_stats` and batch totals") — Kansas has no
//! `state_export_change_log` table or per-source-table triggers, so
//! `incremental` is rejected with a clear error rather than silently
//! producing an incomplete diff. A future task can add incremental support
//! once periodic full re-exports prove too slow or too large; see
//! `docs/ops/ks-12-13-shared-infra-handoff.md`.
//!
//! Every Kansas canonical table already carries a stable internal UUID
//! primary key, so (unlike a source that has to synthesize a stable key
//! from natural fields) `keys::*` just wraps that id — see `keys.rs`'s
//! module doc.
//!
//! What is deliberately left out of this first export, and why:
//! - `comments` (`ks_well.comments`): KS-12 requires raw KGS comments never
//!   reach a map popup, and only reach a detail view "after sanitization."
//!   No sanitization step exists yet anywhere in this repo, so the raw
//!   field is not exported at all rather than exported unsanitized.
//! - `wellbore_geometries` / `well_geometries`: both tickets ask for point
//!   geometry only at launch (no lateral lines) — well location travels on
//!   `WellV1.surface_latitude/surface_longitude` directly, which is all the
//!   shared map's point layer reads (confirmed against `futureBackend`'s
//!   own `map.rs` before writing this module).
//! - `documents` / `document_contents` / `event_document_links`: Kansas has
//!   no document/PDF ingestion yet (that is separately-planned, still
//!   three-digit "KS-011", not built) — these entity files are always
//!   present (required by the contract for a full export) but empty.
//! - Yearly-total / starting-cumulative sentinel rows (`record_kind` other
//!   than `'monthly'`) are never exported into `production_monthly` — the
//!   shared entity's `production_month` is a real calendar date, and the
//!   whole reason KS-10 built `record_kind` was to keep sentinel rows out
//!   of exactly this kind of monthly-shaped consumer.
//! - Kansas lease production is lease-level, not allocated to an individual
//!   well (the same physical well can draw against a shared lease with
//!   other wells) — every `production_monthly` row is exported with
//!   `well_source_key = None` and `relationship_resolution_status =
//!   "unresolved"`, exactly the case the contract's own "Unresolved
//!   Relationships" section describes.

use anyhow::{bail, Context, Result};
use chrono::{DateTime, NaiveDate, Utc};
use futures_util::TryStreamExt;
use serde_json::{json, Value};
use sqlx::{PgPool, Row};
use std::{
    fs,
    path::{Path, PathBuf},
    time::{Duration, Instant, SystemTime},
};
use uuid::Uuid;

use super::{
    contract::{
        CountyV1, EventV1, ExportModeV1, OperatorV1, ProductionMonthlyV1, RegulatoryAreaV1,
        RegulatoryRecordV1, SourceEnvelopeV1, StateExportManifestV1, TombstoneV1, WellIdentifierV1,
        WellV1, WellboreV1, SCHEMA_VERSION, SOURCE_CODE,
    },
    keys,
    package::{self, ChunkLimits, Compression, EntityPartWriter, PartSummary},
};

pub const ENTITY_ORDER: &[&str] = &[
    "counties",
    "operators",
    "regulatory_areas",
    "wells",
    "well_identifiers",
    "wellbores",
    "wellbore_geometries",
    "well_geometries",
    "regulatory_records",
    "documents",
    "document_contents",
    "events",
    "event_document_links",
    "production_monthly",
    "tombstones",
];

#[derive(Debug, Clone)]
pub struct ExportOptions {
    pub consumer: String,
    pub output_dir: PathBuf,
    pub max_records_per_part: u64,
    pub max_uncompressed_bytes_per_part: u64,
    pub compression: Compression,
    pub compression_level: i32,
}

impl Default for ExportOptions {
    fn default() -> Self {
        Self {
            consumer: "future-wells-co".to_owned(),
            output_dir: PathBuf::from("data/state-exports"),
            max_records_per_part: 50_000,
            max_uncompressed_bytes_per_part: 128 * 1024 * 1024,
            compression: Compression::Zstd,
            compression_level: 3,
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct EntityExportSummary {
    pub entity_type: String,
    pub records_written: u64,
    pub part_count: u64,
    pub compressed_bytes: u64,
}

#[derive(Debug, Clone)]
pub struct ExportSummary {
    pub export_id: String,
    pub mode: String,
    pub snapshot_at: DateTime<Utc>,
    pub output_path: PathBuf,
    pub manifest_checksum: String,
    pub total_records: u64,
    pub total_compressed_bytes: u64,
    pub elapsed: Duration,
    pub entities: Vec<EntityExportSummary>,
}

pub async fn preflight(pool: &PgPool) -> Result<()> {
    let required_tables = [
        "ks_ingestion_run",
        "ks_well",
        "ks_well_source_alias",
        "ks_operator",
        "ks_field",
        "ks_lease",
        "ks_lease_production_month",
        "ks_event",
        "ks_drilling_intent",
        "ks_operator_transfer",
        "ks_operator_transfer_well",
        "ks_state_export_run",
    ];
    for table in required_tables {
        let exists: bool =
            sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = $1)")
                .bind(table)
                .fetch_one(pool)
                .await?;
        if !exists {
            bail!("required table is missing: {table}");
        }
    }
    Ok(())
}

pub async fn export_state_v1(
    pool: &PgPool,
    mode: ExportModeV1,
    options: ExportOptions,
) -> Result<ExportSummary> {
    if matches!(mode, ExportModeV1::Incremental) {
        bail!("incremental exports are not yet implemented for kansas_backend; run `full` (see module doc)");
    }
    let started = Instant::now();
    let export_id = Uuid::new_v4().to_string();
    let snapshot_at = Utc::now();
    let mode_name = format!("{mode:?}").to_lowercase();

    let partial_root = options.output_dir.join(".partial");
    let partial_dir = partial_root.join(&export_id);
    let final_dir = options.output_dir.join(&export_id);
    if final_dir.exists() {
        bail!(
            "final export directory already exists: {}",
            final_dir.display()
        );
    }
    if partial_dir.exists() {
        fs::remove_dir_all(&partial_dir)?;
    }
    fs::create_dir_all(&partial_dir)?;

    let mut all_parts = Vec::new();
    let mut summaries = Vec::new();
    for entity in ENTITY_ORDER {
        let (parts, summary) = export_entity(pool, &partial_dir, entity, &options).await?;
        all_parts.extend(parts);
        summaries.push(summary);
    }

    let manifest = StateExportManifestV1 {
        export_id: export_id.clone(),
        schema_version: SCHEMA_VERSION.to_owned(),
        state_code: "KS".to_owned(),
        source_code: SOURCE_CODE.to_owned(),
        export_mode: mode,
        created_at: snapshot_at,
        lower_change_sequence: None,
        upper_change_sequence: None,
        watermark_start: None,
        watermark_end: None,
        snapshot_at: Some(snapshot_at),
        files: all_parts.iter().map(Into::into).collect(),
    };
    let manifest_checksum = package::write_manifest(&partial_dir, &manifest)?;
    package::verify_package(&partial_dir)?;
    fs::rename(&partial_dir, &final_dir)
        .with_context(|| format!("finalizing export directory {}", final_dir.display()))?;

    let total_records = summaries
        .iter()
        .map(|summary| summary.records_written)
        .sum::<u64>();
    let total_compressed_bytes = summaries
        .iter()
        .map(|summary| summary.compressed_bytes)
        .sum::<u64>();

    let records_by_entity = summaries
        .iter()
        .fold(serde_json::Map::new(), |mut map, entity| {
            map.insert(entity.entity_type.clone(), json!(entity.records_written));
            map
        });
    sqlx::query(
        r#"
        INSERT INTO ks_state_export_run (export_id, consumer_code, export_mode, package_path, manifest_checksum, records_by_entity, total_records)
        VALUES ($1, $2, $3, $4, $5, $6, $7)
        "#,
    )
    .bind(&export_id)
    .bind(&options.consumer)
    .bind(&mode_name)
    .bind(final_dir.to_string_lossy().to_string())
    .bind(&manifest_checksum)
    .bind(Value::Object(records_by_entity))
    .bind(total_records as i64)
    .execute(pool)
    .await?;

    Ok(ExportSummary {
        export_id,
        mode: mode_name,
        snapshot_at,
        output_path: final_dir,
        manifest_checksum,
        total_records,
        total_compressed_bytes,
        elapsed: started.elapsed(),
        entities: summaries,
    })
}

pub fn verify_package(path: &Path) -> Result<StateExportManifestV1> {
    package::verify_package(path)
}

pub async fn acknowledge_export(pool: &PgPool, consumer: &str, export_id: &str) -> Result<()> {
    let updated = sqlx::query(
        "UPDATE ks_state_export_run SET acknowledged_at = now() WHERE export_id = $1 AND consumer_code = $2 AND acknowledged_at IS NULL",
    )
    .bind(export_id)
    .bind(consumer)
    .execute(pool)
    .await?;
    if updated.rows_affected() == 0 {
        bail!("no unacknowledged export {export_id} found for consumer {consumer}");
    }
    Ok(())
}

type StatusRow = (String, String, i64, DateTime<Utc>, Option<DateTime<Utc>>);

pub async fn status(pool: &PgPool, consumer: &str) -> Result<String> {
    let rows: Vec<StatusRow> = sqlx::query_as(
        r#"
        SELECT export_id, export_mode, total_records, created_at, acknowledged_at
        FROM ks_state_export_run
        WHERE consumer_code = $1
        ORDER BY created_at DESC
        LIMIT 20
        "#,
    )
    .bind(consumer)
    .fetch_all(pool)
    .await?;
    if rows.is_empty() {
        return Ok(format!(
            "no state-export-v1 runs recorded for consumer {consumer}"
        ));
    }
    let mut out = format!("state-export-v1 runs for consumer {consumer} (most recent first):\n");
    for (export_id, mode, total_records, created_at, acknowledged_at) in rows {
        out.push_str(&format!(
            "  {export_id} mode={mode} records={total_records} created_at={created_at} acknowledged_at={:?}\n",
            acknowledged_at
        ));
    }
    Ok(out)
}

pub fn cleanup_partials(output_dir: &Path, older_than: Duration) -> Result<Vec<PathBuf>> {
    let partial_root = output_dir.join(".partial");
    let mut removed = Vec::new();
    if !partial_root.exists() {
        return Ok(removed);
    }
    let cutoff = SystemTime::now()
        .checked_sub(older_than)
        .unwrap_or(SystemTime::UNIX_EPOCH);
    for entry in fs::read_dir(&partial_root)? {
        let entry = entry?;
        let metadata = entry.metadata()?;
        let modified = metadata.modified().unwrap_or(SystemTime::now());
        if metadata.is_dir() && modified < cutoff {
            fs::remove_dir_all(entry.path())?;
            removed.push(entry.path());
        }
    }
    Ok(removed)
}

async fn export_entity(
    pool: &PgPool,
    package_dir: &Path,
    entity: &str,
    options: &ExportOptions,
) -> Result<(Vec<PartSummary>, EntityExportSummary)> {
    let mut writer = new_writer(package_dir, entity, options);
    let records_written = match entity {
        "counties" => export_counties(pool, &mut writer).await?,
        "operators" => export_operators(pool, &mut writer).await?,
        "regulatory_areas" => export_regulatory_areas(pool, &mut writer).await?,
        "wells" => export_wells(pool, &mut writer, None).await?,
        "well_identifiers" => export_well_identifiers(pool, &mut writer).await?,
        "wellbores" => export_wellbores(pool, &mut writer).await?,
        "wellbore_geometries"
        | "well_geometries"
        | "documents"
        | "document_contents"
        | "event_document_links" => 0,
        "regulatory_records" => export_regulatory_records(pool, &mut writer).await?,
        "events" => export_events(pool, &mut writer).await?,
        "production_monthly" => export_production_monthly(pool, &mut writer).await?,
        "tombstones" => export_tombstones(pool, &mut writer).await?,
        other => bail!("unknown entity type {other}"),
    };
    let parts = finish_writer(writer)?;
    let compressed_bytes = parts.iter().map(|part| part.compressed_bytes).sum();
    Ok((
        parts.clone(),
        EntityExportSummary {
            entity_type: entity.to_owned(),
            records_written,
            part_count: parts.len() as u64,
            compressed_bytes,
        },
    ))
}

fn new_writer(package_dir: &Path, entity_type: &str, options: &ExportOptions) -> EntityPartWriter {
    EntityPartWriter::new(
        package_dir.to_path_buf(),
        entity_type,
        options.compression,
        options.compression_level,
        ChunkLimits {
            max_records: options.max_records_per_part,
            max_uncompressed_bytes: options.max_uncompressed_bytes_per_part,
        },
    )
}

fn finish_writer(writer: EntityPartWriter) -> Result<Vec<PartSummary>> {
    writer.finish()
}

fn envelope(
    source_key: String,
    source_updated_at: Option<DateTime<Utc>>,
    payload: Value,
) -> SourceEnvelopeV1 {
    SourceEnvelopeV1 {
        source_key,
        source_updated_at,
        source_payload: payload,
    }
}

async fn export_counties(pool: &PgPool, writer: &mut EntityPartWriter) -> Result<u64> {
    let names: Vec<String> = sqlx::query_scalar(
        r#"
        SELECT DISTINCT county FROM (
            SELECT county FROM ks_well WHERE county IS NOT NULL AND county <> ''
            UNION
            SELECT county FROM ks_lease WHERE county IS NOT NULL AND county <> ''
            UNION
            SELECT county FROM ks_lease_production_month WHERE county IS NOT NULL AND county <> ''
            UNION
            SELECT county FROM ks_drilling_intent WHERE county IS NOT NULL AND county <> ''
            UNION
            SELECT county FROM ks_operator_transfer WHERE county IS NOT NULL AND county <> ''
        ) AS counties(county)
        ORDER BY county
        "#,
    )
    .fetch_all(pool)
    .await?;

    let mut count = 0_u64;
    for name in names {
        let source_key = keys::county(&name);
        let record = CountyV1 {
            source: envelope(source_key.clone(), None, json!({})),
            name: name.clone(),
            normalized_name: keys::slug(&name),
            fips_code: None,
            regulator_code: None,
        };
        record.validate()?;
        writer.push(&source_key, &record)?;
        count += 1;
    }
    Ok(count)
}

async fn export_operators(pool: &PgPool, writer: &mut EntityPartWriter) -> Result<u64> {
    let mut rows = sqlx::query(
        r#"
        SELECT id, kcc_license_number, reported_company_name, normalized_company_name, active_status,
               source_city, source_updated_at, first_seen_at, last_seen_in_all_list_at, last_seen_in_active_list_at
        FROM ks_operator
        ORDER BY id
        "#,
    )
    .fetch(pool);

    let mut count = 0_u64;
    while let Some(row) = rows.try_next().await? {
        let id: Uuid = row.try_get("id")?;
        let source_key = keys::operator(id);
        let payload = json!({
            "kcc_license_number": row.try_get::<String, _>("kcc_license_number")?,
            "source_city": row.try_get::<Option<String>, _>("source_city")?,
            "first_seen_at": row.try_get::<Option<DateTime<Utc>>, _>("first_seen_at")?,
            "last_seen_in_all_list_at": row.try_get::<Option<DateTime<Utc>>, _>("last_seen_in_all_list_at")?,
            "last_seen_in_active_list_at": row.try_get::<Option<DateTime<Utc>>, _>("last_seen_in_active_list_at")?,
        });
        let record = OperatorV1 {
            source: envelope(
                source_key.clone(),
                row.try_get("source_updated_at")?,
                payload,
            ),
            regulator_operator_number: Some(row.try_get("kcc_license_number")?),
            display_name: row.try_get("reported_company_name")?,
            normalized_name: row.try_get("normalized_company_name")?,
            status: Some(row.try_get("active_status")?),
        };
        record.validate()?;
        writer.push(&source_key, &record)?;
        count += 1;
    }
    Ok(count)
}

async fn export_regulatory_areas(pool: &PgPool, writer: &mut EntityPartWriter) -> Result<u64> {
    let mut count = 0_u64;

    let mut fields = sqlx::query(
        r#"
        SELECT id, official_field_name, field_kid, active_status, field_type, producing_zones,
               approx_acres, cumulative_oil, cumulative_gas, max_oil_wells, max_gas_wells,
               last_oil_production_date, last_gas_production_date, geometry_is_valid,
               geometry_validity_reason, source_geometry_geojson
        FROM ks_field
        ORDER BY id
        "#,
    )
    .fetch(pool);
    while let Some(row) = fields.try_next().await? {
        let id: Uuid = row.try_get("id")?;
        let source_key = keys::field(id);
        let name: String = row.try_get("official_field_name")?;
        let payload = json!({
            "active_status": row.try_get::<String, _>("active_status")?,
            "field_type": row.try_get::<Option<String>, _>("field_type")?,
            "producing_zones": row.try_get::<Option<String>, _>("producing_zones")?,
            "approx_acres": row.try_get::<Option<f64>, _>("approx_acres")?,
            "cumulative_oil": row.try_get::<Option<f64>, _>("cumulative_oil")?,
            "cumulative_gas": row.try_get::<Option<f64>, _>("cumulative_gas")?,
            "max_oil_wells": row.try_get::<Option<i32>, _>("max_oil_wells")?,
            "max_gas_wells": row.try_get::<Option<i32>, _>("max_gas_wells")?,
            "last_oil_production_date": row.try_get::<Option<String>, _>("last_oil_production_date")?,
            "last_gas_production_date": row.try_get::<Option<String>, _>("last_gas_production_date")?,
            "geometry_is_valid": row.try_get::<Option<bool>, _>("geometry_is_valid")?,
            "geometry_validity_reason": row.try_get::<Option<String>, _>("geometry_validity_reason")?,
            "source_geometry_geojson": row.try_get::<Option<Value>, _>("source_geometry_geojson")?,
        });
        let record = RegulatoryAreaV1 {
            source: envelope(source_key.clone(), None, payload),
            area_type: "field".to_owned(),
            code: row.try_get::<Option<String>, _>("field_kid")?,
            name,
            parent_area_source_key: None,
            county_source_key: None,
        };
        record.validate()?;
        writer.push(&source_key, &record)?;
        count += 1;
    }

    let mut leases = sqlx::query(
        r#"
        SELECT id, lease_kid, dor_code, product, reported_lease_name, operator, field_name, county,
               township, township_direction, range, range_direction, section, spot,
               source_latitude, source_longitude, source_crs, cumulative_production,
               year_start, year_stop, source_document_url
        FROM ks_lease
        ORDER BY id
        "#,
    )
    .fetch(pool);
    while let Some(row) = leases.try_next().await? {
        let id: Uuid = row.try_get("id")?;
        let source_key = keys::lease(id);
        let county: Option<String> = row.try_get("county")?;
        let name = row
            .try_get::<Option<String>, _>("reported_lease_name")?
            .unwrap_or_else(|| "Unnamed lease".to_owned());
        let payload = json!({
            "lease_kid": row.try_get::<Option<String>, _>("lease_kid")?,
            "product": row.try_get::<String, _>("product")?,
            "operator": row.try_get::<Option<String>, _>("operator")?,
            "field_name": row.try_get::<Option<String>, _>("field_name")?,
            "county": county,
            "township": row.try_get::<Option<i16>, _>("township")?,
            "township_direction": row.try_get::<Option<String>, _>("township_direction")?,
            "range": row.try_get::<Option<i16>, _>("range")?,
            "range_direction": row.try_get::<Option<String>, _>("range_direction")?,
            "section": row.try_get::<Option<i16>, _>("section")?,
            "spot": row.try_get::<Option<String>, _>("spot")?,
            "source_latitude": opt_f64_string(&row, "source_latitude")?,
            "source_longitude": opt_f64_string(&row, "source_longitude")?,
            "source_crs": row.try_get::<String, _>("source_crs")?,
            "cumulative_production": row.try_get::<Option<f64>, _>("cumulative_production")?,
            "year_start": row.try_get::<Option<i32>, _>("year_start")?,
            "year_stop": row.try_get::<Option<i32>, _>("year_stop")?,
            "source_document_url": row.try_get::<Option<String>, _>("source_document_url")?,
        });
        let record = RegulatoryAreaV1 {
            source: envelope(source_key.clone(), None, payload),
            area_type: "lease".to_owned(),
            code: row.try_get::<Option<String>, _>("dor_code")?,
            name,
            parent_area_source_key: None,
            county_source_key: county.map(|c| keys::county(&c)),
        };
        record.validate()?;
        writer.push(&source_key, &record)?;
        count += 1;
    }

    Ok(count)
}

async fn export_wells(
    pool: &PgPool,
    writer: &mut EntityPartWriter,
    _unused: Option<()>,
) -> Result<u64> {
    let mut rows = sqlx::query(
        r#"
        SELECT internal_well_id, api_raw, api14, well_name, county, lifecycle_status, well_class_raw,
               status_raw, status2_raw, operator_name_kgs_original, operator_name_kgs_reported_current,
               spud_date, completion_date, plug_date, permit_date,
               ST_Y(display_geometry_4326) AS surface_lat, ST_X(display_geometry_4326) AS surface_lon,
               total_depth, producing_formation, formation_at_total_depth, ip_oil, ip_gas, ip_water,
               oil_kid, oil_dor_id, gas_kid, gas_dor_id, horizontal_or_directional, trajectory_available,
               location_source, location_confidence, location_warning, source_crs,
               permit_date_raw, spud_date_raw, completion_date_raw, plug_date_raw,
               first_seen_at, last_seen_at, is_tombstoned, kgs_kid, kcc_permit
        FROM ks_well
        ORDER BY internal_well_id
        "#,
    )
    .fetch(pool);

    let mut count = 0_u64;
    while let Some(row) = rows.try_next().await? {
        let id: Uuid = row.try_get("internal_well_id")?;
        let source_key = keys::well(id);
        let county: Option<String> = row.try_get("county")?;
        let payload = json!({
            "kgs_kid": row.try_get::<Option<String>, _>("kgs_kid")?,
            "kcc_permit": row.try_get::<Option<String>, _>("kcc_permit")?,
            "well_class_raw": row.try_get::<Option<String>, _>("well_class_raw")?,
            "status_raw": row.try_get::<Option<String>, _>("status_raw")?,
            "status2_raw": row.try_get::<Option<String>, _>("status2_raw")?,
            "operator_name_kgs_original": row.try_get::<Option<String>, _>("operator_name_kgs_original")?,
            "operator_name_kgs_reported_current": row.try_get::<Option<String>, _>("operator_name_kgs_reported_current")?,
            "total_depth": row.try_get::<Option<i32>, _>("total_depth")?,
            "producing_formation": row.try_get::<Option<String>, _>("producing_formation")?,
            "formation_at_total_depth": row.try_get::<Option<String>, _>("formation_at_total_depth")?,
            "ip_oil": row.try_get::<Option<f64>, _>("ip_oil")?,
            "ip_gas": row.try_get::<Option<f64>, _>("ip_gas")?,
            "ip_water": row.try_get::<Option<f64>, _>("ip_water")?,
            "oil_kid": row.try_get::<Option<String>, _>("oil_kid")?,
            "oil_dor_id": row.try_get::<Option<String>, _>("oil_dor_id")?,
            "gas_kid": row.try_get::<Option<String>, _>("gas_kid")?,
            "gas_dor_id": row.try_get::<Option<String>, _>("gas_dor_id")?,
            "horizontal_or_directional": row.try_get::<bool, _>("horizontal_or_directional")?,
            "trajectory_available": row.try_get::<bool, _>("trajectory_available")?,
            "location_source": row.try_get::<Option<String>, _>("location_source")?,
            "location_confidence": row.try_get::<Option<String>, _>("location_confidence")?,
            "location_warning": row.try_get::<Option<String>, _>("location_warning")?,
            "source_crs": row.try_get::<String, _>("source_crs")?,
            "permit_date_raw": row.try_get::<Option<String>, _>("permit_date_raw")?,
            "spud_date_raw": row.try_get::<Option<String>, _>("spud_date_raw")?,
            "completion_date_raw": row.try_get::<Option<String>, _>("completion_date_raw")?,
            "plug_date_raw": row.try_get::<Option<String>, _>("plug_date_raw")?,
            "first_seen_at": row.try_get::<DateTime<Utc>, _>("first_seen_at")?,
            "last_seen_at": row.try_get::<DateTime<Utc>, _>("last_seen_at")?,
            "is_tombstoned": row.try_get::<bool, _>("is_tombstoned")?,
        });
        let record = WellV1 {
            source: envelope(
                source_key.clone(),
                Some(row.try_get("last_seen_at")?),
                payload,
            ),
            normalized_api_number: row.try_get::<Option<String>, _>("api14")?,
            displayed_api_number: row.try_get::<Option<String>, _>("api_raw")?,
            well_name: row.try_get::<Option<String>, _>("well_name")?,
            well_number: None,
            operator_source_key: None,
            county_source_key: county.map(|c| keys::county(&c)),
            current_status: Some(row.try_get("lifecycle_status")?),
            well_type: row.try_get::<Option<String>, _>("well_class_raw")?,
            spud_date: row.try_get::<Option<NaiveDate>, _>("spud_date")?,
            completion_date: row.try_get::<Option<NaiveDate>, _>("completion_date")?,
            first_production_date: None,
            plug_date: row.try_get::<Option<NaiveDate>, _>("plug_date")?,
            surface_latitude: opt_f64_string(&row, "surface_lat")?,
            surface_longitude: opt_f64_string(&row, "surface_lon")?,
            bottom_hole_latitude: None,
            bottom_hole_longitude: None,
        };
        record.validate()?;
        writer.push(&source_key, &record)?;
        count += 1;
    }
    Ok(count)
}

async fn export_well_identifiers(pool: &PgPool, writer: &mut EntityPartWriter) -> Result<u64> {
    let mut count = 0_u64;

    let mut api_rows = sqlx::query("SELECT internal_well_id, api14 FROM ks_well WHERE api14 IS NOT NULL ORDER BY internal_well_id").fetch(pool);
    while let Some(row) = api_rows.try_next().await? {
        let well_id: Uuid = row.try_get("internal_well_id")?;
        let well_key = keys::well(well_id);
        let api14: String = row.try_get("api14")?;
        let source_key = keys::well_identifier(&well_key, "api14", &api14);
        let record = WellIdentifierV1 {
            source: envelope(source_key.clone(), None, json!({})),
            well_source_key: well_key,
            identifier_type: "api14".to_owned(),
            value: api14.clone(),
            normalized_value: api14,
            is_primary: true,
        };
        record.validate()?;
        writer.push(&source_key, &record)?;
        count += 1;
    }

    let mut alias_rows = sqlx::query(
        "SELECT well_id, alias_type, alias_value FROM ks_well_source_alias ORDER BY well_id",
    )
    .fetch(pool);
    while let Some(row) = alias_rows.try_next().await? {
        let well_id: Uuid = row.try_get("well_id")?;
        let well_key = keys::well(well_id);
        let alias_type: String = row.try_get("alias_type")?;
        let alias_value: String = row.try_get("alias_value")?;
        let source_key = keys::well_identifier(&well_key, &alias_type, &alias_value);
        let record = WellIdentifierV1 {
            source: envelope(source_key.clone(), None, json!({})),
            well_source_key: well_key,
            identifier_type: alias_type,
            value: alias_value.clone(),
            normalized_value: alias_value,
            is_primary: false,
        };
        record.validate()?;
        writer.push(&source_key, &record)?;
        count += 1;
    }
    Ok(count)
}

async fn export_wellbores(pool: &PgPool, writer: &mut EntityPartWriter) -> Result<u64> {
    let mut rows =
        sqlx::query("SELECT internal_well_id, horizontal_or_directional, lifecycle_status, total_depth FROM ks_well ORDER BY internal_well_id").fetch(pool);
    let mut count = 0_u64;
    while let Some(row) = rows.try_next().await? {
        let well_id: Uuid = row.try_get("internal_well_id")?;
        let well_key = keys::well(well_id);
        let source_key = keys::wellbore(&well_key);
        let horizontal: bool = row.try_get("horizontal_or_directional")?;
        let record = WellboreV1 {
            source: envelope(source_key.clone(), None, json!({})),
            well_source_key: Some(well_key),
            well_identifier_type: None,
            well_identifier_value: None,
            normalized_well_identifier: None,
            display_name: None,
            wellbore_number: None,
            parent_wellbore_source_key: None,
            bore_role: "MAIN".to_owned(),
            trajectory_kind: if horizontal {
                "HORIZONTAL".to_owned()
            } else {
                "VERTICAL".to_owned()
            },
            is_lateral: Some(false),
            classification_method: Some("ks_well.horizontal_or_directional".to_owned()),
            geometry_quality: None,
            status: Some(row.try_get("lifecycle_status")?),
            reported_measured_depth_ft: row
                .try_get::<Option<i32>, _>("total_depth")?
                .map(|value| value.to_string()),
            reported_true_vertical_depth_ft: None,
            reported_lateral_length_ft: None,
        };
        record.validate()?;
        writer.push(&source_key, &record)?;
        count += 1;
    }
    Ok(count)
}

async fn export_regulatory_records(pool: &PgPool, writer: &mut EntityPartWriter) -> Result<u64> {
    let mut count = 0_u64;

    let mut intents = sqlx::query(
        r#"
        SELECT id, matched_well_id, match_method, occurred_at, published_at, reported_lease_or_well_name,
               source_filename, kcc_district, operator_license, county, section, township, range, range_direction
        FROM ks_drilling_intent
        ORDER BY id
        "#,
    )
    .fetch(pool);
    while let Some(row) = intents.try_next().await? {
        let intent_id: Uuid = row.try_get("id")?;
        let match_method: String = row.try_get("match_method")?;
        let matched_well_id: Option<Uuid> = row.try_get("matched_well_id")?;
        let well_source_key = if match_method != "unresolved" {
            matched_well_id.map(keys::well)
        } else {
            None
        };
        let source_key = keys::regulatory_record_intent(intent_id);
        let payload = json!({
            "source_filename": row.try_get::<String, _>("source_filename")?,
            "kcc_district": row.try_get::<Option<String>, _>("kcc_district")?,
            "operator_license": row.try_get::<Option<String>, _>("operator_license")?,
            "county": row.try_get::<Option<String>, _>("county")?,
            "section": row.try_get::<Option<i16>, _>("section")?,
            "township": row.try_get::<Option<i16>, _>("township")?,
            "range": row.try_get::<Option<i16>, _>("range")?,
            "range_direction": row.try_get::<Option<String>, _>("range_direction")?,
            "match_method": match_method,
        });
        let record = RegulatoryRecordV1 {
            source: envelope(source_key.clone(), None, payload),
            well_source_key: well_source_key.clone(),
            original_well_source_key: None,
            record_type: "drilling_intent".to_owned(),
            record_subtype: None,
            effective_at: row.try_get::<Option<DateTime<Utc>>, _>("occurred_at")?,
            filed_at: None,
            status: None,
            title: row.try_get::<Option<String>, _>("reported_lease_or_well_name")?,
            summary: None,
            relationship_resolution_status: if well_source_key.is_some() {
                "resolved".to_owned()
            } else {
                "unresolved".to_owned()
            },
        };
        record.validate()?;
        writer.push(&source_key, &record)?;
        count += 1;
    }

    let mut transfer_rows = sqlx::query(
        r#"
        SELECT t.id, t.past_operator_name, t.past_operator_license, t.new_operator_name, t.new_operator_license,
               t.received_date, t.effective_date, t.approved_date, t.lease_or_well_label, t.county, t.district,
               t.section, t.township, t.range, t.range_direction, t.source_file,
               tw.well_id
        FROM ks_operator_transfer t
        LEFT JOIN ks_operator_transfer_well tw ON tw.transfer_id = t.id AND tw.match_method <> 'fuzzy_candidate'
        ORDER BY t.id
        "#,
    )
    .fetch(pool);

    let mut pending: Option<(Uuid, Value, bool)> = None;
    while let Some(row) = transfer_rows.try_next().await? {
        let transfer_id: Uuid = row.try_get("id")?;
        let well_id: Option<Uuid> = row.try_get("well_id")?;

        if pending.as_ref().map(|(id, _, _)| *id) != Some(transfer_id) {
            if let Some((prev_id, payload, matched_any)) = pending.take() {
                if !matched_any {
                    count += emit_transfer_record(writer, prev_id, None, &payload)?;
                }
            }
            let payload = json!({
                "past_operator_name": row.try_get::<Option<String>, _>("past_operator_name")?,
                "past_operator_license": row.try_get::<Option<String>, _>("past_operator_license")?,
                "new_operator_name": row.try_get::<Option<String>, _>("new_operator_name")?,
                "new_operator_license": row.try_get::<Option<String>, _>("new_operator_license")?,
                "received_date": row.try_get::<Option<NaiveDate>, _>("received_date")?,
                "effective_date": row.try_get::<Option<NaiveDate>, _>("effective_date")?,
                "approved_date": row.try_get::<Option<NaiveDate>, _>("approved_date")?,
                "district": row.try_get::<Option<String>, _>("district")?,
                "county": row.try_get::<Option<String>, _>("county")?,
                "section": row.try_get::<Option<i16>, _>("section")?,
                "township": row.try_get::<Option<i16>, _>("township")?,
                "range": row.try_get::<Option<i16>, _>("range")?,
                "range_direction": row.try_get::<Option<String>, _>("range_direction")?,
                "source_file": row.try_get::<String, _>("source_file")?,
                "lease_or_well_label": row.try_get::<Option<String>, _>("lease_or_well_label")?,
            });
            pending = Some((transfer_id, payload, false));
        }

        if let Some(well_id) = well_id {
            let (_, payload, matched_any) = pending.as_mut().expect("pending transfer just set");
            *matched_any = true;
            count += emit_transfer_record(writer, transfer_id, Some(keys::well(well_id)), payload)?;
        }
    }
    if let Some((prev_id, payload, matched_any)) = pending {
        if !matched_any {
            count += emit_transfer_record(writer, prev_id, None, &payload)?;
        }
    }

    Ok(count)
}

fn emit_transfer_record(
    writer: &mut EntityPartWriter,
    transfer_id: Uuid,
    well_source_key: Option<String>,
    payload: &Value,
) -> Result<u64> {
    let title = payload
        .get("lease_or_well_label")
        .and_then(Value::as_str)
        .map(str::to_owned);
    let effective_date = payload
        .get("effective_date")
        .and_then(Value::as_str)
        .and_then(|value| NaiveDate::parse_from_str(value, "%Y-%m-%d").ok());
    let received_date = payload
        .get("received_date")
        .and_then(Value::as_str)
        .and_then(|value| NaiveDate::parse_from_str(value, "%Y-%m-%d").ok());
    let source_key = keys::regulatory_record_transfer(transfer_id, well_source_key.as_deref());
    let record = RegulatoryRecordV1 {
        source: envelope(source_key.clone(), None, payload.clone()),
        well_source_key: well_source_key.clone(),
        original_well_source_key: None,
        record_type: "operator_transfer".to_owned(),
        record_subtype: None,
        effective_at: effective_date.and_then(date_start),
        filed_at: received_date.and_then(date_start),
        status: Some("approved".to_owned()),
        title,
        summary: None,
        relationship_resolution_status: if well_source_key.is_some() {
            "resolved".to_owned()
        } else {
            "unresolved".to_owned()
        },
    };
    record.validate()?;
    writer.push(&source_key, &record)?;
    Ok(1)
}

async fn export_events(pool: &PgPool, writer: &mut EntityPartWriter) -> Result<u64> {
    // KS-14 gave `ks_event` its own typed `related_well_id`/
    // `related_operator_id`/`related_field_id` columns (for watch-area
    // matching) — reading those directly is both simpler and more
    // complete than KS-12/13's original approach of deriving a well link
    // only for INTENT_POSTED via a join back to `ks_drilling_intent`.
    let mut rows = sqlx::query(
        r#"
        SELECT id, event_type, occurred_at, published_at, related_well_id, related_operator_id, related_field_id
        FROM ks_event
        ORDER BY id
        "#,
    )
    .fetch(pool);

    let mut count = 0_u64;
    while let Some(row) = rows.try_next().await? {
        let event_id: Uuid = row.try_get("id")?;
        let event_type: String = row.try_get("event_type")?;
        let related_well_id: Option<Uuid> = row.try_get("related_well_id")?;
        let related_operator_id: Option<Uuid> = row.try_get("related_operator_id")?;
        let related_field_id: Option<Uuid> = row.try_get("related_field_id")?;
        let well_source_key = related_well_id.map(keys::well);
        let source_key = keys::event(event_id);
        let payload = json!({
            "operator_source_key": related_operator_id.map(keys::operator),
            "field_source_key": related_field_id.map(keys::field),
        });
        let record = EventV1 {
            source: envelope(source_key.clone(), None, payload),
            well_source_key: well_source_key.clone(),
            original_well_source_key: None,
            event_type,
            event_subtype: None,
            event_at: row.try_get::<Option<DateTime<Utc>>, _>("occurred_at")?,
            published_at: row.try_get::<Option<DateTime<Utc>>, _>("published_at")?,
            title: None,
            summary: None,
            relationship_resolution_status: if well_source_key.is_some() {
                "resolved".to_owned()
            } else {
                "unresolved".to_owned()
            },
        };
        record.validate()?;
        writer.push(&source_key, &record)?;
        count += 1;
    }
    Ok(count)
}

async fn export_production_monthly(pool: &PgPool, writer: &mut EntityPartWriter) -> Result<u64> {
    let mut rows = sqlx::query(
        r#"
        SELECT id, lease_kid, product, year, month, production_volume, reported_lease_name, dor_code,
               api_number_raw, field_name, producing_zone, operator, county, township, township_direction,
               range, range_direction, section, spot, source_latitude, source_longitude, source_crs,
               reported_contributing_wells, source_url
        FROM ks_lease_production_month
        WHERE record_kind = 'monthly'
        ORDER BY id
        "#,
    )
    .fetch(pool);

    let mut count = 0_u64;
    while let Some(row) = rows.try_next().await? {
        let id: Uuid = row.try_get("id")?;
        let source_key = keys::production_monthly(id);
        let year: i32 = row.try_get("year")?;
        let month: i16 = row.try_get("month")?;
        let production_month = NaiveDate::from_ymd_opt(year, month as u32, 1)
            .with_context(|| format!("invalid production year/month {year}-{month}"))?;
        let product: String = row.try_get("product")?;
        let volume: f64 = row.try_get("production_volume")?;
        let volume_string = volume.to_string();
        let payload = json!({
            "lease_kid": row.try_get::<String, _>("lease_kid")?,
            "reported_lease_name": row.try_get::<Option<String>, _>("reported_lease_name")?,
            "dor_code": row.try_get::<Option<String>, _>("dor_code")?,
            "api_number_raw": row.try_get::<Option<String>, _>("api_number_raw")?,
            "field_name": row.try_get::<Option<String>, _>("field_name")?,
            "producing_zone": row.try_get::<Option<String>, _>("producing_zone")?,
            "operator": row.try_get::<Option<String>, _>("operator")?,
            "county": row.try_get::<Option<String>, _>("county")?,
            "township": row.try_get::<Option<i16>, _>("township")?,
            "township_direction": row.try_get::<Option<String>, _>("township_direction")?,
            "range": row.try_get::<Option<i16>, _>("range")?,
            "range_direction": row.try_get::<Option<String>, _>("range_direction")?,
            "section": row.try_get::<Option<i16>, _>("section")?,
            "spot": row.try_get::<Option<String>, _>("spot")?,
            "source_latitude": opt_f64_string(&row, "source_latitude")?,
            "source_longitude": opt_f64_string(&row, "source_longitude")?,
            "source_crs": row.try_get::<String, _>("source_crs")?,
            "reported_contributing_wells": row.try_get::<Option<i32>, _>("reported_contributing_wells")?,
            "source_url": row.try_get::<Option<String>, _>("source_url")?,
        });
        let record = ProductionMonthlyV1 {
            source: envelope(source_key.clone(), None, payload),
            well_source_key: None,
            original_well_source_key: None,
            operator_source_key: None,
            production_month,
            oil_volume: (product == "oil").then_some(volume_string.clone()),
            gas_volume: (product == "gas").then_some(volume_string),
            water_volume: None,
            days_produced: None,
            source_allocation_type: Some("lease".to_owned()),
            revision_status: None,
            relationship_resolution_status: "unresolved".to_owned(),
        };
        record.validate()?;
        writer.push(&source_key, &record)?;
        count += 1;
    }
    Ok(count)
}

async fn export_tombstones(pool: &PgPool, writer: &mut EntityPartWriter) -> Result<u64> {
    let mut rows = sqlx::query("SELECT internal_well_id, tombstoned_at FROM ks_well WHERE is_tombstoned = true ORDER BY internal_well_id").fetch(pool);
    let mut count = 0_u64;
    while let Some(row) = rows.try_next().await? {
        let well_id: Uuid = row.try_get("internal_well_id")?;
        let well_key = keys::well(well_id);
        let source_key = keys::tombstone("wells", &well_key);
        let record = TombstoneV1 {
            source: envelope(source_key.clone(), None, json!({})),
            entity_type: "wells".to_owned(),
            entity_source_key: well_key,
            deleted_at: row.try_get::<Option<DateTime<Utc>>, _>("tombstoned_at")?,
            reason: Some(
                "no longer observed in the KGS statewide snapshot after two consecutive absences"
                    .to_owned(),
            ),
        };
        record.validate()?;
        writer.push(&source_key, &record)?;
        count += 1;
    }
    Ok(count)
}

fn date_start(date: NaiveDate) -> Option<DateTime<Utc>> {
    date.and_hms_opt(0, 0, 0)
        .map(|datetime| DateTime::<Utc>::from_naive_utc_and_offset(datetime, Utc))
}

fn opt_f64_string(row: &sqlx::postgres::PgRow, column: &str) -> Result<Option<String>> {
    Ok(row
        .try_get::<Option<f64>, _>(column)
        .ok()
        .flatten()
        .map(|value| format!("{value:.7}")))
}
