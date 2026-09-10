//! KS-08: materializes `ks_field_raw_snapshot` rows from a `completed`
//! `ks_ingestion_run` into canonical `ks_field` rows. ArcGIS is the
//! identity/geometry/active-status source of truth (task doc's "source
//! geometry" and the confirmed-live `STATUS` field); the active/abandoned
//! `.xlsx` files only ever enrich a field matched by exact (normalized)
//! name — never create a name-similarity merge.

use std::collections::BTreeMap;

use serde_json::Value;
use sqlx::PgPool;
use uuid::Uuid;

use crate::normalize::kgs_fields::{derive_active_status, normalize_field_name};
use crate::parse::kgs_fields::xlsx_source::AnnualFieldValues;

#[derive(Debug, thiserror::Error)]
pub enum MaterializeError {
    #[error("database error: {0}")]
    Database(#[from] sqlx::Error),
    #[error("ingestion run {0} not found")]
    RunNotFound(Uuid),
    #[error("ingestion run {0} has status '{1}', not 'completed' — refusing to materialize from a non-published run")]
    RunNotCompleted(Uuid, String),
}

#[derive(Debug, Default, Clone, PartialEq)]
pub struct MaterializeOutcome {
    pub created: i64,
    pub updated: i64,
    pub geometry_invalid_count: i64,
}

#[derive(Debug, sqlx::FromRow)]
struct RawSnapshotRow {
    source_component: String,
    field_kid: Option<String>,
    field_name: String,
    status: Option<String>,
    field_type: Option<String>,
    producing_zones: Option<String>,
    total_oil_prod: Option<f64>,
    total_gas_prod: Option<f64>,
    parsed_annual: Option<Value>,
    raw_geometry: Option<Value>,
    raw_data: Value,
}

#[derive(Default)]
struct MergedField {
    official_field_name: String,
    field_kid: Option<String>,
    status: Option<String>,
    field_type: Option<String>,
    producing_zones: Option<String>,
    geometries: Vec<Value>,
    approx_acres: Option<f64>,
    cumulative_oil: Option<f64>,
    cumulative_gas: Option<f64>,
    max_oil_wells: Option<i32>,
    max_gas_wells: Option<i32>,
    last_oil_date: Option<String>,
    last_gas_date: Option<String>,
    file_total_oil: Option<f64>,
    file_total_gas: Option<f64>,
    annual: BTreeMap<i32, AnnualFieldValues>,
    seen_in_abandoned_file: bool,
}

pub async fn materialize_fields_from_run(
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
        SELECT source_component, field_kid, field_name, status, field_type, producing_zones,
               total_oil_prod, total_gas_prod, parsed_annual, raw_geometry, raw_data
        FROM ks_field_raw_snapshot
        WHERE ingestion_run_id = $1
        "#,
    )
    .bind(ingestion_run_id)
    .fetch_all(pool)
    .await?;

    let mut merged: BTreeMap<String, MergedField> = BTreeMap::new();

    for row in rows {
        let key = normalize_field_name(&row.field_name);
        let entry = merged.entry(key).or_insert_with(|| MergedField {
            official_field_name: row.field_name.clone(),
            ..Default::default()
        });

        match row.source_component.as_str() {
            "arcgis" => {
                entry.official_field_name = row.field_name.clone();
                entry.field_kid = row.field_kid.clone();
                entry.status = row.status.clone();
                entry.field_type = row.field_type.clone();
                if entry.producing_zones.is_none() {
                    entry.producing_zones = row.producing_zones.clone();
                }
                if let Some(geom) = row.raw_geometry {
                    entry.geometries.push(geom);
                }
                entry.approx_acres = f64_attr(&row.raw_data, "APPROXACRE");
                entry.cumulative_oil = f64_attr(&row.raw_data, "CUMM_OIL");
                entry.cumulative_gas = f64_attr(&row.raw_data, "CUMM_GAS");
                entry.max_oil_wells = f64_attr(&row.raw_data, "MAXOILWELL").map(|v| v as i32);
                entry.max_gas_wells = f64_attr(&row.raw_data, "MAXGASWELL").map(|v| v as i32);
                entry.last_oil_date = str_attr(&row.raw_data, "LASTODATE");
                entry.last_gas_date = str_attr(&row.raw_data, "LASTGDATE");
            }
            "active_file" | "abandoned_file" => {
                if row.source_component == "abandoned_file" {
                    entry.seen_in_abandoned_file = true;
                }
                if entry.producing_zones.is_none() {
                    entry.producing_zones = row.producing_zones.clone();
                }
                // Lifetime totals are cumulative-as-of-file-generation;
                // taking the max across every file/appearance is a safe,
                // simple rule since a true lifetime total never decreases.
                entry.file_total_oil = max_option(entry.file_total_oil, row.total_oil_prod);
                entry.file_total_gas = max_option(entry.file_total_gas, row.total_gas_prod);

                if let Some(Value::Object(annual_map)) = row.parsed_annual {
                    for (year_str, v) in annual_map {
                        let Ok(year) = year_str.parse::<i32>() else {
                            continue;
                        };
                        let oil_prod = v.get("oil_prod").and_then(|x| x.as_f64());
                        let oil_wells = v
                            .get("oil_wells")
                            .and_then(|x| x.as_i64())
                            .map(|x| x as i32);
                        let gas_prod = v.get("gas_prod").and_then(|x| x.as_f64());
                        let gas_wells = v
                            .get("gas_wells")
                            .and_then(|x| x.as_i64())
                            .map(|x| x as i32);
                        entry
                            .annual
                            .insert(year, (oil_prod, oil_wells, gas_prod, gas_wells));
                    }
                }
            }
            _ => {}
        }
    }

    let mut created = 0i64;
    let mut updated = 0i64;
    let mut geometry_invalid_count = 0i64;

    for (normalized_name, field) in merged {
        let active_status = if field.status.is_some() {
            derive_active_status(field.status.as_deref())
        } else if field.seen_in_abandoned_file {
            crate::normalize::kgs_fields::FieldActiveStatus::Abandoned
        } else {
            derive_active_status(None)
        };

        let cumulative_oil = field.cumulative_oil.or(field.file_total_oil);
        let cumulative_gas = field.cumulative_gas.or(field.file_total_gas);

        let geometry_json_texts: Vec<String> =
            field.geometries.iter().map(|g| g.to_string()).collect();
        let source_geometry_first = field.geometries.first().cloned();

        let hash = source_record_hash(&field);

        let row: (Uuid, bool, Option<bool>, Option<String>) = sqlx::query_as(
            r#"
            WITH collected AS (
                SELECT CASE WHEN array_length($9::text[], 1) IS NULL THEN NULL
                            ELSE ST_Multi(ST_Collect(ARRAY(SELECT ST_GeomFromGeoJSON(g) FROM unnest($9::text[]) AS g)))
                       END AS geom
            ),
            upsert AS (
                INSERT INTO ks_field (
                    official_field_name, normalized_search_name, active_status, field_type, producing_zones,
                    source_geometry_geojson, display_geometry, geometry_is_valid, geometry_validity_reason,
                    field_kid, approx_acres, cumulative_oil, cumulative_gas, max_oil_wells, max_gas_wells,
                    last_oil_production_date, last_gas_production_date,
                    source_ingestion_run_id, source_record_hash
                )
                SELECT
                    $1, $2, $3, $4, $5, $6, collected.geom,
                    CASE WHEN collected.geom IS NULL THEN NULL ELSE ST_IsValid(collected.geom) END,
                    CASE WHEN collected.geom IS NULL THEN NULL ELSE ST_IsValidReason(collected.geom) END,
                    $7, $8, $10, $11, $12, $13, $14, $15, $16, $17
                FROM collected
                ON CONFLICT (official_field_name) DO UPDATE SET
                    normalized_search_name = EXCLUDED.normalized_search_name,
                    active_status = EXCLUDED.active_status,
                    field_type = EXCLUDED.field_type,
                    producing_zones = COALESCE(EXCLUDED.producing_zones, ks_field.producing_zones),
                    source_geometry_geojson = COALESCE(EXCLUDED.source_geometry_geojson, ks_field.source_geometry_geojson),
                    display_geometry = COALESCE(EXCLUDED.display_geometry, ks_field.display_geometry),
                    geometry_is_valid = COALESCE(EXCLUDED.geometry_is_valid, ks_field.geometry_is_valid),
                    geometry_validity_reason = COALESCE(EXCLUDED.geometry_validity_reason, ks_field.geometry_validity_reason),
                    field_kid = COALESCE(EXCLUDED.field_kid, ks_field.field_kid),
                    approx_acres = COALESCE(EXCLUDED.approx_acres, ks_field.approx_acres),
                    cumulative_oil = COALESCE(EXCLUDED.cumulative_oil, ks_field.cumulative_oil),
                    cumulative_gas = COALESCE(EXCLUDED.cumulative_gas, ks_field.cumulative_gas),
                    max_oil_wells = COALESCE(EXCLUDED.max_oil_wells, ks_field.max_oil_wells),
                    max_gas_wells = COALESCE(EXCLUDED.max_gas_wells, ks_field.max_gas_wells),
                    last_oil_production_date = COALESCE(EXCLUDED.last_oil_production_date, ks_field.last_oil_production_date),
                    last_gas_production_date = COALESCE(EXCLUDED.last_gas_production_date, ks_field.last_gas_production_date),
                    source_ingestion_run_id = EXCLUDED.source_ingestion_run_id,
                    source_record_hash = EXCLUDED.source_record_hash,
                    updated_at = now()
                RETURNING id, (xmax = 0) AS inserted, geometry_is_valid, geometry_validity_reason
            )
            SELECT id, inserted, geometry_is_valid, geometry_validity_reason FROM upsert
            "#,
        )
        .bind(&field.official_field_name)
        .bind(&normalized_name)
        .bind(active_status.as_db_str())
        .bind(&field.field_type)
        .bind(&field.producing_zones)
        .bind(source_geometry_first)
        .bind(&field.field_kid)
        .bind(field.approx_acres)
        .bind(&geometry_json_texts)
        .bind(cumulative_oil)
        .bind(cumulative_gas)
        .bind(field.max_oil_wells)
        .bind(field.max_gas_wells)
        .bind(&field.last_oil_date)
        .bind(&field.last_gas_date)
        .bind(ingestion_run_id)
        .bind(&hash)
        .fetch_one(pool)
        .await?;

        let (field_id, inserted, geometry_is_valid, _reason) = row;
        if inserted {
            created += 1;
        } else {
            updated += 1;
        }
        if geometry_is_valid == Some(false) {
            geometry_invalid_count += 1;
        }

        if let Some(kid) = &field.field_kid {
            sqlx::query(
                r#"
                INSERT INTO ks_field_source_alias (field_id, alias_type, alias_value)
                VALUES ($1, 'field_kid', $2)
                ON CONFLICT (field_id, alias_type, alias_value) DO UPDATE SET last_seen_at = now()
                "#,
            )
            .bind(field_id)
            .bind(kid)
            .execute(pool)
            .await?;
        }

        for (year, (oil_prod, oil_wells, gas_prod, gas_wells)) in &field.annual {
            sqlx::query(
                r#"
                INSERT INTO ks_field_annual_production (field_id, year, oil_production, oil_wells, gas_production, gas_wells, source_ingestion_run_id)
                VALUES ($1, $2, $3, $4, $5, $6, $7)
                ON CONFLICT (field_id, year) DO UPDATE SET
                    oil_production = EXCLUDED.oil_production,
                    oil_wells = EXCLUDED.oil_wells,
                    gas_production = EXCLUDED.gas_production,
                    gas_wells = EXCLUDED.gas_wells,
                    source_ingestion_run_id = EXCLUDED.source_ingestion_run_id
                "#,
            )
            .bind(field_id)
            .bind(year)
            .bind(oil_prod)
            .bind(oil_wells)
            .bind(gas_prod)
            .bind(gas_wells)
            .bind(ingestion_run_id)
            .execute(pool)
            .await?;
        }
    }

    Ok(MaterializeOutcome {
        created,
        updated,
        geometry_invalid_count,
    })
}

fn f64_attr(raw_data: &Value, key: &str) -> Option<f64> {
    raw_data.get(key).and_then(|v| v.as_f64())
}

fn str_attr(raw_data: &Value, key: &str) -> Option<String> {
    raw_data
        .get(key)
        .and_then(|v| v.as_str())
        .map(str::to_string)
}

fn max_option(a: Option<f64>, b: Option<f64>) -> Option<f64> {
    match (a, b) {
        (Some(a), Some(b)) => Some(a.max(b)),
        (Some(a), None) => Some(a),
        (None, b) => b,
    }
}

fn source_record_hash(field: &MergedField) -> String {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update(&field.official_field_name);
    hasher.update(field.status.as_deref().unwrap_or(""));
    hasher.update(field.geometries.len().to_le_bytes());
    format!("{:x}", hasher.finalize())
}

/// Task doc acceptance criteria: "well-to-field relationships." There is
/// no separate join table — `ks_well.field_name` (KS-04) is joined to
/// `ks_field` by exact normalized name, the same discipline as everywhere
/// else in this module (never fuzzy).
pub async fn find_wells_by_field(pool: &PgPool, field_id: Uuid) -> Result<Vec<Uuid>, sqlx::Error> {
    sqlx::query_scalar(
        r#"
        SELECT w.internal_well_id
        FROM ks_well w
        JOIN ks_field f ON f.id = $1 AND upper(trim(w.field_name)) = f.normalized_search_name
        "#,
    )
    .bind(field_id)
    .fetch_all(pool)
    .await
}

#[derive(Debug, Clone, sqlx::FromRow)]
pub struct FieldWellCountReconciliation {
    pub official_field_name: String,
    pub reported_max_oil_wells: Option<i32>,
    pub reported_max_gas_wells: Option<i32>,
    pub materialized_well_count: i64,
}

/// Task doc acceptance criteria: "field-level production can be
/// reconciled separately from well counts." Compares the field file's own
/// reported well counts against `ks_well` rows actually materialized
/// under that field name — never derives one from the other.
pub async fn well_count_reconciliation(
    pool: &PgPool,
    limit: i64,
) -> Result<Vec<FieldWellCountReconciliation>, sqlx::Error> {
    sqlx::query_as(
        r#"
        SELECT
            f.official_field_name,
            f.max_oil_wells AS reported_max_oil_wells,
            f.max_gas_wells AS reported_max_gas_wells,
            count(w.internal_well_id) AS materialized_well_count
        FROM ks_field f
        LEFT JOIN ks_well w ON upper(trim(w.field_name)) = f.normalized_search_name
        GROUP BY f.id, f.official_field_name, f.max_oil_wells, f.max_gas_wells
        ORDER BY f.official_field_name
        LIMIT $1
        "#,
    )
    .bind(limit)
    .fetch_all(pool)
    .await
}

#[derive(Debug, Clone, sqlx::FromRow)]
pub struct PublicField {
    pub id: Uuid,
    pub official_field_name: String,
    pub active_status: String,
    pub producing_zones: Option<String>,
    pub geometry_warning: String,
}

/// Exact lookup by source (`official_field_name`) or normalized name
/// (task doc acceptance criteria).
pub async fn find_field(
    pool: &PgPool,
    name_or_normalized: &str,
) -> Result<Option<PublicField>, sqlx::Error> {
    let normalized = normalize_field_name(name_or_normalized);
    let row: Option<(Uuid, String, String, Option<String>)> = sqlx::query_as(
        "SELECT id, official_field_name, active_status, producing_zones FROM ks_field WHERE official_field_name = $1 OR normalized_search_name = $2",
    )
    .bind(name_or_normalized)
    .bind(&normalized)
    .fetch_optional(pool)
    .await?;
    Ok(row.map(
        |(id, official_field_name, active_status, producing_zones)| PublicField {
            id,
            official_field_name,
            active_status,
            producing_zones,
            geometry_warning: crate::constants::FIELD_GEOMETRY_WARNING.to_string(),
        },
    ))
}
