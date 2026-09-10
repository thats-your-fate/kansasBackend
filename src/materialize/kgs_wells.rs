//! KS-04/KS-14: materializes `ks_well_raw_snapshot` rows from a
//! `completed` `ks_ingestion_run` (KS-03) into canonical `ks_well` rows,
//! resolving identity, deriving lifecycle status, applying the
//! two-consecutive-missing-snapshots tombstone rule, and (KS-14) emitting
//! `SPUD_RECORDED`/`COMPLETION_RECORDED`/`PLUGGING_RECORDED`/
//! `WELL_STATUS_CHANGED` events for a well that already existed before
//! this run, subject to baseline-backfill suppression.

use chrono::{DateTime, NaiveDate, Utc};
use serde_json::Value;
use sqlx::PgPool;
use uuid::Uuid;

use crate::constants::{self, should_emit_event, WellLifecycleStatus};
use crate::event::{self, NewEvent};
use crate::normalize::kgs_wells::{
    api_identity_is_kansas_valid, derive_lifecycle_status, derive_location_confidence,
    parse_api_number, parse_kgs_date, ApiIdentity,
};
use crate::source::ingestion_run;

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
    pub tombstoned: i64,
    pub missing_incremented: i64,
    pub reappeared: i64,
    pub rejected_no_identity: i64,
    pub unknown_status_count: i64,
    pub events_emitted: i64,
}

/// KS-14: the prior state of a well already known to this platform,
/// fetched before upserting so a transition (a date newly recorded, or a
/// status that changed) can be detected. `None` when this run is the
/// first time this well has ever been seen.
struct PriorWellState {
    spud_date: Option<NaiveDate>,
    completion_date: Option<NaiveDate>,
    plug_date: Option<NaiveDate>,
    lifecycle_status: String,
}

/// `(internal_well_id, spud_date, completion_date, plug_date, lifecycle_status)`.
type ExistingWellRow = (
    Uuid,
    Option<NaiveDate>,
    Option<NaiveDate>,
    Option<NaiveDate>,
    String,
);

#[derive(Debug, sqlx::FromRow)]
struct RawSnapshotRow {
    source_object_id: Option<i64>,
    source_format: String,
    raw_data: Value,
    geometry_source_x: Option<f64>,
    geometry_source_y: Option<f64>,
}

/// Field values read out of `raw_data`, uniformly named regardless of
/// whether the row came from the ArcGIS or ZIP source (KS-03: the two
/// schemas differ; this is where that difference is absorbed, once, per
/// dataset).
struct WellFields {
    kid: Option<String>,
    api_number: Option<String>,
    api_num_nodash: Option<String>,
    kcc_permit: Option<String>,
    field_name: Option<String>,
    lease_name: Option<String>,
    well_name: Option<String>,
    well_class: Option<String>,
    status: Option<String>,
    status2: Option<String>,
    operator_original: Option<String>,
    operator_current: Option<String>,
    county: Option<String>,
    township: Option<i16>,
    township_direction: Option<String>,
    range: Option<i16>,
    range_direction: Option<String>,
    section: Option<i16>,
    subdivision_1: Option<String>,
    subdivision_2: Option<String>,
    subdivision_3: Option<String>,
    subdivision_4: Option<String>,
    spot: Option<String>,
    feet_north: Option<i32>,
    feet_east: Option<i32>,
    reference_corner: Option<String>,
    permit_date_raw: Option<String>,
    spud_date_raw: Option<String>,
    completion_date_raw: Option<String>,
    plug_date_raw: Option<String>,
    total_depth: Option<i32>,
    producing_formation: Option<String>,
    formation_at_total_depth: Option<String>,
    ip_oil: Option<f64>,
    ip_gas: Option<f64>,
    ip_water: Option<f64>,
    oil_kid: Option<String>,
    oil_dor_id: Option<String>,
    gas_kid: Option<String>,
    gas_dor_id: Option<String>,
    comments: Option<String>,
    location_source: Option<String>,
    source_latitude: Option<f64>,
    source_longitude: Option<f64>,
}

fn str_field(raw: &Value, key: &str) -> Option<String> {
    raw.get(key).and_then(|v| match v {
        Value::String(s) if !s.trim().is_empty() => Some(s.trim().to_string()),
        Value::Number(n) => Some(n.to_string()),
        _ => None,
    })
}

fn i16_field(raw: &Value, key: &str) -> Option<i16> {
    raw.get(key).and_then(|v| match v {
        Value::Number(n) => n.as_i64().map(|i| i as i16),
        Value::String(s) => s.trim().parse().ok(),
        _ => None,
    })
}

fn i32_field(raw: &Value, key: &str) -> Option<i32> {
    raw.get(key).and_then(|v| match v {
        Value::Number(n) => n.as_i64().map(|i| i as i32),
        Value::String(s) => s.trim().parse().ok(),
        _ => None,
    })
}

fn f64_field(raw: &Value, key: &str) -> Option<f64> {
    raw.get(key).and_then(|v| match v {
        Value::Number(n) => n.as_f64(),
        Value::String(s) => s.trim().parse().ok(),
        _ => None,
    })
}

/// Extracts `WellFields` from `raw_data`, branching on `source_format`
/// since the ArcGIS and ZIP schemas use different field names entirely
/// (confirmed at KS-03) — this is the one place that difference is
/// absorbed.
fn extract_fields(
    source_format: &str,
    raw_data: &Value,
    geometry_source_x: Option<f64>,
    geometry_source_y: Option<f64>,
) -> WellFields {
    if source_format == "zip" {
        WellFields {
            kid: str_field(raw_data, "KID"),
            api_number: str_field(raw_data, "API_NUMBER"),
            api_num_nodash: str_field(raw_data, "API_NUM_NODASH"),
            kcc_permit: str_field(raw_data, "KCC_PERMIT"),
            field_name: str_field(raw_data, "FIELD"),
            lease_name: str_field(raw_data, "LEASE"),
            well_name: str_field(raw_data, "WELL"),
            // The ZIP export has no WELL_CLASS field at all (confirmed at
            // KS-03) — status derivation for zip-sourced rows relies on
            // STATUS/STATUS2 only.
            well_class: None,
            status: str_field(raw_data, "STATUS"),
            status2: str_field(raw_data, "STATUS2"),
            operator_original: str_field(raw_data, "ORIG_OPERATOR"),
            operator_current: str_field(raw_data, "CURR_OPERATOR"),
            // The ZIP export has no COUNTY field either — membership
            // classification for zip rows never had a county fallback
            // (KS-03); materialization has none either.
            county: None,
            township: i16_field(raw_data, "TOWNSHIP"),
            township_direction: str_field(raw_data, "TWN_DIR"),
            range: i16_field(raw_data, "RANGE"),
            range_direction: str_field(raw_data, "RANGE_DIR"),
            section: i16_field(raw_data, "SECTION"),
            subdivision_1: None,
            subdivision_2: None,
            subdivision_3: None,
            subdivision_4: None,
            spot: str_field(raw_data, "SPOT"),
            feet_north: i32_field(raw_data, "FEET_NORTH"),
            feet_east: i32_field(raw_data, "FEET_EAST"),
            reference_corner: str_field(raw_data, "FOOT_REF"),
            permit_date_raw: str_field(raw_data, "PERMIT"),
            spud_date_raw: str_field(raw_data, "SPUD"),
            completion_date_raw: str_field(raw_data, "COMPLETION"),
            plug_date_raw: str_field(raw_data, "PLUGGING"),
            total_depth: i32_field(raw_data, "DEPTH"),
            producing_formation: str_field(raw_data, "PRODUCE_FORM"),
            formation_at_total_depth: str_field(raw_data, "FORMATION_AT_TOTAL_DEPTH"),
            ip_oil: f64_field(raw_data, "IP_OIL"),
            ip_gas: f64_field(raw_data, "IP_GAS"),
            ip_water: f64_field(raw_data, "IP_WATER"),
            oil_kid: str_field(raw_data, "OIL_KID"),
            oil_dor_id: str_field(raw_data, "OIL_DOR_ID"),
            gas_kid: str_field(raw_data, "GAS_KID"),
            gas_dor_id: str_field(raw_data, "GAS_DOR_ID"),
            comments: str_field(raw_data, "COMMENTS"),
            location_source: str_field(raw_data, "LONG_LAT_SOURCE"),
            // Confirmed NAD27 by KGS's own file_format.html (KS-04).
            source_latitude: geometry_source_y,
            source_longitude: geometry_source_x,
        }
    } else {
        WellFields {
            kid: str_field(raw_data, "KID"),
            api_number: str_field(raw_data, "API_NUMBER"),
            api_num_nodash: str_field(raw_data, "API_NUM_NODASH"),
            kcc_permit: str_field(raw_data, "KCC_PERMIT"),
            field_name: str_field(raw_data, "FIELD_NAME"),
            lease_name: str_field(raw_data, "LEASE_NAME"),
            well_name: str_field(raw_data, "WELL_NAME"),
            well_class: str_field(raw_data, "WELL_CLASS"),
            status: str_field(raw_data, "STATUS"),
            status2: str_field(raw_data, "STATUS2"),
            operator_original: str_field(raw_data, "OPERATOR_NAME"),
            operator_current: str_field(raw_data, "CURR_OPERATOR"),
            county: str_field(raw_data, "COUNTY"),
            township: i16_field(raw_data, "TOWNSHIP"),
            township_direction: str_field(raw_data, "TOWNSHIP_DIRECTION"),
            range: i16_field(raw_data, "RANGE"),
            range_direction: str_field(raw_data, "RANGE_DIRECTION"),
            section: i16_field(raw_data, "SECTION"),
            subdivision_1: str_field(raw_data, "SUBDIVISION_1_LARGEST"),
            subdivision_2: str_field(raw_data, "SUBDIVISION_2"),
            subdivision_3: str_field(raw_data, "SUBDIVISION_3"),
            subdivision_4: str_field(raw_data, "SUBDIVISION_4_SMALLEST"),
            spot: str_field(raw_data, "SPOT"),
            feet_north: i32_field(raw_data, "FEET_NORTH_FROM_REFERENCE"),
            feet_east: i32_field(raw_data, "FEET_EAST_FROM_REFERENCE"),
            reference_corner: str_field(raw_data, "REFERENCE_CORNER"),
            permit_date_raw: str_field(raw_data, "PERMIT_DATE_TXT"),
            spud_date_raw: str_field(raw_data, "SPUD_DATE_TXT"),
            completion_date_raw: str_field(raw_data, "COMPLETION_DATE_TXT"),
            plug_date_raw: str_field(raw_data, "PLUG_DATE_TXT"),
            total_depth: i32_field(raw_data, "ROTARY_TOTAL_DEPTH"),
            producing_formation: str_field(raw_data, "PRODUCING_FORMATION"),
            formation_at_total_depth: str_field(raw_data, "FORMATION_AT_TOTAL_DEPT"),
            ip_oil: f64_field(raw_data, "IP_OIL"),
            ip_gas: f64_field(raw_data, "IP_GAS"),
            ip_water: f64_field(raw_data, "IP_WATER"),
            oil_kid: str_field(raw_data, "OIL_KID"),
            oil_dor_id: str_field(raw_data, "OIL_DOR_ID"),
            gas_kid: str_field(raw_data, "GAS_KID"),
            gas_dor_id: str_field(raw_data, "GAS_DOR_ID"),
            comments: str_field(raw_data, "COMMENTS"),
            location_source: str_field(raw_data, "LONGITUDE_LATITUDE_SOURCE"),
            // NAD27_LATITUDE/LONGITUDE, not the outSR=4326-reprojected
            // geometry (that's geometry_source_x/y, used only for
            // display_geometry_4326 below).
            source_latitude: f64_field(raw_data, "NAD27_LATITUDE"),
            source_longitude: f64_field(raw_data, "NAD27_LONGITUDE"),
        }
    }
}

/// Materializes every `included` raw row for `ingestion_run_id` into
/// `ks_well`, then applies the tombstone rule to previously-materialized
/// wells of the same `source_format` that this run didn't touch. Refuses
/// to run against anything but a `completed` run — that's the "publish
/// only after QA passes" guarantee carrying through from KS-03.
pub async fn materialize_wells_from_run(
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
        SELECT source_object_id, source_format, raw_data, geometry_source_x, geometry_source_y
        FROM ks_well_raw_snapshot
        WHERE ingestion_run_id = $1 AND kansas_membership_status = 'included'
        "#,
    )
    .bind(ingestion_run_id)
    .fetch_all(pool)
    .await?;

    let source_format = rows
        .first()
        .map(|r| r.source_format.clone())
        .unwrap_or_else(|| "arcgis".to_string());

    let (is_baseline_backfill, force_historical) =
        ingestion_run::event_policy(pool, ingestion_run_id).await?;
    let cutoff = constants::live_monitoring_cutoff();

    let mut outcome = MaterializeOutcome::default();
    let mut touched_well_ids: Vec<Uuid> = Vec::with_capacity(rows.len());

    for row in &rows {
        let fields = extract_fields(
            &row.source_format,
            &row.raw_data,
            row.geometry_source_x,
            row.geometry_source_y,
        );
        let identity = parse_api_number(
            fields.api_num_nodash.as_deref(),
            fields.api_number.as_deref(),
        );
        let is_kansas_valid_api = api_identity_is_kansas_valid(&identity);

        let match_method: &'static str = if is_kansas_valid_api {
            "api14"
        } else if fields.kid.is_some() {
            "kid_fallback"
        } else {
            outcome.rejected_no_identity += 1;
            ingestion_run::record_rejection(
                pool,
                ingestion_run_id,
                row.source_object_id,
                Some(row.raw_data.clone()),
                "no valid API14 and no KID: cannot resolve a stable well identity",
            )
            .await?;
            continue;
        };

        let lifecycle = derive_lifecycle_status(
            fields.well_class.as_deref(),
            fields.status.as_deref(),
            fields.status2.as_deref(),
        );
        if lifecycle == WellLifecycleStatus::Unknown {
            outcome.unknown_status_count += 1;
        }

        let existing_well: Option<ExistingWellRow> = if match_method == "api14" {
            sqlx::query_as("SELECT internal_well_id, spud_date, completion_date, plug_date, lifecycle_status FROM ks_well WHERE api14 = $1")
                .bind(identity.api14.as_ref())
                .fetch_optional(pool)
                .await?
        } else {
            sqlx::query_as("SELECT internal_well_id, spud_date, completion_date, plug_date, lifecycle_status FROM ks_well WHERE kgs_kid = $1 AND api14 IS NULL")
                .bind(fields.kid.as_ref())
                .fetch_optional(pool)
                .await?
        };
        let existing_well_id = existing_well.as_ref().map(|(id, ..)| *id);
        let prior = existing_well.map(
            |(_, spud_date, completion_date, plug_date, lifecycle_status)| PriorWellState {
                spud_date,
                completion_date,
                plug_date,
                lifecycle_status,
            },
        );

        let well_id = upsert_well(
            pool,
            existing_well_id,
            &fields,
            &identity,
            lifecycle,
            match_method,
            &row.source_format,
            ingestion_run_id,
            row.source_object_id,
        )
        .await?;

        if existing_well_id.is_some() {
            outcome.updated += 1;
        } else {
            outcome.created += 1;
        }

        if let Some(prior) = prior {
            let new_spud_date = parse_kgs_date(fields.spud_date_raw.as_deref(), &row.source_format);
            let new_completion_date =
                parse_kgs_date(fields.completion_date_raw.as_deref(), &row.source_format);
            let new_plug_date = parse_kgs_date(fields.plug_date_raw.as_deref(), &row.source_format);
            let new_lifecycle_str = serde_json::to_value(lifecycle)
                .ok()
                .and_then(|v| v.as_str().map(str::to_string))
                .unwrap_or_else(|| "unknown".to_string());

            let observed_at = Utc::now();
            let mut events_here = 0_i64;
            events_here += emit_well_date_event_if_changed(
                pool,
                event::SPUD_RECORDED,
                well_id,
                prior.spud_date,
                new_spud_date,
                ingestion_run_id,
                observed_at,
                is_baseline_backfill,
                force_historical,
                cutoff,
            )
            .await?;
            events_here += emit_well_date_event_if_changed(
                pool,
                event::COMPLETION_RECORDED,
                well_id,
                prior.completion_date,
                new_completion_date,
                ingestion_run_id,
                observed_at,
                is_baseline_backfill,
                force_historical,
                cutoff,
            )
            .await?;
            events_here += emit_well_date_event_if_changed(
                pool,
                event::PLUGGING_RECORDED,
                well_id,
                prior.plug_date,
                new_plug_date,
                ingestion_run_id,
                observed_at,
                is_baseline_backfill,
                force_historical,
                cutoff,
            )
            .await?;

            if prior.lifecycle_status != new_lifecycle_str
                && should_emit_event(is_baseline_backfill, None, cutoff, force_historical)
            {
                let dedup_key =
                    format!("{well_id}:{}:{}", prior.lifecycle_status, new_lifecycle_str);
                let inserted = event::record_event(
                    pool,
                    NewEvent {
                        event_type: event::WELL_STATUS_CHANGED,
                        dedup_key: &dedup_key,
                        occurred_at: None,
                        published_at: None,
                        observed_at,
                        payload: serde_json::json!({
                            "well_id": well_id,
                            "previous_status": prior.lifecycle_status,
                            "new_status": new_lifecycle_str,
                        }),
                        source_ingestion_run_id: ingestion_run_id,
                        related_well_id: Some(well_id),
                        related_operator_id: None,
                        related_field_id: None,
                        related_lease_id: None,
                        related_county: fields.county.as_deref(),
                        related_well_ids: &[],
                    },
                )
                .await?;
                if inserted {
                    events_here += 1;
                }
            }
            outcome.events_emitted += events_here;
        }

        for (alias_type, value) in [
            ("kgs_kid", fields.kid.as_deref()),
            ("kcc_permit", fields.kcc_permit.as_deref()),
            ("api_raw", identity.api_raw.as_deref()),
        ] {
            if let Some(value) = value {
                upsert_alias(pool, well_id, alias_type, value).await?;
            }
        }

        touched_well_ids.push(well_id);
    }

    let missing_wells: Vec<(Uuid, i32, bool)> = sqlx::query_as(
        r#"
        SELECT internal_well_id, consecutive_missing_snapshot_count, is_tombstoned
        FROM ks_well
        WHERE source_format = $1 AND internal_well_id != ALL($2)
        "#,
    )
    .bind(&source_format)
    .bind(&touched_well_ids)
    .fetch_all(pool)
    .await?;

    for (well_id, missing_count, was_tombstoned) in missing_wells {
        let new_count = missing_count + 1;
        if new_count >= 2 {
            sqlx::query(
                r#"
                UPDATE ks_well
                SET consecutive_missing_snapshot_count = $2, is_tombstoned = true,
                    tombstoned_at = COALESCE(tombstoned_at, now()), updated_at = now()
                WHERE internal_well_id = $1
                "#,
            )
            .bind(well_id)
            .bind(new_count)
            .execute(pool)
            .await?;
            if !was_tombstoned {
                outcome.tombstoned += 1;
            }
        } else {
            sqlx::query(
                "UPDATE ks_well SET consecutive_missing_snapshot_count = $2, updated_at = now() WHERE internal_well_id = $1",
            )
            .bind(well_id)
            .bind(new_count)
            .execute(pool)
            .await?;
            outcome.missing_incremented += 1;
        }
    }

    Ok(outcome)
}

#[allow(clippy::too_many_arguments)]
async fn upsert_well(
    pool: &PgPool,
    existing_well_id: Option<Uuid>,
    fields: &WellFields,
    identity: &ApiIdentity,
    lifecycle: WellLifecycleStatus,
    match_method: &str,
    source_format: &str,
    ingestion_run_id: Uuid,
    source_object_id: Option<i64>,
) -> Result<Uuid, sqlx::Error> {
    let permit_date = parse_kgs_date(fields.permit_date_raw.as_deref(), source_format);
    let spud_date = parse_kgs_date(fields.spud_date_raw.as_deref(), source_format);
    let completion_date = parse_kgs_date(fields.completion_date_raw.as_deref(), source_format);
    let plug_date = parse_kgs_date(fields.plug_date_raw.as_deref(), source_format);
    let location_confidence = derive_location_confidence(fields.location_source.as_deref());

    // Display geometry is WGS84 and is only ever built from 'arcgis' rows
    // (outSR=4326 was actually requested there); 'zip' rows' NAD27
    // coordinates are preserved in source_latitude/source_longitude with
    // no display-geometry claim, until a real datum transform exists.
    let (display_lon, display_lat): (Option<f64>, Option<f64>) = if source_format == "arcgis" {
        (fields.source_longitude, fields.source_latitude)
    } else {
        (None, None)
    };
    let location_warning: Option<&str> = if source_format == "zip" {
        Some("Source coordinates are NAD27 (ZIP export); no WGS84 display geometry available for this row yet")
    } else {
        None
    };

    let lifecycle_str = serde_json::to_value(lifecycle)
        .ok()
        .and_then(|v| v.as_str().map(str::to_string))
        .unwrap_or_else(|| "unknown".to_string());
    let api_validation_status_str = serde_json::to_value(identity.api_validation_status)
        .ok()
        .and_then(|v| v.as_str().map(str::to_string))
        .unwrap_or_else(|| "missing".to_string());

    let well_id: Uuid = sqlx::query_scalar(
        r#"
        INSERT INTO ks_well (
            internal_well_id, state_code, api_raw, api14, api_base, api_suffix,
            api_validation_status, well_identity_match_method, kgs_kid, kcc_permit,
            field_name, lease_name, well_name, well_class_raw, status_raw, status2_raw,
            lifecycle_status, operator_name_kgs_original, operator_name_kgs_reported_current,
            county, township, township_direction, range, range_direction, section,
            subdivision_1_largest, subdivision_2, subdivision_3, subdivision_4_smallest,
            spot, feet_north_from_reference, feet_east_from_reference, reference_corner,
            permit_date, permit_date_raw, spud_date, spud_date_raw,
            completion_date, completion_date_raw, plug_date, plug_date_raw,
            total_depth, producing_formation, formation_at_total_depth,
            ip_oil, ip_gas, ip_water, oil_kid, oil_dor_id, gas_kid, gas_dor_id, comments,
            source_latitude, source_longitude, source_crs, display_geometry_4326,
            location_source, location_confidence, location_warning,
            source_format, source_ingestion_run_id, source_object_id,
            consecutive_missing_snapshot_count, is_tombstoned, tombstoned_at,
            first_seen_at, last_seen_at
        )
        VALUES (
            COALESCE($1, gen_random_uuid()), 'KS', $2, $3, $4, $5,
            $6, $7, $8, $9,
            $10, $11, $12, $13, $14, $15,
            $16, $17, $18,
            $19, $20, $21, $22, $23, $24,
            $25, $26, $27, $28,
            $29, $30, $31, $32,
            $33, $34, $35, $36,
            $37, $38, $39, $40,
            $41, $42, $43,
            $44, $45, $46, $47, $48, $49, $50, $51,
            $52, $53, 'NAD27',
            CASE WHEN $54 IS NOT NULL AND $55 IS NOT NULL THEN ST_SetSRID(ST_MakePoint($54, $55), 4326) ELSE NULL END,
            $56, $57, $58,
            $59, $60, $61,
            0, false, NULL,
            now(), now()
        )
        ON CONFLICT (internal_well_id) DO UPDATE SET
            api_raw = EXCLUDED.api_raw, api14 = EXCLUDED.api14, api_base = EXCLUDED.api_base,
            api_suffix = EXCLUDED.api_suffix, api_validation_status = EXCLUDED.api_validation_status,
            well_identity_match_method = EXCLUDED.well_identity_match_method,
            kgs_kid = EXCLUDED.kgs_kid, kcc_permit = EXCLUDED.kcc_permit,
            field_name = EXCLUDED.field_name, lease_name = EXCLUDED.lease_name, well_name = EXCLUDED.well_name,
            well_class_raw = EXCLUDED.well_class_raw, status_raw = EXCLUDED.status_raw, status2_raw = EXCLUDED.status2_raw,
            lifecycle_status = EXCLUDED.lifecycle_status,
            operator_name_kgs_original = EXCLUDED.operator_name_kgs_original,
            operator_name_kgs_reported_current = EXCLUDED.operator_name_kgs_reported_current,
            county = EXCLUDED.county, township = EXCLUDED.township, township_direction = EXCLUDED.township_direction,
            range = EXCLUDED.range, range_direction = EXCLUDED.range_direction, section = EXCLUDED.section,
            subdivision_1_largest = EXCLUDED.subdivision_1_largest, subdivision_2 = EXCLUDED.subdivision_2,
            subdivision_3 = EXCLUDED.subdivision_3, subdivision_4_smallest = EXCLUDED.subdivision_4_smallest,
            spot = EXCLUDED.spot, feet_north_from_reference = EXCLUDED.feet_north_from_reference,
            feet_east_from_reference = EXCLUDED.feet_east_from_reference, reference_corner = EXCLUDED.reference_corner,
            permit_date = EXCLUDED.permit_date, permit_date_raw = EXCLUDED.permit_date_raw,
            spud_date = EXCLUDED.spud_date, spud_date_raw = EXCLUDED.spud_date_raw,
            completion_date = EXCLUDED.completion_date, completion_date_raw = EXCLUDED.completion_date_raw,
            plug_date = EXCLUDED.plug_date, plug_date_raw = EXCLUDED.plug_date_raw,
            total_depth = EXCLUDED.total_depth, producing_formation = EXCLUDED.producing_formation,
            formation_at_total_depth = EXCLUDED.formation_at_total_depth,
            ip_oil = EXCLUDED.ip_oil, ip_gas = EXCLUDED.ip_gas, ip_water = EXCLUDED.ip_water,
            oil_kid = EXCLUDED.oil_kid, oil_dor_id = EXCLUDED.oil_dor_id,
            gas_kid = EXCLUDED.gas_kid, gas_dor_id = EXCLUDED.gas_dor_id, comments = EXCLUDED.comments,
            source_latitude = EXCLUDED.source_latitude, source_longitude = EXCLUDED.source_longitude,
            display_geometry_4326 = EXCLUDED.display_geometry_4326,
            location_source = EXCLUDED.location_source, location_confidence = EXCLUDED.location_confidence,
            location_warning = EXCLUDED.location_warning,
            source_format = EXCLUDED.source_format, source_ingestion_run_id = EXCLUDED.source_ingestion_run_id,
            source_object_id = EXCLUDED.source_object_id,
            consecutive_missing_snapshot_count = 0, is_tombstoned = false, tombstoned_at = NULL,
            last_seen_at = now(), updated_at = now()
        RETURNING internal_well_id
        "#,
    )
    .bind(existing_well_id)
    .bind(&identity.api_raw)
    .bind(&identity.api14)
    .bind(&identity.api_base)
    .bind(&identity.api_suffix)
    .bind(&api_validation_status_str)
    .bind(match_method)
    .bind(&fields.kid)
    .bind(&fields.kcc_permit)
    .bind(&fields.field_name)
    .bind(&fields.lease_name)
    .bind(&fields.well_name)
    .bind(&fields.well_class)
    .bind(&fields.status)
    .bind(&fields.status2)
    .bind(&lifecycle_str)
    .bind(&fields.operator_original)
    .bind(&fields.operator_current)
    .bind(&fields.county)
    .bind(fields.township)
    .bind(&fields.township_direction)
    .bind(fields.range)
    .bind(&fields.range_direction)
    .bind(fields.section)
    .bind(&fields.subdivision_1)
    .bind(&fields.subdivision_2)
    .bind(&fields.subdivision_3)
    .bind(&fields.subdivision_4)
    .bind(&fields.spot)
    .bind(fields.feet_north)
    .bind(fields.feet_east)
    .bind(&fields.reference_corner)
    .bind(permit_date)
    .bind(&fields.permit_date_raw)
    .bind(spud_date)
    .bind(&fields.spud_date_raw)
    .bind(completion_date)
    .bind(&fields.completion_date_raw)
    .bind(plug_date)
    .bind(&fields.plug_date_raw)
    .bind(fields.total_depth)
    .bind(&fields.producing_formation)
    .bind(&fields.formation_at_total_depth)
    .bind(fields.ip_oil)
    .bind(fields.ip_gas)
    .bind(fields.ip_water)
    .bind(&fields.oil_kid)
    .bind(&fields.oil_dor_id)
    .bind(&fields.gas_kid)
    .bind(&fields.gas_dor_id)
    .bind(&fields.comments)
    .bind(fields.source_latitude)
    .bind(fields.source_longitude)
    .bind(display_lon)
    .bind(display_lat)
    .bind(&fields.location_source)
    .bind(location_confidence)
    .bind(location_warning)
    .bind(source_format)
    .bind(ingestion_run_id)
    .bind(source_object_id)
    .fetch_one(pool)
    .await?;

    Ok(well_id)
}

/// KS-14: emits `event_type` when `new_date` is newly set or has been
/// corrected to a different value than `old_date` — never on a well's
/// very first materialize (there is no "old" to transition from, and
/// `prior` is only `Some` for a well that already existed). The dedup
/// key embeds the actual date, so a genuine correction (a different date
/// observed later) produces a new event while a plain re-observation of
/// the same date is silently absorbed — KS-14's "revision-safe dedupe"
/// and "late-arriving source records supported" acceptance criteria.
#[allow(clippy::too_many_arguments)]
async fn emit_well_date_event_if_changed(
    pool: &PgPool,
    event_type: &'static str,
    well_id: Uuid,
    old_date: Option<NaiveDate>,
    new_date: Option<NaiveDate>,
    ingestion_run_id: Uuid,
    observed_at: DateTime<Utc>,
    is_baseline_backfill: bool,
    force_historical: bool,
    cutoff: DateTime<Utc>,
) -> Result<i64, sqlx::Error> {
    let Some(new_date) = new_date else {
        return Ok(0);
    };
    if old_date == Some(new_date) {
        return Ok(0);
    }
    let occurred_at = new_date.and_hms_opt(0, 0, 0).map(|dt| dt.and_utc());
    if !should_emit_event(is_baseline_backfill, occurred_at, cutoff, force_historical) {
        return Ok(0);
    }
    let dedup_key = format!("{well_id}:{event_type}:{new_date}");
    let inserted = event::record_event(
        pool,
        NewEvent {
            event_type,
            dedup_key: &dedup_key,
            occurred_at,
            published_at: None,
            observed_at,
            payload: serde_json::json!({ "well_id": well_id, "date": new_date }),
            source_ingestion_run_id: ingestion_run_id,
            related_well_id: Some(well_id),
            related_operator_id: None,
            related_field_id: None,
            related_lease_id: None,
            related_county: None,
            related_well_ids: &[],
        },
    )
    .await?;
    Ok(inserted as i64)
}

async fn upsert_alias(
    pool: &PgPool,
    well_id: Uuid,
    alias_type: &str,
    alias_value: &str,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        r#"
        INSERT INTO ks_well_source_alias (well_id, alias_type, alias_value)
        VALUES ($1, $2, $3)
        ON CONFLICT (well_id, alias_type, alias_value) DO UPDATE SET last_seen_at = now()
        "#,
    )
    .bind(well_id)
    .bind(alias_type)
    .bind(alias_value)
    .execute(pool)
    .await?;
    Ok(())
}

/// The KS-04 unknown-status QA report: distinct raw `(well_class, status,
/// status2)` triples currently mapped to `Unknown`, for human review —
/// never silently treated as `Inactive` (task doc §3.5).
#[derive(Debug, sqlx::FromRow, PartialEq, Eq)]
pub struct UnknownStatusCombination {
    pub well_class_raw: Option<String>,
    pub status_raw: Option<String>,
    pub status2_raw: Option<String>,
    pub well_count: i64,
}

pub async fn unknown_status_report(
    pool: &PgPool,
) -> Result<Vec<UnknownStatusCombination>, sqlx::Error> {
    sqlx::query_as(
        r#"
        SELECT well_class_raw, status_raw, status2_raw, count(*) AS well_count
        FROM ks_well
        WHERE lifecycle_status = 'unknown'
        GROUP BY well_class_raw, status_raw, status2_raw
        ORDER BY well_count DESC
        "#,
    )
    .fetch_all(pool)
    .await
}
