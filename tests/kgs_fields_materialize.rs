//! KS-08: materializer tests. Stages raw rows directly (bypassing the
//! full ArcGIS+xlsx pipeline, already covered by
//! `tests/kgs_fields_import.rs`) so these stay focused on materialize
//! logic: exact-name merging across source components, geometry
//! validity, and well-to-field/production reconciliation.

use future_wells_kansas::materialize::kgs_fields::{
    find_field, find_wells_by_field, materialize_fields_from_run, well_count_reconciliation,
    MaterializeError,
};
use future_wells_kansas::source::ingestion_run::{self, DownloadedPayload};
use future_wells_kansas::source::registry;
use serde_json::json;
use sqlx::PgPool;
use uuid::Uuid;

async fn test_pool() -> PgPool {
    let database_url =
        std::env::var("DATABASE_URL").expect("DATABASE_URL must be set to run these tests");
    let pool = future_wells_kansas::db::connect(&database_url)
        .await
        .expect("failed to connect to test database");
    sqlx::migrate!("./migrations").run(&pool).await.unwrap();
    pool
}

struct RawArcgisRow<'a> {
    field_kid: &'a str,
    field_name: &'a str,
    status: &'a str,
    field_type: &'a str,
    formations: Option<&'a str>,
    geometry: Option<serde_json::Value>,
}

struct RawFileRow<'a> {
    component: &'a str,
    field_name: &'a str,
    producing_zones: Option<&'a str>,
    total_oil_prod: Option<f64>,
    total_gas_prod: Option<f64>,
    annual: serde_json::Value,
}

async fn stage_run(
    pool: &PgPool,
    arcgis_rows: &[RawArcgisRow<'_>],
    file_rows: &[RawFileRow<'_>],
) -> Uuid {
    let source = registry::find_source_by_code(pool, "KGS_FIELDS")
        .await
        .unwrap()
        .unwrap();
    let checksum = format!("checksum-{}", Uuid::new_v4());
    let run_id = ingestion_run::start_run(
        pool,
        DownloadedPayload {
            source_id: source.id,
            source_url: "test-fixture",
            retrieved_at: chrono::Utc::now(),
            source_modified_at: None,
            payload_checksum: &checksum,
            schema_fingerprint: None,
            parser_version: "test-fixture",
        },
    )
    .await
    .unwrap();

    for (i, row) in arcgis_rows.iter().enumerate() {
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
        .bind(i as i64)
        .bind(row.field_kid)
        .bind(row.field_name)
        .bind(row.status)
        .bind(row.field_type)
        .bind(row.formations)
        .bind(&row.geometry)
        .bind(json!({"FIELD_NAME": row.field_name, "APPROXACRE": 640.0, "MAXOILWELL": 10.0, "MAXGASWELL": 5.0}))
        .bind(format!("hash-{}", Uuid::new_v4()))
        .execute(pool)
        .await
        .unwrap();
    }

    for row in file_rows {
        sqlx::query(
            r#"
            INSERT INTO ks_field_raw_snapshot (
                ingestion_run_id, source_component, field_name, producing_zones,
                total_oil_prod, total_gas_prod, parsed_annual, raw_data, source_record_hash
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, '{}'::jsonb, $8)
            "#,
        )
        .bind(run_id)
        .bind(row.component)
        .bind(row.field_name)
        .bind(row.producing_zones)
        .bind(row.total_oil_prod)
        .bind(row.total_gas_prod)
        .bind(&row.annual)
        .bind(format!("hash-{}", Uuid::new_v4()))
        .execute(pool)
        .await
        .unwrap();
    }

    ingestion_run::complete_run(
        pool,
        run_id,
        (arcgis_rows.len() + file_rows.len()) as i64,
        0,
        None,
        None,
        json!({}),
    )
    .await
    .unwrap();
    run_id
}

fn square_polygon(min_lon: f64, min_lat: f64, max_lon: f64, max_lat: f64) -> serde_json::Value {
    json!({
        "type": "Polygon",
        "coordinates": [[[min_lon, min_lat], [min_lon, max_lat], [max_lon, max_lat], [max_lon, min_lat], [min_lon, min_lat]]],
    })
}

/// A classic self-intersecting "bowtie" polygon — invalid per PostGIS's
/// `ST_IsValid`.
fn bowtie_polygon() -> serde_json::Value {
    json!({
        "type": "Polygon",
        "coordinates": [[[0.0, 0.0], [1.0, 1.0], [1.0, 0.0], [0.0, 1.0], [0.0, 0.0]]],
    })
}

#[tokio::test]
async fn merges_arcgis_and_active_file_rows_by_exact_name() {
    let pool = test_pool().await;
    let run_id = stage_run(
        &pool,
        &[RawArcgisRow {
            field_kid: "K7001",
            field_name: "MERGE TEST FIELD",
            status: "Active",
            field_type: "Oil",
            formations: None,
            geometry: Some(square_polygon(-98.0, 38.0, -97.9, 38.1)),
        }],
        &[RawFileRow {
            component: "active_file",
            field_name: "MERGE TEST FIELD",
            producing_zones: Some("MISSISSIPPIAN"),
            total_oil_prod: Some(1000.0),
            total_gas_prod: Some(0.0),
            annual: json!({"2020": {"oil_prod": 100.0, "oil_wells": 3, "gas_prod": null, "gas_wells": null}}),
        }],
    )
    .await;

    let outcome = materialize_fields_from_run(&pool, run_id).await.unwrap();
    assert_eq!(outcome.created, 1);

    let field = find_field(&pool, "MERGE TEST FIELD")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(field.official_field_name, "MERGE TEST FIELD");
    assert_eq!(field.active_status, "active");
    assert_eq!(field.producing_zones.as_deref(), Some("MISSISSIPPIAN"));
    assert!(field.geometry_warning.contains("not legal lease"));

    let (year, oil_prod): (i32, f64) = sqlx::query_as(
        "SELECT year, oil_production FROM ks_field_annual_production WHERE field_id = $1",
    )
    .bind(field.id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(year, 2020);
    assert_eq!(oil_prod, 100.0);
}

#[tokio::test]
async fn exact_name_lookup_is_never_a_fuzzy_match() {
    let pool = test_pool().await;
    let first_run_id = stage_run(
        &pool,
        &[RawArcgisRow {
            field_kid: "K7101",
            field_name: "EXACT NAME FIELD",
            status: "Active",
            field_type: "Gas",
            formations: None,
            geometry: None,
        }],
        &[],
    )
    .await;
    materialize_fields_from_run(&pool, first_run_id)
        .await
        .unwrap();
    let run_id = stage_run(
        &pool,
        &[RawArcgisRow {
            field_kid: "K7102",
            field_name: "EXACT NAME FIELD EAST",
            status: "Active",
            field_type: "Gas",
            formations: None,
            geometry: None,
        }],
        &[],
    )
    .await;
    materialize_fields_from_run(&pool, run_id).await.unwrap();

    // Two fields whose names merely share a directional-word prefix must
    // never collapse into one row (task doc: "do not merge two official
    // field names merely because punctuation or directional words look
    // similar").
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM ks_field WHERE official_field_name IN ('EXACT NAME FIELD', 'EXACT NAME FIELD EAST')").fetch_one(&pool).await.unwrap();
    assert_eq!(count, 2);

    assert!(find_field(&pool, "EXACT NAME FIELD")
        .await
        .unwrap()
        .is_some());
    assert!(
        find_field(&pool, "exact name field")
            .await
            .unwrap()
            .is_some(),
        "normalized-name lookup must still work"
    );
    assert!(
        find_field(&pool, "EXACT NAME").await.unwrap().is_none(),
        "a partial name must never match"
    );
}

#[tokio::test]
async fn invalid_geometry_is_stored_and_flagged_not_silently_repaired() {
    let pool = test_pool().await;
    let run_id = stage_run(
        &pool,
        &[RawArcgisRow {
            field_kid: "K7201",
            field_name: "BOWTIE FIELD",
            status: "Active",
            field_type: "Oil",
            formations: None,
            geometry: Some(bowtie_polygon()),
        }],
        &[],
    )
    .await;

    let outcome = materialize_fields_from_run(&pool, run_id).await.unwrap();
    assert_eq!(outcome.geometry_invalid_count, 1);

    let (is_valid, reason): (Option<bool>, Option<String>) =
        sqlx::query_as("SELECT geometry_is_valid, geometry_validity_reason FROM ks_field WHERE official_field_name = 'BOWTIE FIELD'").fetch_one(&pool).await.unwrap();
    assert_eq!(is_valid, Some(false));
    assert!(reason.is_some());

    // The geometry itself is still stored, not discarded.
    let has_geometry: bool = sqlx::query_scalar("SELECT display_geometry IS NOT NULL FROM ks_field WHERE official_field_name = 'BOWTIE FIELD'").fetch_one(&pool).await.unwrap();
    assert!(has_geometry);
}

#[tokio::test]
async fn active_status_is_unknown_when_a_field_has_no_arcgis_row() {
    let pool = test_pool().await;
    let run_id = stage_run(
        &pool,
        &[],
        &[RawFileRow {
            component: "active_file",
            field_name: "FILE ONLY FIELD",
            producing_zones: Some("CHEROKEE"),
            total_oil_prod: Some(50.0),
            total_gas_prod: None,
            annual: json!({}),
        }],
    )
    .await;

    materialize_fields_from_run(&pool, run_id).await.unwrap();
    let field = find_field(&pool, "FILE ONLY FIELD").await.unwrap().unwrap();
    assert_eq!(field.active_status, "unknown");
}

#[tokio::test]
async fn abandoned_file_only_field_is_marked_abandoned() {
    let pool = test_pool().await;
    let run_id = stage_run(
        &pool,
        &[],
        &[RawFileRow {
            component: "abandoned_file",
            field_name: "OLD ABANDONED FIELD",
            producing_zones: Some("LANSING-KANSAS CITY"),
            total_oil_prod: Some(200.0),
            total_gas_prod: None,
            annual: json!({}),
        }],
    )
    .await;

    materialize_fields_from_run(&pool, run_id).await.unwrap();
    let field = find_field(&pool, "OLD ABANDONED FIELD")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(field.active_status, "abandoned");
}

#[tokio::test]
async fn well_to_field_relationship_resolves_by_exact_normalized_name() {
    let pool = test_pool().await;
    let run_id = stage_run(
        &pool,
        &[RawArcgisRow {
            field_kid: "K7301",
            field_name: "WELL LINK FIELD",
            status: "Active",
            field_type: "Oil",
            formations: None,
            geometry: None,
        }],
        &[],
    )
    .await;
    materialize_fields_from_run(&pool, run_id).await.unwrap();
    let field = find_field(&pool, "WELL LINK FIELD").await.unwrap().unwrap();

    let well_run_id = registry::find_source_by_code(&pool, "KGS_WELLS_ARCGIS")
        .await
        .unwrap()
        .unwrap()
        .id;
    let well_ingestion_run = ingestion_run::start_run(
        &pool,
        DownloadedPayload {
            source_id: well_run_id,
            source_url: "test-fixture",
            retrieved_at: chrono::Utc::now(),
            source_modified_at: None,
            payload_checksum: &format!("checksum-{}", Uuid::new_v4()),
            schema_fingerprint: None,
            parser_version: "test-fixture",
        },
    )
    .await
    .unwrap();
    let well_id: Uuid = sqlx::query_scalar(
        r#"
        INSERT INTO ks_well (api_validation_status, well_identity_match_method, lifecycle_status, source_format, source_ingestion_run_id, field_name)
        VALUES ('valid', 'api14', 'unknown', 'arcgis', $1, 'Well Link Field')
        RETURNING internal_well_id
        "#,
    )
    .bind(well_ingestion_run)
    .fetch_one(&pool)
    .await
    .unwrap();

    let wells = find_wells_by_field(&pool, field.id).await.unwrap();
    assert_eq!(wells, vec![well_id]);

    let reconciliation = well_count_reconciliation(&pool, 1000).await.unwrap();
    let entry = reconciliation
        .iter()
        .find(|r| r.official_field_name == "WELL LINK FIELD")
        .unwrap();
    assert_eq!(entry.materialized_well_count, 1);
}

#[tokio::test]
async fn re_materializing_the_same_run_is_idempotent() {
    let pool = test_pool().await;
    let run_id = stage_run(
        &pool,
        &[RawArcgisRow {
            field_kid: "K7401",
            field_name: "IDEMPOTENT FIELD",
            status: "Active",
            field_type: "Oil",
            formations: None,
            geometry: None,
        }],
        &[],
    )
    .await;

    let first = materialize_fields_from_run(&pool, run_id).await.unwrap();
    assert_eq!(first.created, 1);

    let second = materialize_fields_from_run(&pool, run_id).await.unwrap();
    assert_eq!(second.created, 0);
    assert_eq!(second.updated, 1);

    let count: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM ks_field WHERE official_field_name = 'IDEMPOTENT FIELD'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(count, 1);
}

#[tokio::test]
async fn materialize_refuses_a_non_completed_run() {
    let pool = test_pool().await;
    let source = registry::find_source_by_code(&pool, "KGS_FIELDS")
        .await
        .unwrap()
        .unwrap();
    let checksum = format!("checksum-{}", Uuid::new_v4());
    let run_id = ingestion_run::start_run(
        &pool,
        DownloadedPayload {
            source_id: source.id,
            source_url: "test-fixture",
            retrieved_at: chrono::Utc::now(),
            source_modified_at: None,
            payload_checksum: &checksum,
            schema_fingerprint: None,
            parser_version: "test-fixture",
        },
    )
    .await
    .unwrap();

    let err = materialize_fields_from_run(&pool, run_id)
        .await
        .unwrap_err();
    assert!(
        matches!(err, MaterializeError::RunNotCompleted(id, status) if id == run_id && status == "running")
    );
}
