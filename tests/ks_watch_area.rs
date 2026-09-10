//! KS-14: integration tests for `watch::find_matching_watch_areas` — the
//! pure matching predicates themselves are unit-tested directly in
//! `src/watch.rs`; these tests exercise the real database-backed context
//! builder (`event_watch_context`) against real `ks_event`/
//! `ks_event_well_link`/`ks_well` rows.

use future_wells_kansas::event::{self, NewEvent};
use future_wells_kansas::source::ingestion_run::{self, DownloadedPayload};
use future_wells_kansas::source::registry;
use future_wells_kansas::watch::{self, WatchArea, WatchType};
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

async fn seed_run(pool: &PgPool) -> Uuid {
    let source = registry::find_source_by_code(pool, "KGS_WELLS_ARCGIS")
        .await
        .unwrap()
        .unwrap();
    ingestion_run::start_run(
        pool,
        DownloadedPayload {
            source_id: source.id,
            source_url: "test-fixture",
            retrieved_at: chrono::Utc::now(),
            source_modified_at: None,
            payload_checksum: &format!("checksum-{}", Uuid::new_v4()),
            schema_fingerprint: None,
            parser_version: "test-fixture",
        },
    )
    .await
    .unwrap()
}

async fn insert_well(pool: &PgPool, run_id: Uuid, county: &str, lat: f64, lon: f64) -> Uuid {
    sqlx::query_scalar(
        r#"
        INSERT INTO ks_well (api_validation_status, well_identity_match_method, lifecycle_status, county, source_format, source_ingestion_run_id, display_geometry_4326)
        VALUES ('missing', 'kid_fallback', 'active', $1, 'arcgis', $2, ST_SetSRID(ST_MakePoint($4, $3), 4326))
        RETURNING internal_well_id
        "#,
    )
    .bind(county)
    .bind(run_id)
    .bind(lat)
    .bind(lon)
    .fetch_one(pool)
    .await
    .unwrap()
}

async fn insert_watch_area(pool: &PgPool, area: &WatchArea) {
    sqlx::query(
        r#"
        INSERT INTO ks_watch_area (id, watch_type, county, operator_id, field_id, well_id, center_latitude, center_longitude, radius_miles)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
        "#,
    )
    .bind(area.id)
    .bind(area.watch_type)
    .bind(&area.county)
    .bind(area.operator_id)
    .bind(area.field_id)
    .bind(area.well_id)
    .bind(area.center_latitude)
    .bind(area.center_longitude)
    .bind(area.radius_miles)
    .execute(pool)
    .await
    .unwrap();
}

fn area(watch_type: WatchType) -> WatchArea {
    WatchArea {
        id: Uuid::new_v4(),
        owner_reference: None,
        watch_type,
        county: None,
        operator_id: None,
        field_id: None,
        well_id: None,
        center_latitude: None,
        center_longitude: None,
        radius_miles: None,
    }
}

#[tokio::test]
async fn county_watch_area_matches_a_real_well_status_change_event() {
    let pool = test_pool().await;
    let run_id = seed_run(&pool).await;
    let well_id = insert_well(&pool, run_id, "Barton", 38.0, -98.0).await;

    let event_id: Uuid = sqlx::query_scalar(
        "INSERT INTO ks_event (event_type, dedup_key, observed_at, payload, source_ingestion_run_id, related_well_id, related_county) VALUES ($1, $2, now(), '{}'::jsonb, $3, $4, $5) RETURNING id",
    )
    .bind(event::WELL_STATUS_CHANGED)
    .bind(format!("watch-test-{}", Uuid::new_v4()))
    .bind(run_id)
    .bind(well_id)
    .bind("Barton")
    .fetch_one(&pool)
    .await
    .unwrap();

    let mut matching_area = area(WatchType::County);
    matching_area.county = Some("barton".to_string()); // case-insensitive on purpose
    insert_watch_area(&pool, &matching_area).await;
    let mut other_area = area(WatchType::County);
    other_area.county = Some("Reno".to_string());
    insert_watch_area(&pool, &other_area).await;

    let matches = watch::find_matching_watch_areas(&pool, event_id)
        .await
        .unwrap();
    assert!(matches.iter().any(|a| a.id == matching_area.id));
    assert!(!matches.iter().any(|a| a.id == other_area.id));
}

#[tokio::test]
async fn well_watch_area_matches_a_multi_well_transfer_event_via_the_link_table() {
    let pool = test_pool().await;
    let run_id = seed_run(&pool).await;
    let well_a = insert_well(&pool, run_id, "Barton", 38.0, -98.0).await;
    let well_b = insert_well(&pool, run_id, "Barton", 38.1, -98.1).await;

    let event_id = event::record_event(
        &pool,
        NewEvent {
            event_type: event::OPERATOR_TRANSFER_APPROVED,
            dedup_key: &format!("watch-transfer-{}", Uuid::new_v4()),
            occurred_at: None,
            published_at: None,
            observed_at: chrono::Utc::now(),
            payload: json!({}),
            source_ingestion_run_id: run_id,
            related_well_id: None,
            related_operator_id: None,
            related_field_id: None,
            related_lease_id: None,
            related_county: None,
            related_well_ids: &[well_a, well_b],
        },
    )
    .await
    .unwrap();
    assert!(
        event_id,
        "record_event must report a real insert for a fresh dedup_key"
    );

    let event_id: Uuid = sqlx::query_scalar("SELECT id FROM ks_event WHERE event_type = $1 AND source_ingestion_run_id = $2 ORDER BY created_at DESC LIMIT 1")
        .bind(event::OPERATOR_TRANSFER_APPROVED)
        .bind(run_id)
        .fetch_one(&pool)
        .await
        .unwrap();

    let mut watch_on_b = area(WatchType::Well);
    watch_on_b.well_id = Some(well_b);
    insert_watch_area(&pool, &watch_on_b).await;

    let matches = watch::find_matching_watch_areas(&pool, event_id)
        .await
        .unwrap();
    assert!(matches.iter().any(|a| a.id == watch_on_b.id), "a well watch must match via ks_event_well_link, not just the single related_well_id column");
}

#[tokio::test]
async fn radius_watch_area_matches_using_the_wells_real_geometry() {
    let pool = test_pool().await;
    let run_id = seed_run(&pool).await;
    // Great Bend, KS.
    let nearby_well = insert_well(&pool, run_id, "Barton", 38.3646, -98.7648).await;

    let event_id: Uuid = sqlx::query_scalar(
        "INSERT INTO ks_event (event_type, dedup_key, observed_at, payload, source_ingestion_run_id, related_well_id) VALUES ($1, $2, now(), '{}'::jsonb, $3, $4) RETURNING id",
    )
    .bind(event::SPUD_RECORDED)
    .bind(format!("watch-radius-{}", Uuid::new_v4()))
    .bind(run_id)
    .bind(nearby_well)
    .fetch_one(&pool)
    .await
    .unwrap();

    let mut close_watch = area(WatchType::Radius);
    close_watch.center_latitude = Some(38.3646);
    close_watch.center_longitude = Some(-98.7648);
    close_watch.radius_miles = Some(5.0);
    insert_watch_area(&pool, &close_watch).await;

    let mut far_watch = area(WatchType::Radius);
    far_watch.center_latitude = Some(37.6872); // Wichita
    far_watch.center_longitude = Some(-97.3301);
    far_watch.radius_miles = Some(5.0);
    insert_watch_area(&pool, &far_watch).await;

    let matches = watch::find_matching_watch_areas(&pool, event_id)
        .await
        .unwrap();
    assert!(matches.iter().any(|a| a.id == close_watch.id));
    assert!(!matches.iter().any(|a| a.id == far_watch.id));
}

#[tokio::test]
async fn inactive_watch_areas_are_never_returned() {
    let pool = test_pool().await;
    let run_id = seed_run(&pool).await;
    let well_id = insert_well(&pool, run_id, "Barton", 38.0, -98.0).await;
    let event_id: Uuid = sqlx::query_scalar(
        "INSERT INTO ks_event (event_type, dedup_key, observed_at, payload, source_ingestion_run_id, related_well_id, related_county) VALUES ($1, $2, now(), '{}'::jsonb, $3, $4, $5) RETURNING id",
    )
    .bind(event::WELL_STATUS_CHANGED)
    .bind(format!("watch-inactive-{}", Uuid::new_v4()))
    .bind(run_id)
    .bind(well_id)
    .bind("Barton")
    .fetch_one(&pool)
    .await
    .unwrap();

    let mut inactive_area = area(WatchType::County);
    inactive_area.county = Some("Barton".to_string());
    insert_watch_area(&pool, &inactive_area).await;
    sqlx::query("UPDATE ks_watch_area SET is_active = false WHERE id = $1")
        .bind(inactive_area.id)
        .execute(&pool)
        .await
        .unwrap();

    let matches = watch::find_matching_watch_areas(&pool, event_id)
        .await
        .unwrap();
    assert!(!matches.iter().any(|a| a.id == inactive_area.id));
}
