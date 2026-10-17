//! KS-12/KS-13: integration tests for the `state-export-v1` exporter that
//! lets Kansas plug into the shared Future Wells Co map + entity-detail
//! platform. Stages fixture rows directly into the canonical tables (same
//! discipline as the other materialize test files in this repo) rather
//! than going through a full snapshot/materialize pipeline, since this
//! module only reads canonical tables — it never touches raw snapshots.

use chrono::Utc;
use future_wells_kansas::event::{self, NewEvent};
use future_wells_kansas::privacy;
use future_wells_kansas::source::{ingestion_run, registry};
use future_wells_kansas::state_export::contract::{
    CountyV1, EventV1, ExportModeV1, OperatorV1, ProductionMonthlyV1, RegulatoryAreaV1,
    RegulatoryRecordV1, StateExportManifestV1, TombstoneV1, WellV1,
};
use future_wells_kansas::state_export::package::Compression;
use future_wells_kansas::state_export::{self, ExportOptions};
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

async fn seed_ingestion_run(pool: &PgPool) -> Uuid {
    let source = registry::find_source_by_code(pool, "KGS_WELLS_ARCGIS")
        .await
        .unwrap()
        .unwrap();
    let run_id = ingestion_run::start_run(
        pool,
        ingestion_run::DownloadedPayload {
            source_id: source.id,
            source_url: "test-fixture",
            retrieved_at: Utc::now(),
            source_modified_at: None,
            payload_checksum: &format!("checksum-{}", Uuid::new_v4()),
            schema_fingerprint: None,
            parser_version: "test-fixture",
        },
    )
    .await
    .unwrap();
    ingestion_run::complete_run(pool, run_id, 1, 0, None, None, json!({}))
        .await
        .unwrap();
    run_id
}

struct WellFixture {
    id: Uuid,
}

struct WellSpec<'a> {
    api14: &'a str,
    county: &'a str,
    lat: f64,
    lon: f64,
    horizontal: bool,
    tombstoned: bool,
}

async fn insert_well(pool: &PgPool, run_id: Uuid, spec: WellSpec<'_>) -> WellFixture {
    let id: Uuid = sqlx::query_scalar(
        r#"
        INSERT INTO ks_well (
            api_raw, api14, well_name, county, api_validation_status, well_identity_match_method,
            lifecycle_status, well_class_raw, status_raw, comments, total_depth, producing_formation,
            horizontal_or_directional, trajectory_available, display_geometry_4326, source_format,
            source_ingestion_run_id, is_tombstoned, tombstoned_at, field_name, lease_name
        )
        VALUES (
            $1, $1, 'Test Well 1-H', $2, 'valid', 'api14',
            'active', 'OIL', 'AC', 'this is a raw kgs comment nobody should ever export', 4500, 'Mississippian',
            $3, $3, ST_SetSRID(ST_MakePoint($4, $5), 4326), 'arcgis',
            $6, $7, CASE WHEN $7 THEN now() ELSE NULL END, 'Schoenchen', 'Pivonka A'
        )
        RETURNING internal_well_id
        "#,
    )
    .bind(spec.api14)
    .bind(spec.county)
    .bind(spec.horizontal)
    .bind(spec.lon)
    .bind(spec.lat)
    .bind(run_id)
    .bind(spec.tombstoned)
    .fetch_one(pool)
    .await
    .unwrap();
    WellFixture { id }
}

async fn insert_well_alias(pool: &PgPool, well_id: Uuid, alias_type: &str, alias_value: &str) {
    sqlx::query(
        "INSERT INTO ks_well_source_alias (well_id, alias_type, alias_value) VALUES ($1, $2, $3)",
    )
    .bind(well_id)
    .bind(alias_type)
    .bind(alias_value)
    .execute(pool)
    .await
    .unwrap();
}

async fn insert_operator(pool: &PgPool, run_id: Uuid, license: &str) -> Uuid {
    sqlx::query_scalar(
        r#"
        INSERT INTO ks_operator (kcc_license_number, reported_company_name, normalized_company_name, active_status, source_city, source_record_hash, source_ingestion_run_id)
        VALUES ($1, 'Test Operating LLC', 'test operating llc', 'active', 'Wichita', 'hash-1', $2)
        RETURNING id
        "#,
    )
    .bind(license)
    .bind(run_id)
    .fetch_one(pool)
    .await
    .unwrap()
}

async fn insert_field(pool: &PgPool, run_id: Uuid, name: &str) -> Uuid {
    sqlx::query_scalar(
        r#"
        INSERT INTO ks_field (official_field_name, normalized_search_name, active_status, source_ingestion_run_id, source_record_hash)
        VALUES ($1, $2, 'active', $3, 'hash-1')
        RETURNING id
        "#,
    )
    .bind(name)
    .bind(name.to_lowercase())
    .bind(run_id)
    .fetch_one(pool)
    .await
    .unwrap()
}

async fn insert_lease(pool: &PgPool, run_id: Uuid, county: &str) -> Uuid {
    sqlx::query_scalar(
        r#"
        INSERT INTO ks_lease (lease_kid, dor_code, product, reported_lease_name, county, source_ingestion_run_id, source_record_hash)
        VALUES ($1, 'DOR1', 'oil', 'Smith Lease', $2, $3, 'hash-1')
        RETURNING id
        "#,
    )
    .bind(format!("LK-{}", Uuid::new_v4()))
    .bind(county)
    .bind(run_id)
    .fetch_one(pool)
    .await
    .unwrap()
}

struct ProductionRowSpec<'a> {
    lease_kid: &'a str,
    product: &'a str,
    year: i32,
    month: i16,
    record_kind: &'a str,
    volume: f64,
}

async fn insert_production_row(pool: &PgPool, run_id: Uuid, spec: ProductionRowSpec<'_>) {
    sqlx::query(
        r#"
        INSERT INTO ks_lease_production_month (
            lease_kid, product, year, month, record_kind, production_volume, reported_lease_name,
            county, source_ingestion_run_id, source_record_hash
        )
        VALUES ($1, $2, $3, $4, $5, $6, 'Smith Lease', 'Barton', $7, $8)
        "#,
    )
    .bind(spec.lease_kid)
    .bind(spec.product)
    .bind(spec.year)
    .bind(spec.month)
    .bind(spec.record_kind)
    .bind(spec.volume)
    .bind(run_id)
    .bind(format!("hash-{}", Uuid::new_v4()))
    .execute(pool)
    .await
    .unwrap();
}

async fn insert_intent(
    pool: &PgPool,
    run_id: Uuid,
    matched_well_id: Option<Uuid>,
    match_method: &str,
) -> Uuid {
    let now = Utc::now();
    let id: Uuid = sqlx::query_scalar(
        r#"
        INSERT INTO ks_drilling_intent (
            source_filename, county, reported_lease_or_well_name, matched_well_id, match_method,
            occurred_at, first_observed_at, last_observed_at, source_ingestion_run_id, source_record_hash
        )
        VALUES ($1, 'Barton', 'Test Intent Well', $2, $3, $4, $4, $4, $5, 'hash-1')
        RETURNING id
        "#,
    )
    .bind(format!("intent-{}.html", Uuid::new_v4()))
    .bind(matched_well_id)
    .bind(match_method)
    .bind(now)
    .bind(run_id)
    .fetch_one(pool)
    .await
    .unwrap();

    event::record_event(
        pool,
        NewEvent {
            event_type: event::INTENT_POSTED,
            dedup_key: &id.to_string(),
            occurred_at: Some(now),
            published_at: None,
            observed_at: now,
            payload: json!({}),
            source_ingestion_run_id: run_id,
            related_well_id: (match_method != "unresolved")
                .then_some(matched_well_id)
                .flatten(),
            related_operator_id: None,
            related_field_id: None,
            related_lease_id: None,
            related_county: None,
            related_well_ids: &[],
        },
    )
    .await
    .unwrap();
    id
}

async fn insert_transfer(pool: &PgPool, run_id: Uuid, label: &str) -> Uuid {
    let now = Utc::now();
    let id: Uuid = sqlx::query_scalar(
        r#"
        INSERT INTO ks_operator_transfer (
            source_file, lease_or_well_label, county, effective_date, received_date,
            source_ingestion_run_id, source_record_hash, first_observed_at, last_observed_at
        )
        VALUES ($1, $2, 'Barton', CURRENT_DATE, CURRENT_DATE, $3, 'hash-1', $4, $4)
        RETURNING id
        "#,
    )
    .bind(format!("transfer-{}.html", Uuid::new_v4()))
    .bind(label)
    .bind(run_id)
    .bind(now)
    .fetch_one(pool)
    .await
    .unwrap();

    event::record_event(
        pool,
        NewEvent {
            event_type: event::OPERATOR_TRANSFER_APPROVED,
            dedup_key: &id.to_string(),
            occurred_at: Some(now),
            published_at: None,
            observed_at: now,
            payload: json!({}),
            source_ingestion_run_id: run_id,
            related_well_id: None,
            related_operator_id: None,
            related_field_id: None,
            related_lease_id: None,
            related_county: None,
            related_well_ids: &[],
        },
    )
    .await
    .unwrap();
    id
}

async fn event_id_for_dedup_key(pool: &PgPool, dedup_key: &str) -> Uuid {
    sqlx::query_scalar("SELECT id FROM ks_event WHERE dedup_key = $1")
        .bind(dedup_key)
        .fetch_one(pool)
        .await
        .unwrap()
}

async fn link_transfer_well(pool: &PgPool, transfer_id: Uuid, well_id: Uuid, match_method: &str) {
    sqlx::query("INSERT INTO ks_operator_transfer_well (transfer_id, well_id, match_method) VALUES ($1, $2, $3)")
        .bind(transfer_id)
        .bind(well_id)
        .bind(match_method)
        .execute(pool)
        .await
        .unwrap();
}

struct ExportedPackage {
    manifest: StateExportManifestV1,
    dir: std::path::PathBuf,
}

impl ExportedPackage {
    fn records<T: serde::de::DeserializeOwned>(&self, entity_type: &str) -> Vec<T> {
        let mut out = Vec::new();
        for file in self
            .manifest
            .files
            .iter()
            .filter(|f| f.entity_type == entity_type)
        {
            let contents = std::fs::read_to_string(self.dir.join(&file.filename)).unwrap();
            for line in contents.lines() {
                out.push(serde_json::from_str(line).unwrap());
            }
        }
        out
    }
}

async fn run_export(pool: &PgPool) -> ExportedPackage {
    let output_dir = std::env::temp_dir().join(format!("ks-state-export-test-{}", Uuid::new_v4()));
    let options = ExportOptions {
        consumer: "future-wells-co".to_owned(),
        output_dir,
        compression: Compression::None,
        ..Default::default()
    };
    let summary = state_export::export_state_v1(pool, ExportModeV1::Full, options)
        .await
        .unwrap();
    let manifest = state_export::verify_package(&summary.output_path).unwrap();
    ExportedPackage {
        manifest,
        dir: summary.output_path,
    }
}

#[tokio::test]
async fn preflight_passes_against_a_migrated_database() {
    let pool = test_pool().await;
    state_export::preflight(&pool)
        .await
        .expect("preflight must pass against a fully migrated database");
}

#[tokio::test]
async fn incremental_mode_is_explicitly_rejected_not_silently_partial() {
    let pool = test_pool().await;
    let output_dir = std::env::temp_dir().join(format!("ks-state-export-test-{}", Uuid::new_v4()));
    let options = ExportOptions {
        output_dir,
        ..Default::default()
    };
    let error = state_export::export_state_v1(&pool, ExportModeV1::Incremental, options)
        .await
        .unwrap_err();
    assert!(format!("{error:#}").contains("incremental"));
}

#[tokio::test]
async fn full_export_includes_every_entity_file_even_when_empty() {
    let pool = test_pool().await;
    let package = run_export(&pool).await;
    let entity_types: std::collections::HashSet<&str> = package
        .manifest
        .files
        .iter()
        .map(|f| f.entity_type.as_str())
        .collect();
    for expected in [
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
    ] {
        assert!(
            entity_types.contains(expected),
            "missing mandatory entity descriptor: {expected}"
        );
    }
}

#[tokio::test]
async fn well_export_carries_geometry_identity_and_raw_status_but_never_raw_comments() {
    let pool = test_pool().await;
    let run_id = seed_ingestion_run(&pool).await;
    let well = insert_well(
        &pool,
        run_id,
        WellSpec {
            api14: "15-009-12345",
            county: "Barton",
            lat: 38.1234567,
            lon: -98.7654321,
            horizontal: true,
            tombstoned: false,
        },
    )
    .await;
    insert_well_alias(&pool, well.id, "kgs_kid", "1002345").await;

    let package = run_export(&pool).await;
    let wells: Vec<WellV1> = package.records("wells");
    let record = wells
        .iter()
        .find(|w| w.source.source_key == format!("ks:well:{}", well.id))
        .expect("well must be exported");
    assert_eq!(
        record.normalized_api_number.as_deref(),
        Some("15-009-12345")
    );
    assert_eq!(
        record.county_source_key.as_deref(),
        Some("ks:county:barton")
    );
    assert_eq!(record.surface_latitude.as_deref(), Some("38.1234567"));
    assert_eq!(record.surface_longitude.as_deref(), Some("-98.7654321"));

    let payload_text = record.source.source_payload.to_string();
    assert!(
        !payload_text.to_lowercase().contains("raw kgs comment"),
        "raw KGS comments must never be exported, even into source_payload: {payload_text}"
    );
    assert_eq!(record.source.source_payload["status_raw"], json!("AC"));
    assert_eq!(record.source.source_payload["field_name"], json!("Schoenchen"));
    assert_eq!(record.source.source_payload["lease_name"], json!("Pivonka A"));

    let wellbores: Vec<future_wells_kansas::state_export::contract::WellboreV1> =
        package.records("wellbores");
    let bore = wellbores
        .iter()
        .find(|w| w.well_source_key.as_deref() == Some(format!("ks:well:{}", well.id).as_str()))
        .unwrap();
    assert_eq!(bore.trajectory_kind, "HORIZONTAL");
    assert_eq!(bore.classification_method.as_deref(), Some("SOURCE_MAPPING"));
    assert_eq!(
        bore.is_lateral,
        Some(false),
        "point-only at launch: no lateral geometry claim even for a horizontal well"
    );

    let geometries: Vec<serde_json::Value> = package.records("wellbore_geometries");
    assert!(
        geometries.is_empty(),
        "no lateral-line geometry should be exported at launch"
    );

    let identifiers: Vec<future_wells_kansas::state_export::contract::WellIdentifierV1> =
        package.records("well_identifiers");
    assert!(identifiers
        .iter()
        .any(|i| i.identifier_type == "api14" && i.is_primary));
    assert!(identifiers
        .iter()
        .any(|i| i.identifier_type == "kgs_kid" && !i.is_primary));

    privacy::assert_no_forbidden_fields(&wells)
        .expect("well export must never carry a forbidden contact/address field");
}

#[tokio::test]
async fn tombstoned_well_is_flagged_and_also_emitted_as_a_tombstone() {
    let pool = test_pool().await;
    let run_id = seed_ingestion_run(&pool).await;
    let well = insert_well(
        &pool,
        run_id,
        WellSpec {
            api14: "15-009-99999",
            county: "Barton",
            lat: 38.0,
            lon: -98.0,
            horizontal: false,
            tombstoned: true,
        },
    )
    .await;

    let package = run_export(&pool).await;
    let wells: Vec<WellV1> = package.records("wells");
    let record = wells
        .iter()
        .find(|w| w.source.source_key == format!("ks:well:{}", well.id))
        .expect("tombstoned well is still exported");
    assert_eq!(record.source.source_payload["is_tombstoned"], json!(true));

    let tombstones: Vec<TombstoneV1> = package.records("tombstones");
    let tombstone = tombstones
        .iter()
        .find(|t| t.entity_source_key == format!("ks:well:{}", well.id))
        .expect("tombstone must be emitted");
    assert_eq!(tombstone.entity_type, "wells");
}

#[tokio::test]
async fn matched_and_unmatched_transfers_produce_correctly_resolved_regulatory_records() {
    let pool = test_pool().await;
    let run_id = seed_ingestion_run(&pool).await;
    let well_a = insert_well(
        &pool,
        run_id,
        WellSpec {
            api14: "15-009-11111",
            county: "Barton",
            lat: 38.0,
            lon: -98.0,
            horizontal: false,
            tombstoned: false,
        },
    )
    .await;
    let well_b = insert_well(
        &pool,
        run_id,
        WellSpec {
            api14: "15-009-22222",
            county: "Barton",
            lat: 38.1,
            lon: -98.1,
            horizontal: false,
            tombstoned: false,
        },
    )
    .await;

    let matched_transfer = insert_transfer(&pool, run_id, "Matched Transfer Lease").await;
    link_transfer_well(&pool, matched_transfer, well_a.id, "exact_operator_license").await;
    link_transfer_well(&pool, matched_transfer, well_b.id, "exact_plss").await;

    let fuzzy_only_transfer = insert_transfer(&pool, run_id, "Fuzzy Only Transfer Lease").await;
    link_transfer_well(&pool, fuzzy_only_transfer, well_a.id, "fuzzy_candidate").await;

    let unmatched_transfer = insert_transfer(&pool, run_id, "Unmatched Transfer Lease").await;

    let package = run_export(&pool).await;
    let records: Vec<RegulatoryRecordV1> = package.records("regulatory_records");
    let transfer_records: Vec<&RegulatoryRecordV1> = records
        .iter()
        .filter(|r| r.record_type == "operator_transfer")
        .collect();

    let matched_links: Vec<&&RegulatoryRecordV1> = transfer_records
        .iter()
        .filter(|r| r.source.source_key.contains(&matched_transfer.to_string()))
        .collect();
    assert_eq!(
        matched_links.len(),
        2,
        "one regulatory_record per confirmed well link"
    );
    assert!(matched_links
        .iter()
        .all(|r| r.relationship_resolution_status == "resolved"));
    assert!(matched_links
        .iter()
        .all(|r| r.status.as_deref() == Some("approved")));

    let fuzzy_only: Vec<&&RegulatoryRecordV1> = transfer_records
        .iter()
        .filter(|r| {
            r.source
                .source_key
                .contains(&fuzzy_only_transfer.to_string())
        })
        .collect();
    assert_eq!(
        fuzzy_only.len(),
        1,
        "a fuzzy-only candidate must not count as a confirmed match"
    );
    assert_eq!(fuzzy_only[0].well_source_key, None);
    assert_eq!(fuzzy_only[0].relationship_resolution_status, "unresolved");

    let unmatched: Vec<&&RegulatoryRecordV1> = transfer_records
        .iter()
        .filter(|r| {
            r.source
                .source_key
                .contains(&unmatched_transfer.to_string())
        })
        .collect();
    assert_eq!(unmatched.len(), 1);
    assert_eq!(unmatched[0].well_source_key, None);
    assert_eq!(unmatched[0].relationship_resolution_status, "unresolved");

    // Events: the resolved intent's event carries a well link; nothing about
    // the multi-well transfer forces a single well onto its event.
    let intent = insert_intent(&pool, run_id, Some(well_a.id), "exact_permit_or_api").await;
    let unresolved_intent = insert_intent(&pool, run_id, None, "unresolved").await;
    let intent_event_id = event_id_for_dedup_key(&pool, &intent.to_string()).await;
    let unresolved_event_id = event_id_for_dedup_key(&pool, &unresolved_intent.to_string()).await;
    let package = run_export(&pool).await;
    let events: Vec<EventV1> = package.records("events");
    let intent_event = events
        .iter()
        .find(|e| e.source.source_key == format!("ks:event:{intent_event_id}"))
        .unwrap();
    assert_eq!(
        intent_event.well_source_key.as_deref(),
        Some(format!("ks:well:{}", well_a.id).as_str())
    );
    assert_eq!(intent_event.relationship_resolution_status, "resolved");
    let unresolved_event = events
        .iter()
        .find(|e| e.source.source_key == format!("ks:event:{unresolved_event_id}"))
        .unwrap();
    assert_eq!(unresolved_event.well_source_key, None);
    assert_eq!(
        unresolved_event.relationship_resolution_status,
        "unresolved"
    );
}

#[tokio::test]
async fn only_true_monthly_production_rows_are_exported_never_yearly_or_cumulative_sentinels() {
    let pool = test_pool().await;
    let run_id = seed_ingestion_run(&pool).await;
    insert_production_row(
        &pool,
        run_id,
        ProductionRowSpec {
            lease_kid: "LK-EXPORT-1",
            product: "oil",
            year: 2020,
            month: 3,
            record_kind: "monthly",
            volume: 100.0,
        },
    )
    .await;
    insert_production_row(
        &pool,
        run_id,
        ProductionRowSpec {
            lease_kid: "LK-EXPORT-1",
            product: "oil",
            year: 2020,
            month: 0,
            record_kind: "yearly_total",
            volume: 1200.0,
        },
    )
    .await;
    insert_production_row(
        &pool,
        run_id,
        ProductionRowSpec {
            lease_kid: "LK-EXPORT-1",
            product: "oil",
            year: 2020,
            month: -1,
            record_kind: "starting_cumulative",
            volume: 5000.0,
        },
    )
    .await;

    let package = run_export(&pool).await;
    let rows: Vec<ProductionMonthlyV1> = package.records("production_monthly");
    let exported: Vec<&ProductionMonthlyV1> = rows
        .iter()
        .filter(|r| r.source.source_payload["lease_kid"] == json!("LK-EXPORT-1"))
        .collect();
    assert_eq!(
        exported.len(),
        1,
        "only the true monthly row may be exported, never the sentinel rows"
    );
    assert_eq!(exported[0].oil_volume.as_deref(), Some("100"));
    assert_eq!(
        exported[0].well_source_key, None,
        "lease-level production is never allocated to a single well"
    );
    assert_eq!(exported[0].relationship_resolution_status, "unresolved");
    assert_eq!(exported[0].source_allocation_type.as_deref(), Some("lease"));
}

#[tokio::test]
async fn fields_and_leases_both_export_as_regulatory_areas_with_distinct_area_types() {
    let pool = test_pool().await;
    let run_id = seed_ingestion_run(&pool).await;
    let field_id = insert_field(&pool, run_id, "Test Export Field").await;
    let lease_id = insert_lease(&pool, run_id, "Barton").await;

    let package = run_export(&pool).await;
    let areas: Vec<RegulatoryAreaV1> = package.records("regulatory_areas");
    let field_record = areas
        .iter()
        .find(|a| a.source.source_key == format!("ks:field:{field_id}"))
        .expect("field must export as a regulatory_area");
    assert_eq!(field_record.area_type, "field");
    let lease_record = areas
        .iter()
        .find(|a| a.source.source_key == format!("ks:lease:{lease_id}"))
        .expect("lease must export as a regulatory_area");
    assert_eq!(lease_record.area_type, "lease");
    assert_eq!(
        lease_record.county_source_key.as_deref(),
        Some("ks:county:barton")
    );
}

#[tokio::test]
async fn operator_export_never_carries_a_forbidden_contact_field() {
    let pool = test_pool().await;
    let run_id = seed_ingestion_run(&pool).await;
    insert_operator(&pool, run_id, "OP-EXPORT-1").await;

    let package = run_export(&pool).await;
    let operators: Vec<OperatorV1> = package.records("operators");
    assert!(operators
        .iter()
        .any(|o| o.regulator_operator_number.as_deref() == Some("OP-EXPORT-1")));
    privacy::assert_no_forbidden_fields(&operators)
        .expect("operator export must never carry a forbidden contact/address field");
}

#[tokio::test]
async fn counties_are_derived_from_distinct_observed_names_across_source_tables() {
    let pool = test_pool().await;
    let run_id = seed_ingestion_run(&pool).await;
    insert_well(
        &pool,
        run_id,
        WellSpec {
            api14: "15-009-33333",
            county: "Reno",
            lat: 38.0,
            lon: -98.0,
            horizontal: false,
            tombstoned: false,
        },
    )
    .await;

    let package = run_export(&pool).await;
    let counties: Vec<CountyV1> = package.records("counties");
    assert!(counties
        .iter()
        .any(|c| c.name == "Reno" && c.source.source_key == "ks:county:reno"));
}

#[tokio::test]
async fn acknowledge_and_status_round_trip() {
    let pool = test_pool().await;
    // A distinct consumer code, isolated from every other test in this file
    // (and every other test in this shared-database suite) that also calls
    // `status("future-wells-co")` concurrently.
    let consumer = format!("test-consumer-{}", Uuid::new_v4());
    let output_dir = std::env::temp_dir().join(format!("ks-state-export-test-{}", Uuid::new_v4()));
    let options = ExportOptions {
        consumer: consumer.clone(),
        output_dir,
        compression: Compression::None,
        ..Default::default()
    };
    let summary = state_export::export_state_v1(&pool, ExportModeV1::Full, options)
        .await
        .unwrap();

    let status_before = state_export::status(&pool, &consumer).await.unwrap();
    let line_before = status_before
        .lines()
        .find(|line| line.contains(&summary.export_id))
        .expect("export must appear in status");
    assert!(
        line_before.contains("acknowledged_at=None"),
        "unacknowledged export must show acknowledged_at=None: {line_before}"
    );

    state_export::acknowledge_export(&pool, &consumer, &summary.export_id)
        .await
        .unwrap();
    let error = state_export::acknowledge_export(&pool, &consumer, &summary.export_id)
        .await
        .unwrap_err();
    assert!(
        format!("{error:#}").contains("no unacknowledged export"),
        "acknowledging twice must fail, not silently succeed"
    );

    let status_after = state_export::status(&pool, &consumer).await.unwrap();
    let line_after = status_after
        .lines()
        .find(|line| line.contains(&summary.export_id))
        .expect("export must still appear in status");
    assert!(
        !line_after.contains("acknowledged_at=None"),
        "acknowledged export must no longer show acknowledged_at=None: {line_after}"
    );
}
