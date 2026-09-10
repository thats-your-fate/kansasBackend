//! KS-09: materializer tests — lease dimension creation, source aliases,
//! and every well-to-lease linking tier in the task doc's precedence
//! order (exact identifier > exact API number > name match), plus the
//! oil/gas identifier non-collision guarantee and the unresolved-API
//! report. Stages raw rows directly, same discipline as the other
//! materialize test files in this repo.

use future_wells_kansas::materialize::kgs_leases::{
    find_lease, materialize_leases_from_run, unresolved_lease_apis_report, MaterializeError,
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

async fn any_ingestion_run_id(pool: &PgPool, source_code: &str) -> Uuid {
    let source = registry::find_source_by_code(pool, source_code)
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

#[allow(clippy::too_many_arguments)]
async fn insert_well(
    pool: &PgPool,
    api14: Option<&str>,
    oil_kid: Option<&str>,
    oil_dor_id: Option<&str>,
    gas_kid: Option<&str>,
    gas_dor_id: Option<&str>,
    lease_name: Option<&str>,
    county: Option<&str>,
) -> Uuid {
    let run_id = any_ingestion_run_id(pool, "KGS_WELLS_ARCGIS").await;
    sqlx::query_scalar(
        r#"
        INSERT INTO ks_well (
            api14, api_validation_status, well_identity_match_method, lifecycle_status,
            source_format, source_ingestion_run_id, oil_kid, oil_dor_id, gas_kid, gas_dor_id, lease_name, county
        )
        VALUES ($1, 'valid', 'api14', 'unknown', 'arcgis', $2, $3, $4, $5, $6, $7, $8)
        RETURNING internal_well_id
        "#,
    )
    .bind(api14)
    .bind(run_id)
    .bind(oil_kid)
    .bind(oil_dor_id)
    .bind(gas_kid)
    .bind(gas_dor_id)
    .bind(lease_name)
    .bind(county)
    .fetch_one(pool)
    .await
    .unwrap()
}

struct RawLease<'a> {
    lease_kid: &'a str,
    reported_lease_name: Option<&'a str>,
    dor_code: Option<&'a str>,
    api_number_raw: Option<&'a str>,
    county: Option<&'a str>,
    produces_raw: Option<&'a str>,
}

async fn stage_run(pool: &PgPool, leases: &[RawLease<'_>]) -> Uuid {
    let run_id = any_ingestion_run_id(pool, "KGS_LEASES").await;
    for lease in leases {
        sqlx::query(
            r#"
            INSERT INTO ks_lease_raw_snapshot (
                ingestion_run_id, lease_kid, reported_lease_name, dor_code, api_number_raw, county,
                produces_raw, raw_data, source_record_hash
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, '{}'::jsonb, $8)
            "#,
        )
        .bind(run_id)
        .bind(lease.lease_kid)
        .bind(lease.reported_lease_name)
        .bind(lease.dor_code)
        .bind(lease.api_number_raw)
        .bind(lease.county)
        .bind(lease.produces_raw)
        .bind(format!("hash-{}", Uuid::new_v4()))
        .execute(pool)
        .await
        .unwrap();
    }
    ingestion_run::complete_run(pool, run_id, leases.len() as i64, 0, None, None, json!({}))
        .await
        .unwrap();
    run_id
}

#[tokio::test]
async fn creates_a_lease_with_alias_rows_for_kid_and_dor_code() {
    let pool = test_pool().await;
    let run_id = stage_run(
        &pool,
        &[RawLease {
            lease_kid: "L9001",
            reported_lease_name: Some("Alias Test Lease"),
            dor_code: Some("D9001"),
            api_number_raw: None,
            county: Some("Barton"),
            produces_raw: Some("OIL"),
        }],
    )
    .await;

    let outcome = materialize_leases_from_run(&pool, run_id).await.unwrap();
    assert_eq!(outcome.leases_created, 1);

    let lease = find_lease(&pool, "L9001").await.unwrap().unwrap();
    assert_eq!(lease.product, "oil");
    assert_eq!(
        lease.reported_lease_name.as_deref(),
        Some("Alias Test Lease")
    );
    assert!(lease
        .ownership_disclaimer
        .contains("do not establish or imply"));

    let alias_types: Vec<String> = sqlx::query_scalar(
        "SELECT alias_type FROM ks_lease_source_alias WHERE lease_id = $1 ORDER BY alias_type",
    )
    .bind(lease.id)
    .fetch_all(&pool)
    .await
    .unwrap();
    assert_eq!(
        alias_types,
        vec!["dor_code".to_string(), "lease_kid".to_string()]
    );
}

#[tokio::test]
async fn exact_identifier_tier_scopes_oil_and_gas_so_they_never_collide() {
    let pool = test_pool().await;
    // Deliberately share the SAME numeric value as an oil lease's
    // lease_kid and an unrelated well's gas_kid — the oil-product join
    // must never match it via the gas column, and vice versa (task doc:
    // "oil and gas identifiers do not collide").
    let oil_well = insert_well(&pool, None, Some("SHARED123"), None, None, None, None, None).await;
    let gas_well = insert_well(&pool, None, None, None, Some("SHARED123"), None, None, None).await;

    let run_id = stage_run(
        &pool,
        &[RawLease {
            lease_kid: "SHARED123",
            reported_lease_name: Some("Collision Test Lease"),
            dor_code: None,
            api_number_raw: None,
            county: None,
            produces_raw: Some("OIL"),
        }],
    )
    .await;

    let outcome = materialize_leases_from_run(&pool, run_id).await.unwrap();
    assert_eq!(outcome.links_created, 1);

    let linked_well_ids: Vec<Uuid> = sqlx::query_scalar(
        "SELECT well_id FROM ks_well_lease_link l JOIN ks_lease ls ON ls.id = l.lease_id WHERE ls.lease_kid = 'SHARED123'",
    )
    .fetch_all(&pool)
    .await
    .unwrap();
    assert_eq!(linked_well_ids, vec![oil_well]);
    assert!(!linked_well_ids.contains(&gas_well));
}

#[tokio::test]
async fn exact_api_number_tier_links_multiple_wells_from_one_lease_row() {
    let pool = test_pool().await;
    let well_a = insert_well(
        &pool,
        Some("15009111110000"),
        None,
        None,
        None,
        None,
        None,
        None,
    )
    .await;
    let well_b = insert_well(
        &pool,
        Some("15009222220000"),
        None,
        None,
        None,
        None,
        None,
        None,
    )
    .await;

    let run_id = stage_run(
        &pool,
        &[RawLease {
            lease_kid: "L9101",
            reported_lease_name: Some("Multi API Lease"),
            dor_code: None,
            api_number_raw: Some("15-009-11111, 15-009-22222"),
            county: None,
            produces_raw: Some("OIL"),
        }],
    )
    .await;

    let outcome = materialize_leases_from_run(&pool, run_id).await.unwrap();
    assert_eq!(outcome.links_created, 2);
    assert_eq!(outcome.unresolved_api_count, 0);

    let lease = find_lease(&pool, "L9101").await.unwrap().unwrap();
    let linked: Vec<Uuid> = sqlx::query_scalar("SELECT well_id FROM ks_well_lease_link WHERE lease_id = $1 AND link_method = 'exact_api_number'").bind(lease.id).fetch_all(&pool).await.unwrap();
    assert_eq!(linked.len(), 2);
    assert!(linked.contains(&well_a));
    assert!(linked.contains(&well_b));
}

#[tokio::test]
async fn unresolved_api_numbers_are_reported() {
    let pool = test_pool().await;
    let run_id = stage_run(
        &pool,
        &[RawLease {
            lease_kid: "L9201",
            reported_lease_name: Some("Unresolved API Lease"),
            dor_code: None,
            api_number_raw: Some("15-009-88888"),
            county: None,
            produces_raw: Some("GAS"),
        }],
    )
    .await;

    let outcome = materialize_leases_from_run(&pool, run_id).await.unwrap();
    assert_eq!(outcome.unresolved_api_count, 1);
    assert_eq!(outcome.links_created, 0);

    let report = unresolved_lease_apis_report(&pool, 100).await.unwrap();
    assert!(report
        .iter()
        .any(|r| r.raw_data.get("lease_kid").and_then(|v| v.as_str()) == Some("L9201")));
}

#[tokio::test]
async fn name_match_tier_only_applies_when_no_exact_tier_found_anything() {
    let pool = test_pool().await;
    insert_well(
        &pool,
        None,
        None,
        None,
        None,
        None,
        Some("Name Match Lease"),
        Some("Rooks"),
    )
    .await;

    let run_id = stage_run(
        &pool,
        &[RawLease {
            lease_kid: "L9301",
            reported_lease_name: Some("Name Match Lease"),
            dor_code: None,
            api_number_raw: None,
            county: Some("Rooks"),
            produces_raw: Some("OIL"),
        }],
    )
    .await;

    let outcome = materialize_leases_from_run(&pool, run_id).await.unwrap();
    assert_eq!(outcome.links_created, 1);

    let lease = find_lease(&pool, "L9301").await.unwrap().unwrap();
    let method: String =
        sqlx::query_scalar("SELECT link_method FROM ks_well_lease_link WHERE lease_id = $1")
            .bind(lease.id)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(method, "name_match");
}

#[tokio::test]
async fn exact_tiers_take_precedence_over_name_match() {
    let pool = test_pool().await;
    // Both an exact-identifier well AND a same-named well exist; only
    // the exact-identifier link should be created (task doc: "exact
    // ID-based links take precedence over name matching").
    let exact_well = insert_well(&pool, None, Some("L9401"), None, None, None, None, None).await;
    insert_well(
        &pool,
        None,
        None,
        None,
        None,
        None,
        Some("Precedence Lease"),
        None,
    )
    .await;

    let run_id = stage_run(
        &pool,
        &[RawLease {
            lease_kid: "L9401",
            reported_lease_name: Some("Precedence Lease"),
            dor_code: None,
            api_number_raw: None,
            county: None,
            produces_raw: Some("OIL"),
        }],
    )
    .await;

    let outcome = materialize_leases_from_run(&pool, run_id).await.unwrap();
    assert_eq!(outcome.links_created, 1);

    let lease = find_lease(&pool, "L9401").await.unwrap().unwrap();
    let (well_id, method): (Uuid, String) =
        sqlx::query_as("SELECT well_id, link_method FROM ks_well_lease_link WHERE lease_id = $1")
            .bind(lease.id)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(well_id, exact_well);
    assert_eq!(method, "exact_identifier");
}

#[tokio::test]
async fn unknown_product_lease_is_created_but_never_linked_via_identifier_tier() {
    let pool = test_pool().await;
    insert_well(
        &pool,
        None,
        Some("L9501"),
        None,
        Some("L9501"),
        None,
        None,
        None,
    )
    .await;

    let run_id = stage_run(
        &pool,
        &[RawLease {
            lease_kid: "L9501",
            reported_lease_name: Some("Unknown Product Lease"),
            dor_code: None,
            api_number_raw: None,
            county: None,
            produces_raw: Some("NULL"),
        }],
    )
    .await;

    let outcome = materialize_leases_from_run(&pool, run_id).await.unwrap();
    assert_eq!(outcome.leases_created, 1);
    assert_eq!(outcome.links_created, 0);

    let lease = find_lease(&pool, "L9501").await.unwrap().unwrap();
    assert_eq!(lease.product, "unknown");
}

#[tokio::test]
async fn re_materializing_the_same_run_is_idempotent() {
    let pool = test_pool().await;
    let run_id = stage_run(
        &pool,
        &[RawLease {
            lease_kid: "L9601",
            reported_lease_name: Some("Idempotent Lease"),
            dor_code: None,
            api_number_raw: None,
            county: None,
            produces_raw: Some("OIL"),
        }],
    )
    .await;

    let first = materialize_leases_from_run(&pool, run_id).await.unwrap();
    assert_eq!(first.leases_created, 1);

    let second = materialize_leases_from_run(&pool, run_id).await.unwrap();
    assert_eq!(second.leases_created, 0);
    assert_eq!(second.leases_updated, 1);

    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM ks_lease WHERE lease_kid = 'L9601'")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(count, 1);
}

#[tokio::test]
async fn materialize_refuses_a_non_completed_run() {
    let pool = test_pool().await;
    let run_id = any_ingestion_run_id(&pool, "KGS_LEASES").await;

    let err = materialize_leases_from_run(&pool, run_id)
        .await
        .unwrap_err();
    assert!(
        matches!(err, MaterializeError::RunNotCompleted(id, status) if id == run_id && status == "running")
    );
}
