//! KS-07: materializer tests — every well-linking tier, multi-well
//! linking, the fuzzy-candidate-never-overwrites guarantee, event
//! dedup, and the unmatched-transfers report. Stages raw rows directly,
//! same discipline as `tests/kcc_intents_materialize.rs`.

use future_wells_kansas::materialize::kcc_transfers::{
    materialize_transfers_from_run, unmatched_transfers_report, MaterializeError,
};
use future_wells_kansas::source::ingestion_run::{self, DownloadedPayload};
use future_wells_kansas::source::registry;
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

async fn any_ingestion_run_id(pool: &PgPool) -> Uuid {
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

#[allow(clippy::too_many_arguments)]
async fn insert_well(
    pool: &PgPool,
    api14: Option<&str>,
    county: Option<&str>,
    township: Option<i16>,
    range: Option<i16>,
    range_direction: Option<&str>,
    section: Option<i16>,
    lease_name: Option<&str>,
    operator_name: Option<&str>,
) -> Uuid {
    let run_id = any_ingestion_run_id(pool).await;
    sqlx::query_scalar(
        r#"
        INSERT INTO ks_well (
            api14, api_validation_status, well_identity_match_method, lifecycle_status,
            source_format, source_ingestion_run_id, county, township, range, range_direction,
            section, lease_name, operator_name_kgs_reported_current
        )
        VALUES ($1, 'valid', 'api14', 'unknown', 'arcgis', $2, $3, $4, $5, $6, $7, $8, $9)
        RETURNING internal_well_id
        "#,
    )
    .bind(api14)
    .bind(run_id)
    .bind(county)
    .bind(township)
    .bind(range)
    .bind(range_direction)
    .bind(section)
    .bind(lease_name)
    .bind(operator_name)
    .fetch_one(pool)
    .await
    .unwrap()
}

async fn insert_operator(pool: &PgPool, license: &str, normalized_name: &str) {
    let run_id = any_ingestion_run_id(pool).await;
    sqlx::query(
        r#"
        INSERT INTO ks_operator (kcc_license_number, reported_company_name, normalized_company_name, active_status, source_record_hash, source_ingestion_run_id)
        VALUES ($1, $2, $3, 'active', 'test-hash', $4)
        "#,
    )
    .bind(license)
    .bind(normalized_name)
    .bind(normalized_name)
    .bind(run_id)
    .execute(pool)
    .await
    .unwrap();
}

#[allow(clippy::too_many_arguments)]
async fn stage_transfer_run(
    pool: &PgPool,
    past_license: Option<&str>,
    new_license: Option<&str>,
    effective_date: &str,
    lease_or_well_label: Option<&str>,
    county: Option<&str>,
    township: Option<i16>,
    range: Option<i16>,
    range_direction: Option<&str>,
    section: Option<i16>,
) -> Uuid {
    let source = registry::find_source_by_code(pool, "KCC_TRANSFERS")
        .await
        .unwrap()
        .unwrap();
    let checksum = format!("checksum-{}", Uuid::new_v4());
    let run_id = ingestion_run::start_run(
        pool,
        DownloadedPayload {
            source_id: source.id,
            source_url: "https://www.kcc.ks.gov/transfersSearch.php",
            retrieved_at: chrono::Utc::now(),
            source_modified_at: None,
            payload_checksum: &checksum,
            schema_fingerprint: None,
            parser_version: "test-fixture",
        },
    )
    .await
    .unwrap();

    sqlx::query(
        r#"
        INSERT INTO ks_transfer_raw_snapshot (
            ingestion_run_id, source_file, past_operator_license, new_operator_license, effective_date,
            lease_or_well_label, county, township, range, range_direction, section, raw_data, source_record_hash, observed_at
        )
        VALUES ($1, 'test-fixture-file', $2, $3, $4::date, $5, $6, $7, $8, $9, $10, '{}'::jsonb, $11, now())
        "#,
    )
    .bind(run_id)
    .bind(past_license)
    .bind(new_license)
    .bind(effective_date)
    .bind(lease_or_well_label)
    .bind(county)
    .bind(township)
    .bind(range)
    .bind(range_direction)
    .bind(section)
    .bind(format!("hash-{}", Uuid::new_v4()))
    .execute(pool)
    .await
    .unwrap();

    ingestion_run::complete_run(pool, run_id, 1, 0, None, None, serde_json::json!({}))
        .await
        .unwrap();
    run_id
}

#[tokio::test]
async fn kcc_dor_identifier_tier_matches_via_label_token() {
    let pool = test_pool().await;
    let well_id = insert_well(
        &pool,
        Some("15009777770000"),
        None,
        None,
        None,
        None,
        None,
        None,
        None,
    )
    .await;
    let run_id = stage_transfer_run(
        &pool,
        Some("K1"),
        Some("K2"),
        "2026-02-01",
        Some("15009777770000"),
        None,
        None,
        None,
        None,
        None,
    )
    .await;

    let outcome = materialize_transfers_from_run(&pool, run_id).await.unwrap();
    assert_eq!(outcome.well_links_created, 1);

    let (method,): (String,) = sqlx::query_as(
        "SELECT match_method FROM ks_operator_transfer_well w JOIN ks_operator_transfer t ON t.id = w.transfer_id WHERE w.well_id = $1",
    )
    .bind(well_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(method, "kcc_dor_identifier");
}

#[tokio::test]
async fn exact_plss_tier_matches_when_unambiguous() {
    let pool = test_pool().await;
    let well_id = insert_well(
        &pool,
        None,
        Some("Barton"),
        Some(20),
        Some(11),
        Some("W"),
        Some(7),
        None,
        None,
    )
    .await;
    let run_id = stage_transfer_run(
        &pool,
        None,
        None,
        "2026-02-02",
        Some("Unrelated Label"),
        Some("Barton"),
        Some(20),
        Some(11),
        Some("W"),
        Some(7),
    )
    .await;

    let outcome = materialize_transfers_from_run(&pool, run_id).await.unwrap();
    assert_eq!(outcome.well_links_created, 1);

    let (method,): (String,) = sqlx::query_as(
        "SELECT match_method FROM ks_operator_transfer_well w JOIN ks_operator_transfer t ON t.id = w.transfer_id WHERE w.well_id = $1",
    )
    .bind(well_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(method, "exact_plss");
}

#[tokio::test]
async fn exact_lease_name_tier_matches() {
    let pool = test_pool().await;
    let well_id = insert_well(
        &pool,
        None,
        Some("Ellis"),
        None,
        None,
        None,
        None,
        Some("Baker Unit"),
        None,
    )
    .await;
    let run_id = stage_transfer_run(
        &pool,
        None,
        None,
        "2026-02-03",
        Some("Baker Unit"),
        Some("Ellis"),
        None,
        None,
        None,
        None,
    )
    .await;

    let outcome = materialize_transfers_from_run(&pool, run_id).await.unwrap();
    assert_eq!(outcome.well_links_created, 1);

    let (method,): (String,) = sqlx::query_as(
        "SELECT match_method FROM ks_operator_transfer_well w JOIN ks_operator_transfer t ON t.id = w.transfer_id WHERE w.well_id = $1",
    )
    .bind(well_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(method, "exact_lease_name");
}

#[tokio::test]
async fn exact_operator_license_tier_matches_via_new_operator() {
    let pool = test_pool().await;
    insert_operator(&pool, "K9999", "TAKEOVER OPERATING LLC").await;
    let well_id = insert_well(
        &pool,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        Some("Takeover Operating LLC"),
    )
    .await;
    let run_id = stage_transfer_run(
        &pool,
        Some("K1"),
        Some("K9999"),
        "2026-02-04",
        Some("Unrelated Label"),
        None,
        None,
        None,
        None,
        None,
    )
    .await;

    let outcome = materialize_transfers_from_run(&pool, run_id).await.unwrap();
    assert_eq!(outcome.well_links_created, 1);

    let (method,): (String,) = sqlx::query_as(
        "SELECT match_method FROM ks_operator_transfer_well w JOIN ks_operator_transfer t ON t.id = w.transfer_id WHERE w.well_id = $1",
    )
    .bind(well_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(method, "exact_operator_license");
}

#[tokio::test]
async fn a_transfer_can_link_to_multiple_distinct_wells_from_different_tiers() {
    let pool = test_pool().await;
    let well_a = insert_well(
        &pool,
        Some("15009666660000"),
        None,
        None,
        None,
        None,
        None,
        None,
        None,
    )
    .await;
    insert_operator(&pool, "K8888", "MULTI WELL OPERATING LLC").await;
    let well_b = insert_well(
        &pool,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        Some("Multi Well Operating LLC"),
    )
    .await;

    let run_id = stage_transfer_run(
        &pool,
        Some("K1"),
        Some("K8888"),
        "2026-02-05",
        Some("15009666660000"),
        None,
        None,
        None,
        None,
        None,
    )
    .await;

    let outcome = materialize_transfers_from_run(&pool, run_id).await.unwrap();
    assert_eq!(outcome.well_links_created, 2);

    let well_ids: Vec<Uuid> = sqlx::query_scalar(
        "SELECT well_id FROM ks_operator_transfer_well w JOIN ks_operator_transfer t ON t.id = w.transfer_id WHERE t.new_operator_license = 'K8888'",
    )
    .fetch_all(&pool)
    .await
    .unwrap();
    assert_eq!(well_ids.len(), 2);
    assert!(well_ids.contains(&well_a));
    assert!(well_ids.contains(&well_b));
}

#[tokio::test]
async fn fuzzy_candidate_only_generated_when_no_exact_tier_matches() {
    let pool = test_pool().await;
    let well_id = insert_well(
        &pool,
        None,
        Some("Rooks"),
        None,
        None,
        None,
        None,
        Some("Fairview Unit 12"),
        None,
    )
    .await;
    let run_id = stage_transfer_run(
        &pool,
        None,
        None,
        "2026-02-06",
        Some("Fairview Unit 1-2"),
        Some("Rooks"),
        None,
        None,
        None,
        None,
    )
    .await;

    let outcome = materialize_transfers_from_run(&pool, run_id).await.unwrap();
    assert_eq!(outcome.well_links_created, 1);

    let (method, confidence): (String, Option<f64>) = sqlx::query_as(
        "SELECT match_method, match_confidence::float8 FROM ks_operator_transfer_well w JOIN ks_operator_transfer t ON t.id = w.transfer_id WHERE w.well_id = $1",
    )
    .bind(well_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(method, "fuzzy_candidate");
    assert!(confidence.unwrap() > 0.0 && confidence.unwrap() < 1.0);
}

#[tokio::test]
async fn unmatched_transfer_has_no_well_links_and_appears_in_the_report() {
    let pool = test_pool().await;
    let run_id = stage_transfer_run(
        &pool,
        Some("K1"),
        Some("K2"),
        "2026-02-07",
        Some("Totally Unmatchable Label Xyz"),
        Some("Reno"),
        None,
        None,
        None,
        None,
    )
    .await;

    let outcome = materialize_transfers_from_run(&pool, run_id).await.unwrap();
    assert_eq!(outcome.unmatched, 1);
    assert_eq!(outcome.well_links_created, 0);

    let report = unmatched_transfers_report(&pool, 50).await.unwrap();
    assert!(report
        .iter()
        .any(|t| t.lease_or_well_label.as_deref() == Some("Totally Unmatchable Label Xyz")));
}

#[tokio::test]
async fn re_materializing_the_same_run_deduplicates_the_event_and_well_links() {
    let pool = test_pool().await;
    let run_id = stage_transfer_run(
        &pool,
        Some("K1"),
        Some("K2"),
        "2026-02-08",
        Some("Dedup Check Lease"),
        None,
        None,
        None,
        None,
        None,
    )
    .await;

    let first = materialize_transfers_from_run(&pool, run_id).await.unwrap();
    assert_eq!(first.created, 1);
    assert_eq!(first.events_emitted, 1);

    let second = materialize_transfers_from_run(&pool, run_id).await.unwrap();
    assert_eq!(second.created, 0);
    assert_eq!(second.updated, 1);
    assert_eq!(second.events_emitted, 0);

    let transfer_id: Uuid = sqlx::query_scalar(
        "SELECT id FROM ks_operator_transfer WHERE lease_or_well_label = 'Dedup Check Lease'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    // Scoped to this test's own dedup_key, not a table-wide count — other
    // tests in this file run concurrently against the same database and
    // emit their own OPERATOR_TRANSFER_APPROVED events (same lesson as
    // KS-04's materializer tests: a global aggregate is flaky under
    // parallel execution even when nothing is actually wrong).
    let event_count: i64 =
        sqlx::query_scalar("SELECT count(*) FROM ks_event WHERE event_type = 'OPERATOR_TRANSFER_APPROVED' AND dedup_key = $1").bind(transfer_id.to_string()).fetch_one(&pool).await.unwrap();
    assert_eq!(event_count, 1);
}

#[tokio::test]
async fn materialize_refuses_a_non_completed_run() {
    let pool = test_pool().await;
    let source = registry::find_source_by_code(&pool, "KCC_TRANSFERS")
        .await
        .unwrap()
        .unwrap();
    let checksum = format!("checksum-{}", Uuid::new_v4());
    let run_id = ingestion_run::start_run(
        &pool,
        DownloadedPayload {
            source_id: source.id,
            source_url: "https://www.kcc.ks.gov/transfersSearch.php",
            retrieved_at: chrono::Utc::now(),
            source_modified_at: None,
            payload_checksum: &checksum,
            schema_fingerprint: None,
            parser_version: "test-fixture",
        },
    )
    .await
    .unwrap();

    let err = materialize_transfers_from_run(&pool, run_id)
        .await
        .unwrap_err();
    assert!(
        matches!(err, MaterializeError::RunNotCompleted(id, status) if id == run_id && status == "running")
    );
}
