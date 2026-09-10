//! KS-17: integration tests for `monitoring` — source freshness (stale
//! vs. failed as independent states), row-count/schema-fingerprint/
//! content-type change detection, rejected-run anomalies, well data
//! quality, link confidence, and the two hard publication gates.

use future_wells_kansas::monitoring;
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

async fn start_run_for(pool: &PgPool, source_code: &str, checksum: &str) -> Uuid {
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
            payload_checksum: checksum,
            schema_fingerprint: None,
            parser_version: "test-fixture",
        },
    )
    .await
    .unwrap()
}

#[tokio::test]
async fn stale_and_failed_are_independent_states() {
    let pool = test_pool().await;

    // A source with a real completed run "now" is not stale, regardless
    // of its own most-recent status.
    let run_id = start_run_for(
        &pool,
        "KGS_WELLS_ARCGIS",
        &format!("checksum-{}", Uuid::new_v4()),
    )
    .await;
    ingestion_run::complete_run(&pool, run_id, 10, 0, None, None, json!({}))
        .await
        .unwrap();

    let report = monitoring::source_freshness_report(&pool).await.unwrap();
    let wells = report
        .iter()
        .find(|f| f.source_code == "KGS_WELLS_ARCGIS")
        .unwrap();
    assert!(
        !wells.is_stale(),
        "a source with a just-completed run must not be stale"
    );
    assert!(
        !wells.is_failed(),
        "the most recent run completed, so this source must not be reported failed"
    );

    // KCC_CONSERVATION_DOCKETS has no importer anywhere in this repo (no
    // conservation-docket task has been built) and a real, non-"irregular"
    // cadence ("daily") — a deliberately chosen source that no test in
    // this suite ever creates a run for, so this assertion stays robust
    // regardless of which other tests happen to run alongside it.
    let candidate = report
        .iter()
        .find(|f| f.source_code == "KCC_CONSERVATION_DOCKETS")
        .unwrap();
    assert!(candidate.last_successful_import.is_none());
    assert!(
        candidate.is_stale(),
        "a source with no successful import ever, on a real cadence, must be stale: {candidate:?}"
    );
    assert!(
        !candidate.is_failed(),
        "no run at all is not the same as a failed run"
    );
}

#[tokio::test]
async fn a_failed_most_recent_run_is_reported_failed_independent_of_staleness() {
    let pool = test_pool().await;
    let source_code = "KCC_TRANSFERS";

    let good_run = start_run_for(&pool, source_code, &format!("checksum-{}", Uuid::new_v4())).await;
    ingestion_run::complete_run(&pool, good_run, 5, 0, None, None, json!({}))
        .await
        .unwrap();

    let failing_run =
        start_run_for(&pool, source_code, &format!("checksum-{}", Uuid::new_v4())).await;
    ingestion_run::fail_run(&pool, failing_run, "simulated failure for test")
        .await
        .unwrap();

    let report = monitoring::source_freshness_report(&pool).await.unwrap();
    let transfers = report
        .iter()
        .find(|f| f.source_code == source_code)
        .unwrap();
    assert!(
        transfers.is_failed(),
        "the most recent run failed, so this must be reported failed"
    );
    assert!(!transfers.is_stale(), "a real completed run happened moments ago, so this must not be stale even though the latest run failed");
}

#[tokio::test]
async fn row_count_change_compares_the_two_most_recent_completed_runs() {
    let pool = test_pool().await;
    let source_code = "KGS_FIELDS";

    let run1 = start_run_for(&pool, source_code, &format!("checksum-{}", Uuid::new_v4())).await;
    ingestion_run::complete_run(&pool, run1, 100, 0, None, None, json!({}))
        .await
        .unwrap();
    let run2 = start_run_for(&pool, source_code, &format!("checksum-{}", Uuid::new_v4())).await;
    ingestion_run::complete_run(&pool, run2, 150, 0, None, None, json!({}))
        .await
        .unwrap();

    let report = monitoring::row_count_change_report(&pool).await.unwrap();
    let fields = report
        .iter()
        .find(|r| r.source_code == source_code)
        .unwrap();
    assert_eq!(fields.current_row_count, Some(150));
    assert_eq!(fields.previous_row_count, Some(100));
    assert_eq!(fields.change_ratio(), Some(0.5));
}

#[tokio::test]
async fn schema_fingerprint_change_is_none_when_unknown_and_some_when_known() {
    let pool = test_pool().await;
    // KCC_KOLAR_DOCUMENTS has no importer anywhere in this repo, so no
    // other test file ever creates a run for it — this test needs an
    // exact pairing of "the two most recent runs," which a widely-shared
    // fixture source (e.g. any real KGS_* source) cannot safely guarantee
    // under this suite's full parallel execution.
    let source_code = "KCC_KOLAR_DOCUMENTS";
    let source = registry::find_source_by_code(&pool, source_code)
        .await
        .unwrap()
        .unwrap();

    let run1 = ingestion_run::start_run(
        &pool,
        DownloadedPayload {
            source_id: source.id,
            source_url: "test",
            retrieved_at: chrono::Utc::now(),
            source_modified_at: None,
            payload_checksum: &format!("c-{}", Uuid::new_v4()),
            schema_fingerprint: Some("fp-v1"),
            parser_version: "test",
        },
    )
    .await
    .unwrap();
    ingestion_run::complete_run(&pool, run1, 1, 0, None, None, json!({}))
        .await
        .unwrap();

    let run2 = ingestion_run::start_run(
        &pool,
        DownloadedPayload {
            source_id: source.id,
            source_url: "test",
            retrieved_at: chrono::Utc::now(),
            source_modified_at: None,
            payload_checksum: &format!("c-{}", Uuid::new_v4()),
            schema_fingerprint: Some("fp-v2-different"),
            parser_version: "test",
        },
    )
    .await
    .unwrap();
    ingestion_run::complete_run(&pool, run2, 1, 0, None, None, json!({}))
        .await
        .unwrap();

    let report = monitoring::schema_fingerprint_change_report(&pool)
        .await
        .unwrap();
    let documents = report
        .iter()
        .find(|r| r.source_code == source_code)
        .unwrap();
    assert_eq!(documents.changed(), Some(true));

    // A source that has never recorded a fingerprint reports `None`
    // (unknown), never `false` (silently claimed unchanged).
    // KCC_CONSERVATION_DOCKETS has no importer anywhere in this repo, so
    // no test ever creates a run for it — a stable, always-untouched
    // candidate regardless of what else runs alongside this test.
    let unknown_source = report
        .iter()
        .find(|r| r.source_code == "KCC_CONSERVATION_DOCKETS")
        .unwrap();
    assert_eq!(unknown_source.current_fingerprint, None);
    assert_eq!(unknown_source.changed(), None);
}

#[tokio::test]
async fn rejected_runs_are_counted_as_anomalies_within_the_window() {
    let pool = test_pool().await;
    let source_code = "KCC_INTENTS";
    let run_id = start_run_for(&pool, source_code, &format!("checksum-{}", Uuid::new_v4())).await;
    ingestion_run::reject_run(&pool, run_id, "zero rows returned", json!({}))
        .await
        .unwrap();

    let report = monitoring::rejected_run_anomalies_report(&pool, 30)
        .await
        .unwrap();
    let intents = report
        .iter()
        .find(|r| r.source_code == source_code)
        .unwrap();
    assert!(intents.rejected_run_count >= 1);

    let outside_window = monitoring::rejected_run_anomalies_report(&pool, 0)
        .await
        .unwrap();
    assert!(
        !outside_window.iter().any(|r| r.source_code == source_code),
        "a 0-day window must not include a run that started before this exact instant"
    );
}

#[tokio::test]
async fn well_data_quality_finds_missing_geometry_invalid_api_and_raw_duplicate_apis() {
    let pool = test_pool().await;
    let run_id = start_run_for(
        &pool,
        "KGS_WELLS_ARCGIS",
        &format!("checksum-{}", Uuid::new_v4()),
    )
    .await;

    // Two raw rows reporting the SAME API within one run — a real
    // source-side duplicate, even though materialization's own unique
    // index means it could never corrupt the canonical table.
    for _ in 0..2 {
        sqlx::query("INSERT INTO ks_well_raw_snapshot (ingestion_run_id, source_format, raw_data, kansas_membership_status) VALUES ($1, 'arcgis', $2, 'included')")
            .bind(run_id)
            .bind(json!({"API_NUMBER": "15-009-99999-0000"}))
            .execute(&pool)
            .await
            .unwrap();
    }
    ingestion_run::complete_run(&pool, run_id, 2, 0, None, None, json!({}))
        .await
        .unwrap();

    // A canonical well with an invalid API and no geometry.
    sqlx::query(
        "INSERT INTO ks_well (api_raw, api_validation_status, well_identity_match_method, lifecycle_status, source_format, source_ingestion_run_id) VALUES ('garbage', 'malformed', 'kid_fallback', 'active', 'arcgis', $1)",
    )
    .bind(run_id)
    .execute(&pool)
    .await
    .unwrap();

    let report = monitoring::well_data_quality_report(&pool).await.unwrap();
    assert_eq!(
        report.duplicate_api_count_latest_wells_run, 1,
        "one distinct API value appeared twice in the raw snapshot"
    );
    assert!(report.invalid_api_count >= 1);
    assert!(report.missing_geometry_count >= 1);
}

#[tokio::test]
async fn link_confidence_reports_a_distribution_across_categories() {
    let pool = test_pool().await;
    let run_id = start_run_for(
        &pool,
        "KCC_INTENTS",
        &format!("checksum-{}", Uuid::new_v4()),
    )
    .await;
    sqlx::query(
        "INSERT INTO ks_drilling_intent (source_filename, match_method, first_observed_at, last_observed_at, source_ingestion_run_id, source_record_hash) VALUES ($1, 'unresolved', now(), now(), $2, 'hash')",
    )
    .bind(format!("intent-{}.pdf", Uuid::new_v4()))
    .bind(run_id)
    .execute(&pool)
    .await
    .unwrap();

    let report = monitoring::link_confidence_report(&pool).await.unwrap();
    assert!(report
        .iter()
        .any(|b| b.category == "intent_well_link" && b.method == "unresolved" && b.count >= 1));
}

#[tokio::test]
async fn publication_gate_fails_on_a_restricted_year_violation_and_passes_without_one() {
    let pool = test_pool().await;
    let before = monitoring::publication_gate(&pool).await.unwrap();
    assert!(
        before.restricted_year_violations.is_empty(),
        "a fresh database must start with zero restricted-year rows"
    );

    let run_id = start_run_for(
        &pool,
        "KGS_PRODUCTION_OIL",
        &format!("checksum-{}", Uuid::new_v4()),
    )
    .await;
    ingestion_run::complete_run(&pool, run_id, 1, 0, None, None, json!({}))
        .await
        .unwrap();
    sqlx::query(
        r#"
        INSERT INTO ks_lease_production_month (lease_kid, product, year, month, record_kind, production_volume, source_ingestion_run_id, source_record_hash)
        VALUES ($1, 'oil', 1985, 3, 'monthly', 10.0, $2, 'hash')
        "#,
    )
    .bind(format!("LK-GATE-{}", Uuid::new_v4()))
    .bind(run_id)
    .execute(&pool)
    .await
    .unwrap();

    let after = monitoring::publication_gate(&pool).await.unwrap();
    assert!(
        !after.passes(),
        "a pre-permitted-year production row must fail the publication gate"
    );
    assert!(after
        .restricted_year_violations
        .iter()
        .any(|v| v.year == 1985));
}

#[tokio::test]
async fn publication_gate_fails_on_a_phone_number_leaking_into_an_indexed_field() {
    let pool = test_pool().await;
    let run_id = start_run_for(
        &pool,
        "KCC_OPERATORS",
        &format!("checksum-{}", Uuid::new_v4()),
    )
    .await;
    sqlx::query(
        "INSERT INTO ks_operator (kcc_license_number, reported_company_name, normalized_company_name, active_status, source_record_hash, source_ingestion_run_id) VALUES ($1, 'Call us at 785-555-0199', 'call us', 'active', 'hash', $2)",
    )
    .bind(format!("OP-GATE-{}", Uuid::new_v4()))
    .bind(run_id)
    .execute(&pool)
    .await
    .unwrap();

    let result = monitoring::publication_gate(&pool).await.unwrap();
    assert!(!result.passes());
    assert!(result
        .personal_field_leakage
        .iter()
        .any(|h| h.table_name == "ks_operator" && h.column_name == "reported_company_name"));
}
