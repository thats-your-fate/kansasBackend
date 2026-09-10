use chrono::Utc;
use future_wells_kansas::constants;
use future_wells_kansas::source::ingestion_run::{self, DownloadedPayload, IngestionRunError};
use future_wells_kansas::source::registry;
use sqlx::PgPool;
use uuid::Uuid;

async fn test_pool() -> sqlx::PgPool {
    let database_url =
        std::env::var("DATABASE_URL").expect("DATABASE_URL must be set to run these tests");
    let pool = future_wells_kansas::db::connect(&database_url)
        .await
        .expect("failed to connect to test database");
    sqlx::migrate!("./migrations").run(&pool).await.unwrap();
    pool
}

async fn source_id(pool: &PgPool, code: &str) -> Uuid {
    registry::find_source_by_code(pool, code)
        .await
        .unwrap()
        .unwrap_or_else(|| panic!("{code} must be seeded by 0002_ks_source_registry.sql"))
        .id
}

#[tokio::test]
async fn all_twelve_ingestion_source_codes_are_seeded() {
    let pool = test_pool().await;
    let sources = registry::list_sources(&pool).await.unwrap();
    let seeded_codes: Vec<&str> = sources.iter().map(|s| s.code.as_str()).collect();

    for code in constants::INGESTION_SOURCE_CODES {
        assert!(
            seeded_codes.contains(code),
            "{code} is declared in constants::INGESTION_SOURCE_CODES but missing from ks_source"
        );
    }
    assert_eq!(
        sources.len(),
        constants::INGESTION_SOURCE_CODES.len(),
        "ks_source row count must match constants::INGESTION_SOURCE_CODES exactly"
    );
}

/// KS-02: "Set Kansas production to a default minimum_allowed_year=1990."
/// This asserts the DB-seeded value stays in sync with
/// `constants::MIN_PRODUCTION_YEAR` (already enforced independently by
/// `constants::tests::production_files_never_predate_min_year`) rather
/// than letting the two representations drift apart.
#[tokio::test]
async fn production_sources_minimum_year_matches_constant() {
    let pool = test_pool().await;
    for code in [constants::KGS_PRODUCTION_OIL, constants::KGS_PRODUCTION_GAS] {
        let source = registry::find_source_by_code(&pool, code)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            source.minimum_allowed_year,
            Some(constants::MIN_PRODUCTION_YEAR),
            "{code}.minimum_allowed_year must match constants::MIN_PRODUCTION_YEAR"
        );
        assert!(source.allows_year(constants::MIN_PRODUCTION_YEAR));
        assert!(!source.allows_year(constants::MIN_PRODUCTION_YEAR - 1));
    }
}

/// KS-02: "Mark KCC operator and intent source contact fields as excluded
/// from product indexes and exports."
#[tokio::test]
async fn kcc_operators_and_intents_require_the_privacy_guard() {
    let pool = test_pool().await;
    for code in [constants::KCC_OPERATORS, constants::KCC_INTENTS] {
        let source = registry::find_source_by_code(&pool, code)
            .await
            .unwrap()
            .unwrap();
        assert!(
            source.requires_privacy_guard(),
            "{code} must require privacy::assert_no_forbidden_fields on any public response"
        );
        assert!(
            source.allowed_for_public_product,
            "{code}'s non-contact fields (name, license, dates) remain public product data"
        );
    }
}

/// KS-02 acceptance criterion: "identical snapshots are idempotent."
#[tokio::test]
async fn identical_checksum_is_recognized_as_already_completed() {
    let pool = test_pool().await;
    let source_id = source_id(&pool, constants::KGS_WELLS_ZIP).await;
    let checksum = format!("checksum-{}", Uuid::new_v4());

    assert!(
        ingestion_run::find_completed_run_by_checksum(&pool, source_id, &checksum)
            .await
            .unwrap()
            .is_none(),
        "a never-seen checksum must not match any completed run"
    );

    let run_id = ingestion_run::start_run(
        &pool,
        DownloadedPayload {
            source_id,
            source_url: "https://example.invalid/ks_wells.zip",
            retrieved_at: Utc::now(),
            source_modified_at: None,
            payload_checksum: &checksum,
            schema_fingerprint: None,
            parser_version: "test-1",
        },
    )
    .await
    .unwrap();
    ingestion_run::complete_run(&pool, run_id, 100, 0, None, None, serde_json::json!({}))
        .await
        .unwrap();

    let found = ingestion_run::find_completed_run_by_checksum(&pool, source_id, &checksum)
        .await
        .unwrap();
    assert_eq!(
        found,
        Some(run_id),
        "the same checksum must now resolve to the completed run"
    );
}

/// KS-02 acceptance criterion: "failed runs cannot partially replace the
/// published dataset" — enforced here as "a run can only reach `completed`
/// via `running`, never directly, and never twice."
#[tokio::test]
async fn a_run_cannot_complete_or_fail_twice() {
    let pool = test_pool().await;
    let source_id = source_id(&pool, constants::KGS_FIELDS).await;
    let checksum = format!("checksum-{}", Uuid::new_v4());

    let run_id = ingestion_run::start_run(
        &pool,
        DownloadedPayload {
            source_id,
            source_url: "https://example.invalid/fields.zip",
            retrieved_at: Utc::now(),
            source_modified_at: None,
            payload_checksum: &checksum,
            schema_fingerprint: None,
            parser_version: "test-1",
        },
    )
    .await
    .unwrap();

    ingestion_run::fail_run(&pool, run_id, "simulated failure")
        .await
        .unwrap();

    let second_fail = ingestion_run::fail_run(&pool, run_id, "second failure").await;
    assert!(matches!(
        second_fail,
        Err(IngestionRunError::InvalidTransition { .. })
    ));

    let complete_after_fail =
        ingestion_run::complete_run(&pool, run_id, 1, 0, None, None, serde_json::json!({})).await;
    assert!(matches!(
        complete_after_fail,
        Err(IngestionRunError::InvalidTransition { .. })
    ));
}

/// KS-03: a run that executed successfully but failed its own QA gate
/// (count tolerance / source-change / missing object-ID range) must be
/// distinguishable from a plain execution failure, and must equally never
/// be completable afterward.
#[tokio::test]
async fn a_rejected_run_stores_its_manifest_and_cannot_then_complete() {
    let pool = test_pool().await;
    let source_id = source_id(&pool, constants::KGS_WELLS_ARCGIS).await;
    let checksum = format!("checksum-{}", Uuid::new_v4());

    let run_id = ingestion_run::start_run(
        &pool,
        DownloadedPayload {
            source_id,
            source_url: "https://example.invalid/wells",
            retrieved_at: Utc::now(),
            source_modified_at: None,
            payload_checksum: &checksum,
            schema_fingerprint: None,
            parser_version: "test-1",
        },
    )
    .await
    .unwrap();

    let manifest = serde_json::json!({"count_estimate": 519092, "count_extracted": 400000});
    ingestion_run::reject_run(
        &pool,
        run_id,
        "extracted count outside tolerance",
        manifest.clone(),
    )
    .await
    .unwrap();

    let (status, failure_message, stored_manifest): (String, Option<String>, serde_json::Value) =
        sqlx::query_as(
            "SELECT status, failure_message, manifest FROM ks_ingestion_run WHERE id = $1",
        )
        .bind(run_id)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(status, "rejected");
    assert_eq!(
        failure_message.as_deref(),
        Some("extracted count outside tolerance")
    );
    assert_eq!(stored_manifest, manifest);

    let complete_after_reject =
        ingestion_run::complete_run(&pool, run_id, 1, 0, None, None, serde_json::json!({})).await;
    assert!(matches!(
        complete_after_reject,
        Err(IngestionRunError::InvalidTransition { .. })
    ));
}

#[tokio::test]
async fn rejection_can_be_recorded_against_a_run() {
    let pool = test_pool().await;
    let source_id = source_id(&pool, constants::KCC_TRANSFERS).await;
    let checksum = format!("checksum-{}", Uuid::new_v4());

    let run_id = ingestion_run::start_run(
        &pool,
        DownloadedPayload {
            source_id,
            source_url: "https://example.invalid/transfers",
            retrieved_at: Utc::now(),
            source_modified_at: None,
            payload_checksum: &checksum,
            schema_fingerprint: None,
            parser_version: "test-1",
        },
    )
    .await
    .unwrap();

    ingestion_run::record_rejection(
        &pool,
        run_id,
        Some(7),
        Some(serde_json::json!({"lease_name": "bad row"})),
        "missing operator_license_number",
    )
    .await
    .unwrap();

    ingestion_run::complete_run(&pool, run_id, 10, 1, None, None, serde_json::json!({}))
        .await
        .unwrap();

    let rejection_count: i64 =
        sqlx::query_scalar("SELECT count(*) FROM ks_source_rejection WHERE ingestion_run_id = $1")
            .bind(run_id)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(rejection_count, 1);
}
