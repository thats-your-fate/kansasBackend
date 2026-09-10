//! KS-05: materializer tests. Stages realistic raw rows directly (rather
//! than re-running the full HTML-discovery/CSV pipeline, which is already
//! covered by `tests/kcc_operators_import.rs`) so these stay focused on
//! materialize logic: identity resolution, active/all reconciliation, and
//! alias retention.
//!
//! Unlike `tests/kgs_wells_materialize.rs`, `materialize_operators_from_run`
//! only ever touches rows scoped to its own `ingestion_run_id` — there is
//! no global cross-run tombstone scan for operators (KS-05 didn't ask for
//! one) — so tests here only need distinct `kcc_license_number` values per
//! test to avoid collisions, not `#[serial]`.

use future_wells_kansas::materialize::kcc_operators::{
    materialize_operators_from_run, MaterializeError,
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

struct RawRow<'a> {
    list_source: &'a str,
    license: Option<&'a str>,
    name: Option<&'a str>,
    city: Option<&'a str>,
}

async fn stage_completed_run(pool: &PgPool, rows: &[RawRow<'_>]) -> Uuid {
    let source = registry::find_source_by_code(pool, "KCC_OPERATORS")
        .await
        .unwrap()
        .unwrap();
    let checksum = format!("checksum-{}", Uuid::new_v4());
    let run_id = ingestion_run::start_run(
        pool,
        DownloadedPayload {
            source_id: source.id,
            source_url: "https://www.kcc.ks.gov/kansas-oil-operators",
            retrieved_at: chrono::Utc::now(),
            source_modified_at: None,
            payload_checksum: &checksum,
            schema_fingerprint: None,
            parser_version: "test-fixture",
        },
    )
    .await
    .unwrap();

    for row in rows {
        let rejection_reason = if row.license.is_none() {
            Some("no parseable license number in this row")
        } else {
            None
        };
        sqlx::query(
            r#"
            INSERT INTO ks_operator_raw_snapshot (
                ingestion_run_id, list_source, kcc_license_number, rejection_reason,
                reported_company_name, source_city, raw_data, source_record_hash
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
            "#,
        )
        .bind(run_id)
        .bind(row.list_source)
        .bind(row.license)
        .bind(rejection_reason)
        .bind(row.name)
        .bind(row.city)
        .bind(serde_json::json!({}))
        .bind(format!("hash-{}", Uuid::new_v4()))
        .execute(pool)
        .await
        .unwrap();
    }

    ingestion_run::complete_run(
        pool,
        run_id,
        rows.len() as i64,
        0,
        None,
        None,
        serde_json::json!({}),
    )
    .await
    .unwrap();
    run_id
}

#[tokio::test]
async fn materialize_creates_operators_and_derives_active_status_from_list_membership() {
    let pool = test_pool().await;
    let run_id = stage_completed_run(
        &pool,
        &[
            RawRow {
                list_source: "active",
                license: Some("K7001"),
                name: Some("Active Only LLC"),
                city: Some("Wichita"),
            },
            RawRow {
                list_source: "all",
                license: Some("K7001"),
                name: Some("Active Only LLC"),
                city: Some("Wichita"),
            },
            RawRow {
                list_source: "all",
                license: Some("K7002"),
                name: Some("Inactive Only Inc"),
                city: Some("Hays"),
            },
        ],
    )
    .await;

    let outcome = materialize_operators_from_run(&pool, run_id).await.unwrap();
    assert_eq!(outcome.created, 2);
    assert_eq!(outcome.updated, 0);
    assert_eq!(outcome.rejected_no_identity, 0);

    let (status_7001,): (String,) =
        sqlx::query_as("SELECT active_status FROM ks_operator WHERE kcc_license_number = 'K7001'")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(status_7001, "active");

    let (status_7002,): (String,) =
        sqlx::query_as("SELECT active_status FROM ks_operator WHERE kcc_license_number = 'K7002'")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(status_7002, "inactive");
}

#[tokio::test]
async fn re_materializing_the_same_run_is_idempotent_not_a_duplicate_create() {
    let pool = test_pool().await;
    let run_id = stage_completed_run(
        &pool,
        &[RawRow {
            list_source: "all",
            license: Some("K7101"),
            name: Some("Repeat Materialize LLC"),
            city: None,
        }],
    )
    .await;

    let first = materialize_operators_from_run(&pool, run_id).await.unwrap();
    assert_eq!(first.created, 1);

    let second = materialize_operators_from_run(&pool, run_id).await.unwrap();
    assert_eq!(second.created, 0);
    assert_eq!(second.updated, 1);

    let count: i64 =
        sqlx::query_scalar("SELECT count(*) FROM ks_operator WHERE kcc_license_number = 'K7101'")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(count, 1);
}

#[tokio::test]
async fn row_with_no_license_is_rejected_not_materialized() {
    let pool = test_pool().await;
    let run_id = stage_completed_run(
        &pool,
        &[
            RawRow {
                list_source: "all",
                license: None,
                name: Some("Unparseable Row Co"),
                city: None,
            },
            RawRow {
                list_source: "all",
                license: Some("K7201"),
                name: Some("Valid Row LLC"),
                city: None,
            },
        ],
    )
    .await;

    let outcome = materialize_operators_from_run(&pool, run_id).await.unwrap();
    assert_eq!(outcome.created, 1);
    assert_eq!(outcome.rejected_no_identity, 1);
}

#[tokio::test]
async fn alias_table_retains_both_variant_spellings_without_losing_reported_name() {
    let pool = test_pool().await;
    let run_id = stage_completed_run(
        &pool,
        &[
            RawRow {
                list_source: "active",
                license: Some("K7301"),
                name: Some("Variant Spelling Co"),
                city: None,
            },
            RawRow {
                list_source: "all",
                license: Some("K7301"),
                name: Some("Variant Spelling Company, LLC"),
                city: None,
            },
        ],
    )
    .await;

    materialize_operators_from_run(&pool, run_id).await.unwrap();

    let (operator_id, reported_name): (Uuid, String) = sqlx::query_as(
        "SELECT id, reported_company_name FROM ks_operator WHERE kcc_license_number = 'K7301'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    // The 'all' list is the more authoritative source for descriptive
    // fields (see materialize::kcc_operators's doc comment).
    assert_eq!(reported_name, "Variant Spelling Company, LLC");

    let alias_names: Vec<String> = sqlx::query_scalar("SELECT reported_company_name FROM ks_operator_alias WHERE operator_id = $1 ORDER BY reported_company_name")
        .bind(operator_id)
        .fetch_all(&pool)
        .await
        .unwrap();
    assert_eq!(
        alias_names,
        vec![
            "Variant Spelling Co".to_string(),
            "Variant Spelling Company, LLC".to_string()
        ]
    );
}

#[tokio::test]
async fn materialize_refuses_a_non_completed_run() {
    let pool = test_pool().await;
    let source = registry::find_source_by_code(&pool, "KCC_OPERATORS")
        .await
        .unwrap()
        .unwrap();
    let checksum = format!("checksum-{}", Uuid::new_v4());
    let run_id = ingestion_run::start_run(
        &pool,
        DownloadedPayload {
            source_id: source.id,
            source_url: "https://www.kcc.ks.gov/kansas-oil-operators",
            retrieved_at: chrono::Utc::now(),
            source_modified_at: None,
            payload_checksum: &checksum,
            schema_fingerprint: None,
            parser_version: "test-fixture",
        },
    )
    .await
    .unwrap();
    // Left in `running` status deliberately — never completed.

    let err = materialize_operators_from_run(&pool, run_id)
        .await
        .unwrap_err();
    assert!(
        matches!(err, MaterializeError::RunNotCompleted(id, status) if id == run_id && status == "running")
    );
}
