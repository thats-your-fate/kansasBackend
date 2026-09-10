//! KS-10: materializer tests — monthly-vs-sentinel `record_kind`
//! separation, correction-safe reruns, unit correctness by product, and
//! the reconciliation/coverage reports. Stages raw rows directly, same
//! discipline as the other materialize test files in this repo.

use future_wells_kansas::event;
use future_wells_kansas::materialize::kgs_production::{
    coverage_summary, materialize_production_from_run, reconciliation_report, MaterializeError,
    ReconciliationGrouping,
};
use future_wells_kansas::source::ingestion_run::{self, DownloadedPayload};
use future_wells_kansas::source::registry;
use serde_json::json;
use sqlx::PgPool;
use uuid::Uuid;

async fn event_count_for_lease(pool: &PgPool, event_type: &str, lease_kid: &str) -> i64 {
    sqlx::query_scalar("SELECT count(*) FROM ks_event WHERE event_type = $1 AND dedup_key LIKE $2")
        .bind(event_type)
        .bind(format!("{lease_kid}:%"))
        .fetch_one(pool)
        .await
        .unwrap()
}

async fn test_pool() -> PgPool {
    let database_url =
        std::env::var("DATABASE_URL").expect("DATABASE_URL must be set to run these tests");
    let pool = future_wells_kansas::db::connect(&database_url)
        .await
        .expect("failed to connect to test database");
    sqlx::migrate!("./migrations").run(&pool).await.unwrap();
    pool
}

struct RawProdRow<'a> {
    lease_kid: &'a str,
    county: Option<&'a str>,
    field_name: Option<&'a str>,
    year: i32,
    month: i16,
    product_raw: &'a str,
    production_volume: f64,
}

async fn stage_run(pool: &PgPool, source_code: &str, rows: &[RawProdRow<'_>]) -> Uuid {
    let source = registry::find_source_by_code(pool, source_code)
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

    for row in rows {
        sqlx::query(
            r#"
            INSERT INTO ks_lease_production_raw_snapshot (
                ingestion_run_id, source_file, lease_kid, county, field_name, year, month, product_raw,
                production_volume, raw_data, source_record_hash
            )
            VALUES ($1, 'test-file', $2, $3, $4, $5, $6, $7, $8, '{}'::jsonb, $9)
            "#,
        )
        .bind(run_id)
        .bind(row.lease_kid)
        .bind(row.county)
        .bind(row.field_name)
        .bind(row.year)
        .bind(row.month)
        .bind(row.product_raw)
        .bind(row.production_volume)
        .bind(format!("hash-{}", Uuid::new_v4()))
        .execute(pool)
        .await
        .unwrap();
    }
    ingestion_run::complete_run(pool, run_id, rows.len() as i64, 0, None, None, json!({}))
        .await
        .unwrap();
    run_id
}

#[tokio::test]
async fn monthly_and_sentinel_rows_get_distinct_record_kinds() {
    let pool = test_pool().await;
    let run_id = stage_run(
        &pool,
        "KGS_PRODUCTION_OIL",
        &[
            RawProdRow {
                lease_kid: "L9001",
                county: Some("Barton"),
                field_name: Some("Test Field"),
                year: 2020,
                month: 1,
                product_raw: "O",
                production_volume: 100.0,
            },
            RawProdRow {
                lease_kid: "L9001",
                county: Some("Barton"),
                field_name: Some("Test Field"),
                year: 2020,
                month: 0,
                product_raw: "O",
                production_volume: 1200.0,
            },
            RawProdRow {
                lease_kid: "L9001",
                county: Some("Barton"),
                field_name: Some("Test Field"),
                year: 2020,
                month: -1,
                product_raw: "O",
                production_volume: 5000.0,
            },
        ],
    )
    .await;

    let outcome = materialize_production_from_run(&pool, run_id)
        .await
        .unwrap();
    assert_eq!(outcome.created, 3);

    let kinds: Vec<(i16, String)> = sqlx::query_as("SELECT month, record_kind FROM ks_lease_production_month WHERE lease_kid = 'L9001' ORDER BY month").fetch_all(&pool).await.unwrap();
    assert_eq!(
        kinds,
        vec![
            (-1, "starting_cumulative".to_string()),
            (0, "yearly_total".to_string()),
            (1, "monthly".to_string())
        ]
    );
}

#[tokio::test]
async fn monthly_chart_query_excludes_sentinel_rows() {
    let pool = test_pool().await;
    let run_id = stage_run(
        &pool,
        "KGS_PRODUCTION_GAS",
        &[
            RawProdRow {
                lease_kid: "L9101",
                county: None,
                field_name: None,
                year: 2021,
                month: 3,
                product_raw: "G",
                production_volume: 50.0,
            },
            RawProdRow {
                lease_kid: "L9101",
                county: None,
                field_name: None,
                year: 2021,
                month: 0,
                product_raw: "G",
                production_volume: 600.0,
            },
        ],
    )
    .await;
    materialize_production_from_run(&pool, run_id)
        .await
        .unwrap();

    let monthly_count: i64 = sqlx::query_scalar("SELECT count(*) FROM ks_lease_production_month WHERE lease_kid = 'L9101' AND record_kind = 'monthly'").fetch_one(&pool).await.unwrap();
    assert_eq!(monthly_count, 1);
    let monthly_sum: f64 = sqlx::query_scalar("SELECT sum(production_volume) FROM ks_lease_production_month WHERE lease_kid = 'L9101' AND record_kind = 'monthly'").fetch_one(&pool).await.unwrap();
    assert_eq!(
        monthly_sum, 50.0,
        "the yearly-total row must never be summed into a monthly aggregation"
    );
}

#[tokio::test]
async fn correction_safe_rerun_upserts_in_place() {
    let pool = test_pool().await;
    let run_id = stage_run(
        &pool,
        "KGS_PRODUCTION_OIL",
        &[RawProdRow {
            lease_kid: "L9201",
            county: None,
            field_name: None,
            year: 2022,
            month: 5,
            product_raw: "O",
            production_volume: 100.0,
        }],
    )
    .await;
    let first = materialize_production_from_run(&pool, run_id)
        .await
        .unwrap();
    assert_eq!(first.created, 1);

    // A later run reports a *corrected* value for the same
    // (lease_kid, product, year, month).
    let run_id_2 = stage_run(
        &pool,
        "KGS_PRODUCTION_OIL",
        &[RawProdRow {
            lease_kid: "L9201",
            county: None,
            field_name: None,
            year: 2022,
            month: 5,
            product_raw: "O",
            production_volume: 175.5,
        }],
    )
    .await;
    let second = materialize_production_from_run(&pool, run_id_2)
        .await
        .unwrap();
    assert_eq!(second.created, 0);
    assert_eq!(second.updated, 1);

    let (count, volume): (i64, f64) = sqlx::query_as("SELECT count(*), max(production_volume) FROM ks_lease_production_month WHERE lease_kid = 'L9201'").fetch_one(&pool).await.unwrap();
    assert_eq!(
        count, 1,
        "a correction must upsert in place, never duplicate"
    );
    assert_eq!(volume, 175.5);
}

#[tokio::test]
async fn oil_and_gas_the_same_lease_kid_and_period_do_not_collide() {
    let pool = test_pool().await;
    // Same lease_kid/year/month, different product — the task doc's
    // upsert key is (lease_kid, product, year, month), so these must
    // remain two distinct rows, not overwrite each other.
    let run_id = stage_run(
        &pool,
        "KGS_PRODUCTION_OIL",
        &[RawProdRow {
            lease_kid: "L9301",
            county: None,
            field_name: None,
            year: 2023,
            month: 6,
            product_raw: "O",
            production_volume: 10.0,
        }],
    )
    .await;
    materialize_production_from_run(&pool, run_id)
        .await
        .unwrap();
    let run_id_gas = stage_run(
        &pool,
        "KGS_PRODUCTION_GAS",
        &[RawProdRow {
            lease_kid: "L9301",
            county: None,
            field_name: None,
            year: 2023,
            month: 6,
            product_raw: "G",
            production_volume: 20.0,
        }],
    )
    .await;
    materialize_production_from_run(&pool, run_id_gas)
        .await
        .unwrap();

    let count: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM ks_lease_production_month WHERE lease_kid = 'L9301'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(count, 2);
    let oil_volume: f64 = sqlx::query_scalar("SELECT production_volume FROM ks_lease_production_month WHERE lease_kid = 'L9301' AND product = 'oil'").fetch_one(&pool).await.unwrap();
    assert_eq!(oil_volume, 10.0);
    let gas_volume: f64 = sqlx::query_scalar("SELECT production_volume FROM ks_lease_production_month WHERE lease_kid = 'L9301' AND product = 'gas'").fetch_one(&pool).await.unwrap();
    assert_eq!(gas_volume, 20.0);
}

#[tokio::test]
async fn coverage_summary_reports_latest_monthly_period_per_product() {
    let pool = test_pool().await;
    let run_id = stage_run(
        &pool,
        "KGS_PRODUCTION_OIL",
        &[
            RawProdRow {
                lease_kid: "L9401",
                county: None,
                field_name: None,
                year: 2024,
                month: 3,
                product_raw: "O",
                production_volume: 10.0,
            },
            RawProdRow {
                lease_kid: "L9402",
                county: None,
                field_name: None,
                year: 2025,
                month: 7,
                product_raw: "O",
                production_volume: 12.0,
            },
        ],
    )
    .await;
    materialize_production_from_run(&pool, run_id)
        .await
        .unwrap();

    let summary = coverage_summary(&pool).await.unwrap();
    assert_eq!(summary.latest_oil_year, Some(2025));
    assert_eq!(summary.latest_oil_month, Some(7));
    assert!(summary.last_successful_import.is_some());
}

#[tokio::test]
async fn reconciliation_report_flags_a_real_discrepancy() {
    let pool = test_pool().await;
    let run_id = stage_run(
        &pool,
        "KGS_PRODUCTION_OIL",
        &[
            RawProdRow {
                lease_kid: "L9501",
                county: Some("ReconciliationCounty"),
                field_name: None,
                year: 2020,
                month: 1,
                product_raw: "O",
                production_volume: 100.0,
            },
            RawProdRow {
                lease_kid: "L9501",
                county: Some("ReconciliationCounty"),
                field_name: None,
                year: 2020,
                month: 2,
                product_raw: "O",
                production_volume: 100.0,
            },
            // KGS's own reported yearly total disagrees with the sum of
            // its own monthly rows (100 + 100 = 200, but reports 500).
            RawProdRow {
                lease_kid: "L9501",
                county: Some("ReconciliationCounty"),
                field_name: None,
                year: 2020,
                month: 0,
                product_raw: "O",
                production_volume: 500.0,
            },
        ],
    )
    .await;
    materialize_production_from_run(&pool, run_id)
        .await
        .unwrap();

    let report = reconciliation_report(&pool, ReconciliationGrouping::County, 0.02, 100)
        .await
        .unwrap();
    let row = report
        .iter()
        .find(|r| r.group_value.as_deref() == Some("ReconciliationCounty"))
        .unwrap();
    assert_eq!(row.monthly_sum, 200.0);
    assert_eq!(row.reported_yearly_total, Some(500.0));
    assert_eq!(row.within_tolerance, Some(false));
}

#[tokio::test]
async fn materialize_refuses_a_non_completed_run() {
    let pool = test_pool().await;
    let source = registry::find_source_by_code(&pool, "KGS_PRODUCTION_OIL")
        .await
        .unwrap()
        .unwrap();
    let run_id = ingestion_run::start_run(
        &pool,
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
    .unwrap();

    let err = materialize_production_from_run(&pool, run_id)
        .await
        .unwrap_err();
    assert!(
        matches!(err, MaterializeError::RunNotCompleted(id, status) if id == run_id && status == "running")
    );
}

/// KS-14: a lease/product's very first monthly row ever materialized is
/// `FIRST_LEASE_PRODUCTION_REPORTED`; every later month (or correction to
/// an existing month) is `LEASE_PRODUCTION_UPDATED` instead.
#[tokio::test]
async fn first_ever_lease_production_and_later_months_are_classified_distinctly() {
    let pool = test_pool().await;
    let lease_kid = format!("LK-EVT-{}", Uuid::new_v4());
    let run1 = stage_run(
        &pool,
        "KGS_PRODUCTION_OIL",
        &[RawProdRow {
            lease_kid: &lease_kid,
            county: None,
            field_name: None,
            year: 2020,
            month: 1,
            product_raw: "O",
            production_volume: 10.0,
        }],
    )
    .await;
    materialize_production_from_run(&pool, run1).await.unwrap();
    assert_eq!(
        event_count_for_lease(&pool, event::FIRST_LEASE_PRODUCTION_REPORTED, &lease_kid).await,
        1
    );
    assert_eq!(
        event_count_for_lease(&pool, event::LEASE_PRODUCTION_UPDATED, &lease_kid).await,
        0
    );

    // A second, later month for the same lease/product is an update, not
    // a second "first."
    let run2 = stage_run(
        &pool,
        "KGS_PRODUCTION_OIL",
        &[RawProdRow {
            lease_kid: &lease_kid,
            county: None,
            field_name: None,
            year: 2020,
            month: 2,
            product_raw: "O",
            production_volume: 12.0,
        }],
    )
    .await;
    materialize_production_from_run(&pool, run2).await.unwrap();
    assert_eq!(
        event_count_for_lease(&pool, event::FIRST_LEASE_PRODUCTION_REPORTED, &lease_kid).await,
        1,
        "still only the one true first-ever event"
    );
    assert_eq!(
        event_count_for_lease(&pool, event::LEASE_PRODUCTION_UPDATED, &lease_kid).await,
        1
    );
}

/// KS-14 "revision-safe dedupe": re-observing the same month/volume must
/// not duplicate an event; a genuine correction to the volume must.
#[tokio::test]
async fn revision_safe_dedupe_for_a_production_correction() {
    let pool = test_pool().await;
    let lease_kid = format!("LK-EVT-{}", Uuid::new_v4());
    let run1 = stage_run(
        &pool,
        "KGS_PRODUCTION_GAS",
        &[RawProdRow {
            lease_kid: &lease_kid,
            county: None,
            field_name: None,
            year: 2021,
            month: 5,
            product_raw: "G",
            production_volume: 100.0,
        }],
    )
    .await;
    materialize_production_from_run(&pool, run1).await.unwrap();
    assert_eq!(
        event_count_for_lease(&pool, event::FIRST_LEASE_PRODUCTION_REPORTED, &lease_kid).await,
        1
    );

    // Re-observing the identical volume must not create a second event of
    // either type.
    let run2 = stage_run(
        &pool,
        "KGS_PRODUCTION_GAS",
        &[RawProdRow {
            lease_kid: &lease_kid,
            county: None,
            field_name: None,
            year: 2021,
            month: 5,
            product_raw: "G",
            production_volume: 100.0,
        }],
    )
    .await;
    materialize_production_from_run(&pool, run2).await.unwrap();
    assert_eq!(
        event_count_for_lease(&pool, event::FIRST_LEASE_PRODUCTION_REPORTED, &lease_kid).await,
        1
    );
    assert_eq!(
        event_count_for_lease(&pool, event::LEASE_PRODUCTION_UPDATED, &lease_kid).await,
        0
    );

    // A genuine correction to the volume must produce a new
    // LEASE_PRODUCTION_UPDATED event.
    let run3 = stage_run(
        &pool,
        "KGS_PRODUCTION_GAS",
        &[RawProdRow {
            lease_kid: &lease_kid,
            county: None,
            field_name: None,
            year: 2021,
            month: 5,
            product_raw: "G",
            production_volume: 150.0,
        }],
    )
    .await;
    materialize_production_from_run(&pool, run3).await.unwrap();
    assert_eq!(
        event_count_for_lease(&pool, event::LEASE_PRODUCTION_UPDATED, &lease_kid).await,
        1,
        "a genuine volume correction must produce a new event"
    );
}

/// KS-14 acceptance criteria: baseline-event suppression, and that it is
/// skipped without cost on the large-scale path (this test only checks
/// correctness; the skip-cost claim is a design property documented in
/// the module itself and confirmed via the real 11.68M-row KS-10 run).
#[tokio::test]
async fn backfill_run_suppresses_production_events_for_historical_months() {
    let pool = test_pool().await;
    let lease_kid = format!("LK-EVT-{}", Uuid::new_v4());
    let run1 = stage_run(
        &pool,
        "KGS_PRODUCTION_OIL",
        &[RawProdRow {
            lease_kid: &lease_kid,
            county: None,
            field_name: None,
            year: 1995,
            month: 6,
            product_raw: "O",
            production_volume: 5.0,
        }],
    )
    .await;
    ingestion_run::mark_as_backfill(&pool, run1).await.unwrap();
    materialize_production_from_run(&pool, run1).await.unwrap();
    assert_eq!(
        event_count_for_lease(&pool, event::FIRST_LEASE_PRODUCTION_REPORTED, &lease_kid).await,
        0,
        "a historical month in a backfill run must not emit an event"
    );

    // The row itself must still be materialized — only the event is
    // suppressed, not the canonical data.
    let materialized: i64 =
        sqlx::query_scalar("SELECT count(*) FROM ks_lease_production_month WHERE lease_kid = $1")
            .bind(&lease_kid)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(materialized, 1);
}
