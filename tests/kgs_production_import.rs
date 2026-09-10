//! KS-10 acceptance criteria: fixture tests for the KGS monthly
//! lease-production importer. Confirmed live 2026-09-05 against all 8
//! real decade archives (11,678,706 real rows staged, 0 rejected, in
//! ~9 minutes) — see `docs/ops/ks-10-kgs-production-live-smoke.md`.
//! These automated tests still run only against small hand-built
//! fixture ZIPs, never the live service.

use future_wells_kansas::parse::kgs_production::{
    run_single_file_import, ImportOptions, ImportStatus,
};
use future_wells_kansas::source::retry::RetryPolicy;
use sqlx::PgPool;
use std::io::Write;
use std::time::Duration;

async fn test_pool() -> PgPool {
    let database_url =
        std::env::var("DATABASE_URL").expect("DATABASE_URL must be set to run these tests");
    let pool = future_wells_kansas::db::connect(&database_url)
        .await
        .expect("failed to connect to test database");
    sqlx::migrate!("./migrations").run(&pool).await.unwrap();
    pool
}

fn fast_options() -> ImportOptions {
    ImportOptions {
        retry_policy: RetryPolicy {
            max_attempts: 3,
            base_delay: Duration::from_millis(5),
        },
        batch_size: 10,
        legal_review_override_enabled: false,
    }
}

fn build_fixture_zip(entry_name: &str, rows: &[&str]) -> Vec<u8> {
    let header = "\"LEASE_KID\",\"LEASE\",\"DOR_CODE\",\"API_NUMBER\",\"FIELD\",\"PRODUCING_ZONE\",\"OPERATOR\",\"COUNTY\",\"TOWNSHIP\",\"TWN_DIR\",\"RANGE\",\"RANGE_DIR\",\"SECTION\",\"SPOT\",\"LATITUDE\",\"LONGITUDE\",\"MONTH-YEAR\",\"PRODUCT\",\"WELLS\",\"PRODUCTION\",\"URL\"\r\n";
    let mut body = header.to_string();
    for row in rows {
        body.push_str(row);
        body.push_str("\r\n");
    }
    let mut buf = Vec::new();
    {
        let mut zip = zip::ZipWriter::new(std::io::Cursor::new(&mut buf));
        zip.start_file(entry_name, zip::write::SimpleFileOptions::default())
            .unwrap();
        zip.write_all(body.as_bytes()).unwrap();
        zip.finish().unwrap();
    }
    buf
}

fn oil_row(lease_kid: &str, month_year: &str, volume: &str) -> String {
    format!(
        "\"{lease_kid}\",\"IMPORT TEST LEASE\",\"111111\",\"15-009-11111\",\"TEST FIELD\",\"Mississippian\",\"Test Operating LLC\",\"Barton\",\"20\",\"S\",\"10\",\"W\",\"4\",\"\",\"38.1\",\"-98.1\",\"{month_year}\",\"O\",\"2\",\"{volume}\",\"https://example/{lease_kid}\""
    )
}

async fn run_oil_2020_present(
    server_dir: &std::path::Path,
    rows: &[String],
) -> (
    PgPool,
    future_wells_kansas::parse::kgs_production::FileImportOutcome,
) {
    let pool = test_pool().await;
    let row_refs: Vec<&str> = rows.iter().map(String::as_str).collect();
    let zip_bytes = build_fixture_zip("oil_leases_2020_present.txt", &row_refs);

    let server = wiremock::MockServer::start().await;
    wiremock::Mock::given(wiremock::matchers::method("GET"))
        .and(wiremock::matchers::path("/oil_leases_2020_present.zip"))
        .respond_with(wiremock::ResponseTemplate::new(200).set_body_bytes(zip_bytes))
        .mount(&server)
        .await;

    let client = reqwest::Client::new();
    let url = format!("{}/oil_leases_2020_present.zip", server.uri());
    let dest = server_dir.join(format!("dest_{}.zip", uuid::Uuid::new_v4()));
    let outcome = run_single_file_import(
        &pool,
        &client,
        &url,
        "oil_leases_2020_present.zip",
        &dest,
        fast_options(),
    )
    .await
    .unwrap();
    (pool, outcome)
}

#[tokio::test]
async fn full_import_stages_monthly_and_sentinel_rows() {
    let dir = std::env::temp_dir();
    let rows = vec![
        oil_row("9001", "1-2020", "100"),
        oil_row("9001", "0-2020", "1200"),
        oil_row("9001", "-1-2020", "5000"),
    ];
    let (pool, outcome) = run_oil_2020_present(&dir, &rows).await;

    assert_eq!(outcome.status, ImportStatus::Completed);
    assert_eq!(outcome.staged_count, 3);
    assert_eq!(outcome.rejected_count, 0);

    let staged: i64 = sqlx::query_scalar("SELECT count(*) FROM ks_lease_production_raw_snapshot WHERE ingestion_run_id = $1 AND rejection_reason IS NULL")
        .bind(outcome.run_id)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(staged, 3);
}

#[tokio::test]
async fn zero_rows_rejects_the_run() {
    let dir = std::env::temp_dir();
    let rows: Vec<String> = vec![];
    let (_, outcome) = run_oil_2020_present(&dir, &rows).await;
    assert_eq!(outcome.status, ImportStatus::Rejected);
}

#[tokio::test]
async fn row_with_no_lease_kid_is_rejected_not_staged() {
    let dir = std::env::temp_dir();
    let bad_row = "\"\",\"NO KID\",\"111111\",\"\",\"TEST FIELD\",\"\",\"Test Operating LLC\",\"Barton\",\"20\",\"S\",\"10\",\"W\",\"4\",\"\",\"38.1\",\"-98.1\",\"1-2020\",\"O\",\"2\",\"100\",\"https://example/no-kid\"".to_string();
    let rows = vec![oil_row("9101", "1-2020", "100"), bad_row];
    let (_, outcome) = run_oil_2020_present(&dir, &rows).await;

    assert_eq!(outcome.status, ImportStatus::Completed);
    assert_eq!(outcome.staged_count, 1);
    assert_eq!(outcome.rejected_count, 1);
}

#[tokio::test]
async fn pre_1990_rows_are_blocked_unless_the_legal_review_override_is_enabled() {
    let dir = std::env::temp_dir();
    let pool = test_pool().await;
    let rows = [oil_row("9201", "1-1985", "100")];
    let row_refs: Vec<&str> = rows.iter().map(String::as_str).collect();
    let zip_bytes = build_fixture_zip("oil_leases_2020_present.txt", &row_refs);

    let server = wiremock::MockServer::start().await;
    wiremock::Mock::given(wiremock::matchers::method("GET"))
        .and(wiremock::matchers::path("/oil_leases_2020_present.zip"))
        .respond_with(wiremock::ResponseTemplate::new(200).set_body_bytes(zip_bytes))
        .mount(&server)
        .await;
    let client = reqwest::Client::new();
    let url = format!("{}/oil_leases_2020_present.zip", server.uri());
    let dest = dir.join(format!("dest_{}.zip", uuid::Uuid::new_v4()));

    let blocked = run_single_file_import(
        &pool,
        &client,
        &url,
        "oil_leases_2020_present.zip",
        &dest,
        fast_options(),
    )
    .await
    .unwrap();
    assert_eq!(blocked.status, ImportStatus::Rejected, "with zero staged rows and one pre-1990-blocked row, the run must reject, not silently publish nothing");
    assert_eq!(blocked.pre_1990_blocked_count, 1);
}

#[tokio::test]
async fn pre_1990_override_flag_allows_ingestion_when_explicitly_enabled() {
    let dir = std::env::temp_dir();
    let pool = test_pool().await;
    let rows = [oil_row("9301", "1-1985", "100")];
    let row_refs: Vec<&str> = rows.iter().map(String::as_str).collect();
    let zip_bytes = build_fixture_zip("oil_leases_2020_present.txt", &row_refs);

    let server = wiremock::MockServer::start().await;
    wiremock::Mock::given(wiremock::matchers::method("GET"))
        .and(wiremock::matchers::path("/oil_leases_2020_present.zip"))
        .respond_with(wiremock::ResponseTemplate::new(200).set_body_bytes(zip_bytes))
        .mount(&server)
        .await;
    let client = reqwest::Client::new();
    let url = format!("{}/oil_leases_2020_present.zip", server.uri());
    let dest = dir.join(format!("dest_{}.zip", uuid::Uuid::new_v4()));

    let mut options = fast_options();
    options.legal_review_override_enabled = true;
    let outcome = run_single_file_import(
        &pool,
        &client,
        &url,
        "oil_leases_2020_present.zip",
        &dest,
        options,
    )
    .await
    .unwrap();
    assert_eq!(outcome.status, ImportStatus::Completed);
    assert_eq!(outcome.staged_count, 1);
    assert_eq!(outcome.pre_1990_blocked_count, 0);
}
