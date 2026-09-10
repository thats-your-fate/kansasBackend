//! KS-05 acceptance criteria: fixture tests for the KCC operator importer.
//! Everything here runs against a `wiremock` server serving a hand-built
//! HTML page and CSV bodies modeled on KCC's documented conventions —
//! `kcc.ks.gov` itself is confirmed unreachable from this development
//! environment (see docs/architecture/kansas-source-boundary.md and the
//! task doc's KS-05 entry), so unlike the KS-03/KS-04 fixture tests, there
//! is no live-service smoke check to pair this with yet.

use future_wells_kansas::parse::kcc_operators::{run_operator_import, ImportOptions, ImportStatus};
use future_wells_kansas::source::retry::RetryPolicy;
use sqlx::PgPool;
use std::time::Duration;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

async fn test_pool() -> PgPool {
    let database_url =
        std::env::var("DATABASE_URL").expect("DATABASE_URL must be set to run these tests");
    let pool = future_wells_kansas::db::connect(&database_url)
        .await
        .expect("failed to connect to test database");
    sqlx::migrate!("./migrations").run(&pool).await.unwrap();
    pool
}

fn fast_retry_policy() -> RetryPolicy {
    RetryPolicy {
        max_attempts: 3,
        base_delay: Duration::from_millis(5),
    }
}

fn page_html_with_links() -> String {
    r#"
        <html><body>
            <h1>Kansas Oil Operators</h1>
            <a href="/active-operators.csv">Download Active Operators (CSV)</a>
            <a href="/all-operators.csv">Download All Operators (CSV)</a>
        </body></html>
    "#
    .to_string()
}

fn page_html_without_links() -> String {
    r#"<html><body><a href="/home">Home</a></body></html>"#.to_string()
}

fn operator_csv(rows: &[(&str, &str, &str)]) -> String {
    let mut csv = String::from("License Number,Operator Name,City\n");
    for (license, name, city) in rows {
        csv.push_str(&format!("{license},{name},{city}\n"));
    }
    csv
}

async fn mount_page(server: &MockServer, html: String) {
    Mock::given(method("GET"))
        .and(path("/kansas-oil-operators"))
        .respond_with(ResponseTemplate::new(200).set_body_string(html))
        .mount(server)
        .await;
}

async fn mount_csv(server: &MockServer, route: &str, body: String) {
    Mock::given(method("GET"))
        .and(path(route))
        .respond_with(ResponseTemplate::new(200).set_body_string(body))
        .mount(server)
        .await;
}

#[tokio::test]
async fn full_import_discovers_links_downloads_and_stages_rows() {
    let pool = test_pool().await;
    let server = MockServer::start().await;
    mount_page(&server, page_html_with_links()).await;
    mount_csv(
        &server,
        "/active-operators.csv",
        operator_csv(&[("9001", "Test A Operating LLC", "Wichita")]),
    )
    .await;
    mount_csv(
        &server,
        "/all-operators.csv",
        operator_csv(&[
            ("9001", "Test A Operating LLC", "Wichita"),
            ("9002", "Test A Idle Wells Inc", "Hays"),
        ]),
    )
    .await;

    let client = reqwest::Client::new();
    let page_url = format!("{}/kansas-oil-operators", server.uri());
    let outcome = run_operator_import(
        &pool,
        &client,
        &page_url,
        ImportOptions {
            retry_policy: fast_retry_policy(),
        },
    )
    .await
    .unwrap();

    assert_eq!(outcome.status, ImportStatus::Completed);
    assert_eq!(outcome.active_count, 1);
    assert_eq!(outcome.all_count, 2);

    let staged: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM ks_operator_raw_snapshot WHERE ingestion_run_id = $1",
    )
    .bind(outcome.run_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(staged, 3);

    let reconciliation = outcome.manifest.get("reconciliation").unwrap();
    assert_eq!(reconciliation.get("all_not_in_active_count").unwrap(), 1);
    assert_eq!(reconciliation.get("active_not_in_all_count").unwrap(), 0);
}

#[tokio::test]
async fn no_download_links_found_rejects_the_run_as_a_page_change_alert() {
    let pool = test_pool().await;
    let server = MockServer::start().await;
    mount_page(&server, page_html_without_links()).await;

    let client = reqwest::Client::new();
    let page_url = format!("{}/kansas-oil-operators", server.uri());
    let outcome = run_operator_import(
        &pool,
        &client,
        &page_url,
        ImportOptions {
            retry_policy: fast_retry_policy(),
        },
    )
    .await
    .unwrap();

    assert_eq!(outcome.status, ImportStatus::Rejected);
    let status: String = sqlx::query_scalar("SELECT status FROM ks_ingestion_run WHERE id = $1")
        .bind(outcome.run_id)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(status, "rejected");
    let failure_message: String =
        sqlx::query_scalar("SELECT failure_message FROM ks_ingestion_run WHERE id = $1")
            .bind(outcome.run_id)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert!(failure_message.contains("link discovery failed"));
}

#[tokio::test]
async fn zero_parsed_operators_rejects_the_run_rather_than_publishing_empty() {
    let pool = test_pool().await;
    let server = MockServer::start().await;
    mount_page(&server, page_html_with_links()).await;
    // Header-only CSVs: links resolve, but zero rows parse — must not be
    // treated as "zero operators, successfully imported."
    mount_csv(
        &server,
        "/active-operators.csv",
        "License Number,Operator Name,City\n".to_string(),
    )
    .await;
    mount_csv(
        &server,
        "/all-operators.csv",
        "License Number,Operator Name,City\n".to_string(),
    )
    .await;

    let client = reqwest::Client::new();
    let page_url = format!("{}/kansas-oil-operators", server.uri());
    let outcome = run_operator_import(
        &pool,
        &client,
        &page_url,
        ImportOptions {
            retry_policy: fast_retry_policy(),
        },
    )
    .await
    .unwrap();

    assert_eq!(outcome.status, ImportStatus::Rejected);
    let failure_message: String =
        sqlx::query_scalar("SELECT failure_message FROM ks_ingestion_run WHERE id = $1")
            .bind(outcome.run_id)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert!(failure_message.contains("zero operator records"));
}

#[tokio::test]
async fn identical_payload_checksum_is_skipped_on_a_second_run() {
    let pool = test_pool().await;
    let server = MockServer::start().await;
    mount_page(&server, page_html_with_links()).await;
    mount_csv(
        &server,
        "/active-operators.csv",
        operator_csv(&[("9101", "Repeat Run Operating LLC", "Dodge City")]),
    )
    .await;
    mount_csv(
        &server,
        "/all-operators.csv",
        operator_csv(&[("9101", "Repeat Run Operating LLC", "Dodge City")]),
    )
    .await;

    let client = reqwest::Client::new();
    let page_url = format!("{}/kansas-oil-operators", server.uri());
    let options = ImportOptions {
        retry_policy: fast_retry_policy(),
    };

    let first = run_operator_import(&pool, &client, &page_url, options)
        .await
        .unwrap();
    assert_eq!(first.status, ImportStatus::Completed);

    let second = run_operator_import(&pool, &client, &page_url, options)
        .await
        .unwrap();
    assert_eq!(second.status, ImportStatus::UnchangedSkipped);
    assert_eq!(second.run_id, first.run_id);
}
