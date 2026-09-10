//! KS-06 acceptance criteria: fixture tests for the KCC drilling-intent
//! snapshot importer. Everything here runs against `wiremock` serving a
//! hand-built HTML results table — `kcc.ks.gov` is confirmed unreachable
//! from this development environment (see
//! docs/ops/ks-06-kcc-intents-unverified.md), so unlike KS-03/KS-04
//! there is no live-service smoke check to pair this with yet.

use future_wells_kansas::parse::kcc_intents::{run_intent_import, ImportOptions, ImportStatus};
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

fn intents_page_html(rows: &[(&str, &str, &str, &str)]) -> String {
    let mut body = String::from(
        "<html><body><table><tr><th>Permit Date</th><th>District</th><th>County</th><th>Operator License</th><th>Document</th></tr>",
    );
    for (date, district, county, license) in rows {
        body.push_str(&format!(
            "<tr><td>{date}</td><td>{district}</td><td>{county}</td><td>{license}</td><td><a href=\"/docs/{date}-{county}.pdf\">doc</a></td></tr>"
        ));
    }
    body.push_str("</table></body></html>");
    body
}

async fn mount_page(server: &MockServer, html: String) {
    Mock::given(method("GET"))
        .and(path("/notices-of-intents-search"))
        .respond_with(ResponseTemplate::new(200).set_body_string(html))
        .mount(server)
        .await;
}

#[tokio::test]
async fn full_import_parses_table_rows_and_stages_them() {
    let pool = test_pool().await;
    let server = MockServer::start().await;
    mount_page(
        &server,
        intents_page_html(&[
            ("01/15/2026", "1", "Barton", "K9001"),
            ("01/15/2026", "1", "Ellis", "K9002"),
        ]),
    )
    .await;

    let client = reqwest::Client::new();
    let url = format!("{}/notices-of-intents-search", server.uri());
    let outcome = run_intent_import(
        &pool,
        &client,
        &url,
        ImportOptions {
            retry_policy: fast_retry_policy(),
        },
    )
    .await
    .unwrap();

    assert_eq!(outcome.status, ImportStatus::Completed);
    assert_eq!(outcome.staged_count, 2);
    assert_eq!(outcome.rejected_count, 0);

    let staged: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM ks_intent_raw_snapshot WHERE ingestion_run_id = $1",
    )
    .bind(outcome.run_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(staged, 2);
}

#[tokio::test]
async fn zero_rows_rejects_the_run_as_a_zero_result_safeguard() {
    let pool = test_pool().await;
    let server = MockServer::start().await;
    mount_page(
        &server,
        "<html><body><table><tr><th>Permit Date</th></tr></table></body></html>".to_string(),
    )
    .await;

    let client = reqwest::Client::new();
    let url = format!("{}/notices-of-intents-search", server.uri());
    let outcome = run_intent_import(
        &pool,
        &client,
        &url,
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
    assert!(failure_message.contains("zero-result safeguard"));
}

#[tokio::test]
async fn no_table_found_rejects_the_run() {
    let pool = test_pool().await;
    let server = MockServer::start().await;
    mount_page(
        &server,
        "<html><body><p>Under maintenance</p></body></html>".to_string(),
    )
    .await;

    let client = reqwest::Client::new();
    let url = format!("{}/notices-of-intents-search", server.uri());
    let outcome = run_intent_import(
        &pool,
        &client,
        &url,
        ImportOptions {
            retry_policy: fast_retry_policy(),
        },
    )
    .await
    .unwrap();

    assert_eq!(outcome.status, ImportStatus::Rejected);
}

#[tokio::test]
async fn majority_unparseable_rows_rejects_the_run_as_a_partial_result_safeguard() {
    let pool = test_pool().await;
    let server = MockServer::start().await;
    // Rows with no <a> link at all can't yield a source_filename identity
    // — build a page where most rows lack one.
    let html = r#"
        <html><body><table>
            <tr><th>Permit Date</th><th>County</th></tr>
            <tr><td>01/15/2026</td><td>Barton</td></tr>
            <tr><td>01/16/2026</td><td>Ellis</td></tr>
            <tr><td>01/17/2026</td><td>Reno</td></tr>
        </table></body></html>
    "#;
    mount_page(&server, html.to_string()).await;

    let client = reqwest::Client::new();
    let url = format!("{}/notices-of-intents-search", server.uri());
    let outcome = run_intent_import(
        &pool,
        &client,
        &url,
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
    assert!(failure_message.contains("partial-result safeguard"));
}

#[tokio::test]
async fn identical_page_checksum_is_skipped_on_a_second_run() {
    let pool = test_pool().await;
    let server = MockServer::start().await;
    mount_page(
        &server,
        intents_page_html(&[("01/20/2026", "2", "Reno", "K9101")]),
    )
    .await;

    let client = reqwest::Client::new();
    let url = format!("{}/notices-of-intents-search", server.uri());
    let options = ImportOptions {
        retry_policy: fast_retry_policy(),
    };

    let first = run_intent_import(&pool, &client, &url, options)
        .await
        .unwrap();
    assert_eq!(first.status, ImportStatus::Completed);

    let second = run_intent_import(&pool, &client, &url, options)
        .await
        .unwrap();
    assert_eq!(second.status, ImportStatus::UnchangedSkipped);
    assert_eq!(second.run_id, first.run_id);
}
