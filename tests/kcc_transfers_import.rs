//! KS-07 acceptance criteria: fixture tests for the KCC approved-transfer
//! snapshot importer. Same fixture-only discipline as
//! `tests/kcc_intents_import.rs` — `kcc.ks.gov` is unreachable from this
//! development environment (see docs/ops/ks-07-kcc-transfers-unverified.md).

use future_wells_kansas::parse::kcc_transfers::{run_transfer_import, ImportOptions, ImportStatus};
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

fn transfers_page_html(rows: &[(&str, &str, &str, &str)]) -> String {
    let mut body = String::from(
        "<html><body><table><tr><th>Past License</th><th>New License</th><th>Effective Date</th><th>Lease</th></tr>",
    );
    for (past, new, effective, lease) in rows {
        body.push_str(&format!(
            "<tr><td>{past}</td><td>{new}</td><td>{effective}</td><td>{lease}</td></tr>"
        ));
    }
    body.push_str("</table></body></html>");
    body
}

async fn mount_page(server: &MockServer, html: String) {
    Mock::given(method("GET"))
        .and(path("/transfersSearch.php"))
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
        transfers_page_html(&[
            ("K1001", "K2001", "02/01/2026", "Import Test Lease A"),
            ("K1002", "K2002", "02/02/2026", "Import Test Lease B"),
        ]),
    )
    .await;

    let client = reqwest::Client::new();
    let url = format!("{}/transfersSearch.php", server.uri());
    let outcome = run_transfer_import(
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

    let staged: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM ks_transfer_raw_snapshot WHERE ingestion_run_id = $1",
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
        "<html><body><table><tr><th>Past License</th></tr></table></body></html>".to_string(),
    )
    .await;

    let client = reqwest::Client::new();
    let url = format!("{}/transfersSearch.php", server.uri());
    let outcome = run_transfer_import(
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
async fn majority_missing_both_licenses_rejects_the_run_as_a_partial_result_safeguard() {
    let pool = test_pool().await;
    let server = MockServer::start().await;
    let html = r#"
        <html><body><table>
            <tr><th>Lease</th><th>County</th></tr>
            <tr><td>Some Lease</td><td>Barton</td></tr>
            <tr><td>Another Lease</td><td>Ellis</td></tr>
        </table></body></html>
    "#;
    mount_page(&server, html.to_string()).await;

    let client = reqwest::Client::new();
    let url = format!("{}/transfersSearch.php", server.uri());
    let outcome = run_transfer_import(
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
        transfers_page_html(&[("K3001", "K4001", "03/01/2026", "Repeat Run Lease")]),
    )
    .await;

    let client = reqwest::Client::new();
    let url = format!("{}/transfersSearch.php", server.uri());
    let options = ImportOptions {
        retry_policy: fast_retry_policy(),
    };

    let first = run_transfer_import(&pool, &client, &url, options)
        .await
        .unwrap();
    assert_eq!(first.status, ImportStatus::Completed);

    let second = run_transfer_import(&pool, &client, &url, options)
        .await
        .unwrap();
    assert_eq!(second.status, ImportStatus::UnchangedSkipped);
    assert_eq!(second.run_id, first.run_id);
}
