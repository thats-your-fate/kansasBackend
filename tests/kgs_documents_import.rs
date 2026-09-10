//! KS-15 acceptance criteria: fixture tests for the KGS well-document
//! metadata importer. `chasm.kgs.ku.edu` (the real per-well "Qualified
//! Well Database" detail page host) is confirmed reachable and was
//! fetched directly during this task's discovery — see
//! `docs/ops/ks-15-kgs-documents-live-smoke.md` — but these automated
//! tests still run only against a `wiremock`-served fixture page, never
//! the live service.

use future_wells_kansas::parse::kgs_documents::{run_document_import, ImportStatus};
use sqlx::PgPool;
use wiremock::matchers::{method, query_param};
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

/// A real page, fetched directly during KS-15 discovery (KID 1001190703,
/// API 15-009-03905, 2026-09-05) — trimmed to the sections the parser
/// reads. `kid` is embedded in the WDL link exactly as the real page
/// does (`?whk=<KID>`) — real pages always differ per well, so a test
/// exercising two different wells must too, or a page-content checksum
/// collision (this repo's real idempotency mechanism) would conflate
/// them the same way a byte-identical real page never actually would.
fn real_well_page_fixture(kid: &str) -> String {
    format!(
        r#"
    <html><body>
    <table align="center" border="1"><tr valign="top"><td>
    <strong>API: </strong>15-009-03905
    </td></tr>
    <tr><td colspan="2">
    <a href="https://www.kgslibrary.com/index.cfm?whk={kid}" target="_BLANK">View well in the Walter's Digital Library (WDL login required)</a>
    </td></tr>
    </table>
    <h3 id="tops">Tops Data</h3>
    <table border="1" align="center">
    <tr><th>Formation<br>(source name)</th><th>Top</th><th>Base</th><th>Source</th><th>Updated</th></tr>
    <tr><td>Stone Corral Formation<br>(STNC)</td><td>730</td><td>&nbsp;</td><td>Completion Report</td><td>Apr-15-1999</td></tr>
    </table>
    </body></html>
    "#
    )
}

#[tokio::test]
async fn full_import_stages_wdl_reference_and_tops_data_citation() {
    let pool = test_pool().await;
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(query_param("f_kid", "1001190703"))
        .respond_with(
            ResponseTemplate::new(200).set_body_string(real_well_page_fixture("1001190703")),
        )
        .mount(&server)
        .await;

    let client = reqwest::Client::new();
    let outcome = run_document_import(
        &pool,
        &client,
        &server.uri(),
        "1001190703",
        Some("15-009-03905"),
    )
    .await
    .unwrap();

    assert_eq!(outcome.status, ImportStatus::Completed);
    assert_eq!(
        outcome.staged_count, 2,
        "the WDL reference plus the one deduped tops-data citation"
    );

    let staged: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM ks_well_document_raw_snapshot WHERE ingestion_run_id = $1",
    )
    .bind(outcome.run_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(staged, 2);
}

#[tokio::test]
async fn identical_page_content_is_skipped_on_a_second_fetch() {
    let pool = test_pool().await;
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(query_param("f_kid", "1001190704"))
        .respond_with(
            ResponseTemplate::new(200).set_body_string(real_well_page_fixture("1001190704")),
        )
        .mount(&server)
        .await;

    let client = reqwest::Client::new();
    let first = run_document_import(&pool, &client, &server.uri(), "1001190704", None)
        .await
        .unwrap();
    assert_eq!(first.status, ImportStatus::Completed);

    let second = run_document_import(&pool, &client, &server.uri(), "1001190704", None)
        .await
        .unwrap();
    assert_eq!(second.status, ImportStatus::SkippedIdenticalChecksum);
    assert_eq!(second.staged_count, 0);
}

#[tokio::test]
async fn a_page_with_no_documents_stages_nothing_but_still_completes() {
    let pool = test_pool().await;
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(query_param("f_kid", "1001190705"))
        .respond_with(
            ResponseTemplate::new(200).set_body_string("<html><body>No data</body></html>"),
        )
        .mount(&server)
        .await;

    let client = reqwest::Client::new();
    let outcome = run_document_import(&pool, &client, &server.uri(), "1001190705", None)
        .await
        .unwrap();
    assert_eq!(outcome.status, ImportStatus::Completed);
    assert_eq!(outcome.staged_count, 0);
}
