//! KS-09 acceptance criteria: fixture tests for the KGS all-leases
//! archive importer. Confirmed live 2026-09-05 against the real
//! `KGS_ALL_LEASES_ZIP` (98,639 real rows staged successfully) — see
//! `docs/ops/ks-09-kgs-leases-live-smoke.md`. These automated tests still
//! run only against a small hand-built fixture ZIP, never the live
//! service, mirroring `tests/kgs_wells_zip_import.rs`.

use future_wells_kansas::parse::kgs_leases::{run_lease_import, ImportStatus};
use future_wells_kansas::source::retry::RetryPolicy;
use sqlx::PgPool;
use std::io::Write;
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

/// Each test passes rows with content unique to that test (distinct
/// LEASE_KID/name/etc.), which is enough to keep ZIP checksums distinct
/// across tests without a separate marker row — same lesson as
/// `tests/kgs_wells_zip_import.rs`'s `build_fixture_zip`, applied more
/// simply since every real column here is already test-specific.
fn build_fixture_zip(rows: &[&str]) -> Vec<u8> {
    let header = "\"LEASE_KID\",\"LEASE\",\"DOR_CODE\",\"API_NUMBER\",\"OPERATOR\",\"COUNTY\",\"TOWNSHIP\",\"TWN_DIR\",\"RANGE\",\"RANGE_DIR\",\"SECTION\",\"SPOT\",\"LATITUDE\",\"LONGITUDE\",\"PRODUCES\",\"PRODUCTION\",\"YEAR_START\",\"YEAR_STOP\",\"URL\"\r\n";
    let mut body = header.to_string();
    for row in rows {
        body.push_str(row);
        body.push_str("\r\n");
    }

    let mut buf = Vec::new();
    {
        let mut zip = zip::ZipWriter::new(std::io::Cursor::new(&mut buf));
        zip.start_file(
            "all_kansas_leases.txt",
            zip::write::SimpleFileOptions::default(),
        )
        .unwrap();
        zip.write_all(body.as_bytes()).unwrap();
        zip.finish().unwrap();
    }
    buf
}

async fn mount_zip(server: &MockServer, bytes: Vec<u8>) {
    Mock::given(method("GET"))
        .and(path("/all_kansas_leases.zip"))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(bytes))
        .mount(server)
        .await;
}

#[tokio::test]
async fn full_import_stages_lease_rows() {
    let pool = test_pool().await;
    let server = MockServer::start().await;
    let rows = [
        r#""9001","IMPORT TEST LEASE A","111111","15-009-11111","Test Operating LLC","Barton","20","S","10","W","4","","38.1","-98.1","OIL","100","2000","2020","https://example/9001""#,
    ];
    mount_zip(&server, build_fixture_zip(&rows)).await;

    let client = reqwest::Client::new();
    let url = format!("{}/all_kansas_leases.zip", server.uri());
    let dest = std::env::temp_dir().join(format!("ks_lease_test_{}.zip", uuid::Uuid::new_v4()));
    let outcome = run_lease_import(&pool, &client, &url, &dest, fast_retry_policy())
        .await
        .unwrap();

    assert_eq!(outcome.status, ImportStatus::Completed);
    assert_eq!(outcome.staged_count, 1);
    assert_eq!(outcome.rejected_count, 0);

    let staged: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM ks_lease_raw_snapshot WHERE ingestion_run_id = $1",
    )
    .bind(outcome.run_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(staged, 1);
}

#[tokio::test]
async fn identical_zip_checksum_is_skipped_on_a_second_run() {
    let pool = test_pool().await;
    let server = MockServer::start().await;
    let rows = [
        r#""9101","REPEAT RUN LEASE","222222","15-009-22222","Test Operating LLC","Ellis","15","S","20","W","7","","38.2","-99.2","GAS","500","2010","2026","https://example/9101""#,
    ];
    mount_zip(&server, build_fixture_zip(&rows)).await;

    let client = reqwest::Client::new();
    let url = format!("{}/all_kansas_leases.zip", server.uri());
    let dest = std::env::temp_dir().join(format!("ks_lease_test_{}.zip", uuid::Uuid::new_v4()));

    let first = run_lease_import(&pool, &client, &url, &dest, fast_retry_policy())
        .await
        .unwrap();
    assert_eq!(first.status, ImportStatus::Completed);

    let dest2 = std::env::temp_dir().join(format!("ks_lease_test_{}.zip", uuid::Uuid::new_v4()));
    let second = run_lease_import(&pool, &client, &url, &dest2, fast_retry_policy())
        .await
        .unwrap();
    assert_eq!(second.status, ImportStatus::UnchangedSkipped);
    assert_eq!(second.run_id, first.run_id);
}
