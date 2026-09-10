//! KS-03: the ZIP fallback path. Builds a small real ZIP fixture (same
//! shape verified live against KGS_WELLS_STATIC_ZIP at KS-03: a single
//! `ks_wells.txt` quoted-CSV entry) and serves it from a local wiremock
//! server — never downloads the real 47MB file in tests.

use future_wells_kansas::parse::kgs_wells::{run_zip_snapshot_import, ImportStatus};
use future_wells_kansas::source::retry::RetryPolicy;
use sqlx::PgPool;
use std::io::Write;
use std::time::Duration;
use wiremock::matchers::method;
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

/// `test_id` distinguishes each test's fixture bytes (embedded in the
/// lease name) so parallel-running tests never produce byte-identical ZIP
/// content — otherwise two tests using this fixture would race on the
/// same (source_id, payload_checksum) idempotency key, which is a real
/// concurrency property being tested deliberately in
/// `identical_zip_checksum_is_skipped_on_a_second_run`, not something to
/// trigger accidentally between unrelated tests.
fn build_fixture_zip(test_id: &str) -> Vec<u8> {
    let csv_body = format!(
        concat!(
            "\"KID\",\"API_NUMBER\",\"API_NUM_NODASH\",\"LEASE\",\"WELL\",\"FIELD\",\"LATITUDE\",\"LONGITUDE\",",
            "\"LONG_LAT_SOURCE\",\"ORIG_OPERATOR\",\"CURR_OPERATOR\",\"STATUS\",\"STATUS2\"\n",
            "\"1001184201\",\"15-007-20008\",\"15007200080000\",\"Kirkbride 'B' {test_id}\",\"1\",\"MEDICINE LODGE NORTH\",",
            "\"37.2378121\",\"-98.6341308\",\"Calc. from footages\",\"Petroleum, Inc.\",\"unavailable\",\"D&A\",\"Plugged and Abandoned\"\n",
            "\"1001184206\",\"15-999-99999\",\"35999999990000\",\"OUT OF STATE\",\"1\",\"UNKNOWN\",",
            "\"35.0\",\"-97.0\",\"Calc. from footages\",\"Someone\",\"unavailable\",\"LOC\",\"Active\"\n",
        ),
        test_id = test_id,
    );

    let mut buffer = Vec::new();
    {
        let cursor = std::io::Cursor::new(&mut buffer);
        let mut writer = zip::ZipWriter::new(cursor);
        writer
            .start_file::<_, ()>("ks_wells.txt", zip::write::FileOptions::default())
            .unwrap();
        writer.write_all(csv_body.as_bytes()).unwrap();
        writer.finish().unwrap();
    }
    buffer
}

fn fast_retry_policy() -> RetryPolicy {
    RetryPolicy {
        max_attempts: 3,
        base_delay: Duration::from_millis(5),
    }
}

#[tokio::test]
async fn zip_fallback_import_stages_and_classifies_rows() {
    let server = MockServer::start().await;
    let zip_bytes = build_fixture_zip("stages_and_classifies");

    Mock::given(method("GET"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_bytes(zip_bytes)
                .insert_header("last-modified", "Fri, 07 Aug 2026 22:17:17 GMT")
                .insert_header("content-type", "application/zip"),
        )
        .mount(&server)
        .await;

    let pool = test_pool().await;
    let client = reqwest::Client::new();
    let dest =
        std::env::temp_dir().join(format!("ks-wells-zip-fixture-{}.zip", uuid::Uuid::new_v4()));

    let outcome =
        run_zip_snapshot_import(&pool, &client, &server.uri(), &dest, fast_retry_policy())
            .await
            .expect("zip import should succeed");

    assert_eq!(outcome.status, ImportStatus::Completed);
    assert_eq!(
        outcome.included_count, 1,
        "the 15-prefixed API row must be included"
    );
    assert_eq!(
        outcome.excluded_count, 1,
        "the 35-prefixed API row with no county field must be excluded"
    );

    let rows: Vec<(String, Option<String>)> = sqlx::query_as(
        "SELECT source_format, kansas_membership_reason FROM ks_well_raw_snapshot WHERE ingestion_run_id = $1 ORDER BY kansas_membership_status",
    )
    .bind(outcome.run_id)
    .fetch_all(&pool)
    .await
    .unwrap();
    assert_eq!(rows.len(), 2);
    assert!(rows.iter().all(|(format, _)| format == "zip"));
    assert!(rows.iter().any(|(_, reason)| reason.is_some()));

    let _ = std::fs::remove_file(&dest);
}

#[tokio::test]
async fn identical_zip_checksum_is_skipped_on_a_second_run() {
    let server = MockServer::start().await;
    let zip_bytes = build_fixture_zip("identical_checksum_skip");

    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(zip_bytes))
        .mount(&server)
        .await;

    let pool = test_pool().await;
    let client = reqwest::Client::new();
    let dest =
        std::env::temp_dir().join(format!("ks-wells-zip-fixture-{}.zip", uuid::Uuid::new_v4()));

    let first = run_zip_snapshot_import(&pool, &client, &server.uri(), &dest, fast_retry_policy())
        .await
        .unwrap();
    assert_eq!(first.status, ImportStatus::Completed);

    let second = run_zip_snapshot_import(&pool, &client, &server.uri(), &dest, fast_retry_policy())
        .await
        .unwrap();
    assert_eq!(second.status, ImportStatus::UnchangedSkipped);
    assert_eq!(second.run_id, first.run_id);

    let _ = std::fs::remove_file(&dest);
}
