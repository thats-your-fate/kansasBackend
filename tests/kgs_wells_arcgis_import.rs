//! KS-03 acceptance criteria: "fixture tests for ArcGIS pagination,
//! retries and schema changes." Everything here runs against a
//! `wiremock` server, never the live KGS service — see
//! `docs/ops/ks-03-kgs-wells-live-smoke.md` for the separate, manual,
//! `#[ignore]`d live-service check.

use future_wells_kansas::parse::kgs_wells::{
    run_arcgis_snapshot_import, ImportOptions, ImportStatus,
};
use future_wells_kansas::source::retry::RetryPolicy;
use serde_json::json;
use sqlx::PgPool;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;
use wiremock::matchers::{method, path, query_param};
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

fn sample_metadata(field_names: &[&str], data_last_edit_date: i64) -> serde_json::Value {
    json!({
        "maxRecordCount": 2,
        "editingInfo": {"dataLastEditDate": data_last_edit_date, "schemaLastEditDate": data_last_edit_date},
        "fields": field_names.iter().map(|n| json!({"name": n, "type": "esriFieldTypeString"})).collect::<Vec<_>>(),
    })
}

fn sample_feature(
    object_id: i64,
    api_num_nodash: &str,
    county: &str,
    x: f64,
    y: f64,
) -> serde_json::Value {
    json!({
        "attributes": {
            "OBJECTID": object_id,
            "KID": 1000000 + object_id,
            "API_NUM_NODASH": api_num_nodash,
            "COUNTY": county,
        },
        "geometry": {"x": x, "y": y},
    })
}

async fn mount_metadata(
    server: &MockServer,
    field_names: &'static [&'static str],
    data_last_edit_date: i64,
) {
    Mock::given(method("GET"))
        .and(path("/0"))
        .and(query_param("f", "json"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(sample_metadata(field_names, data_last_edit_date)),
        )
        .mount(server)
        .await;
}

async fn mount_count(server: &MockServer, count: i64) {
    Mock::given(method("GET"))
        .and(path("/0/query"))
        .and(query_param("returnCountOnly", "true"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"count": count})))
        .mount(server)
        .await;
}

const KS_ATTRS: (&str, &str, f64, f64) = ("15059232820000", "Franklin", -95.2327, 38.4460);

#[tokio::test]
async fn full_import_with_paginated_object_ids_completes_and_stages_rows() {
    let server = MockServer::start().await;
    mount_metadata(
        &server,
        &["OBJECTID", "KID", "API_NUM_NODASH", "COUNTY"],
        1000,
    )
    .await;
    mount_count(&server, 3).await;

    // Object-ID pagination: first page (no cursor) returns [1, 2] with
    // exceededTransferLimit=true; second page (OBJECTID>2) returns [3]
    // with no truncation flag — exercises the cursor-pagination path this
    // layer actually requires (see arcgis::fetch_object_ids_page's doc
    // comment).
    Mock::given(method("GET"))
        .and(path("/0/query"))
        .and(query_param("returnIdsOnly", "true"))
        .and(query_param("where", "1=1"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "objectIdFieldName": "OBJECTID", "objectIds": [1, 2], "exceededTransferLimit": true
        })))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/0/query"))
        .and(query_param("returnIdsOnly", "true"))
        .and(query_param("where", "OBJECTID>2"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "objectIdFieldName": "OBJECTID", "objectIds": [3], "exceededTransferLimit": false
        })))
        .mount(&server)
        .await;

    // maxRecordCount is 2, so object IDs [1,2,3] split into batches [1,2]
    // and [3].
    Mock::given(method("GET"))
        .and(path("/0/query"))
        .and(query_param("objectIds", "1,2"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "features": [
                sample_feature(1, KS_ATTRS.0, KS_ATTRS.1, KS_ATTRS.2, KS_ATTRS.3),
                sample_feature(2, "35000000000000", "Comanche", -97.5, 35.4),
            ]
        })))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/0/query"))
        .and(query_param("objectIds", "3"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "features": [sample_feature(3, KS_ATTRS.0, KS_ATTRS.1, KS_ATTRS.2, KS_ATTRS.3)]
        })))
        .mount(&server)
        .await;

    let pool = test_pool().await;
    let client = reqwest::Client::new();
    let layer_url = format!("{}/0", server.uri());

    let outcome = run_arcgis_snapshot_import(
        &pool,
        &client,
        &layer_url,
        ImportOptions {
            count_tolerance_ratio: 0.5,
            retry_policy: fast_retry_policy(),
            limit_object_ids: None,
        },
    )
    .await
    .expect("import should succeed");

    assert_eq!(outcome.status, ImportStatus::Completed);
    assert_eq!(outcome.included_count, 2);
    assert_eq!(outcome.excluded_count, 1);

    let staged_count: i64 =
        sqlx::query_scalar("SELECT count(*) FROM ks_well_raw_snapshot WHERE ingestion_run_id = $1")
            .bind(outcome.run_id)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(
        staged_count, 3,
        "all 3 object IDs across both pages must be staged"
    );

    let excluded_reason: Option<String> = sqlx::query_scalar(
        "SELECT kansas_membership_reason FROM ks_well_raw_snapshot WHERE ingestion_run_id = $1 AND kansas_membership_status = 'excluded_other_state'",
    )
    .bind(outcome.run_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert!(excluded_reason.is_some());
}

#[tokio::test]
async fn transient_batch_failure_is_retried_and_then_succeeds() {
    let server = MockServer::start().await;
    mount_metadata(
        &server,
        &["OBJECTID", "KID", "API_NUM_NODASH", "COUNTY"],
        2000,
    )
    .await;
    mount_count(&server, 1).await;

    Mock::given(method("GET"))
        .and(path("/0/query"))
        .and(query_param("returnIdsOnly", "true"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "objectIdFieldName": "OBJECTID", "objectIds": [101], "exceededTransferLimit": false
        })))
        .mount(&server)
        .await;

    // First attempt at fetching the feature batch fails with a 500;
    // the importer must retry rather than giving up immediately.
    let attempt_count = std::sync::Arc::new(AtomicU32::new(0));
    let counter = attempt_count.clone();
    Mock::given(method("GET"))
        .and(path("/0/query"))
        .and(query_param("objectIds", "101"))
        .respond_with(move |_: &wiremock::Request| {
            let n = counter.fetch_add(1, Ordering::SeqCst);
            if n == 0 {
                ResponseTemplate::new(500)
            } else {
                ResponseTemplate::new(200).set_body_json(json!({
                    "features": [sample_feature(101, KS_ATTRS.0, KS_ATTRS.1, KS_ATTRS.2, KS_ATTRS.3)]
                }))
            }
        })
        .mount(&server)
        .await;

    let pool = test_pool().await;
    let client = reqwest::Client::new();
    let layer_url = format!("{}/0", server.uri());

    let outcome = run_arcgis_snapshot_import(
        &pool,
        &client,
        &layer_url,
        ImportOptions {
            count_tolerance_ratio: 0.5,
            retry_policy: fast_retry_policy(),
            limit_object_ids: None,
        },
    )
    .await
    .expect("import should succeed after retrying the failed batch");

    assert_eq!(outcome.status, ImportStatus::Completed);
    assert_eq!(outcome.included_count, 1);
    assert!(
        attempt_count.load(Ordering::SeqCst) >= 2,
        "the batch fetch must have been retried at least once"
    );
}

#[tokio::test]
async fn source_schema_change_during_import_rejects_the_run() {
    let server = MockServer::start().await;
    mount_count(&server, 1).await;

    // Metadata differs between the "before" and "after" reads (a field
    // was added) — the importer re-reads metadata after extraction and
    // must detect this rather than silently publishing.
    let call_count = std::sync::Arc::new(AtomicU32::new(0));
    let counter = call_count.clone();
    Mock::given(method("GET"))
        .and(path("/0"))
        .and(query_param("f", "json"))
        .respond_with(move |_: &wiremock::Request| {
            let n = counter.fetch_add(1, Ordering::SeqCst);
            let fields: &[&str] = if n == 0 {
                &["OBJECTID", "KID", "API_NUM_NODASH", "COUNTY"]
            } else {
                &["OBJECTID", "KID", "API_NUM_NODASH", "COUNTY", "NEW_FIELD"]
            };
            ResponseTemplate::new(200).set_body_json(sample_metadata(fields, 1000))
        })
        .mount(&server)
        .await;

    Mock::given(method("GET"))
        .and(path("/0/query"))
        .and(query_param("returnIdsOnly", "true"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "objectIdFieldName": "OBJECTID", "objectIds": [201], "exceededTransferLimit": false
        })))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/0/query"))
        .and(query_param("objectIds", "201"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "features": [sample_feature(201, KS_ATTRS.0, KS_ATTRS.1, KS_ATTRS.2, KS_ATTRS.3)]
        })))
        .mount(&server)
        .await;

    let pool = test_pool().await;
    let client = reqwest::Client::new();
    let layer_url = format!("{}/0", server.uri());

    let outcome = run_arcgis_snapshot_import(
        &pool,
        &client,
        &layer_url,
        ImportOptions {
            count_tolerance_ratio: 0.5,
            retry_policy: fast_retry_policy(),
            limit_object_ids: None,
        },
    )
    .await
    .expect("the function returns Ok with a Rejected status, not an Err, for a QA-gate failure");

    assert_eq!(outcome.status, ImportStatus::Rejected);
    let status: String = sqlx::query_scalar("SELECT status FROM ks_ingestion_run WHERE id = $1")
        .bind(outcome.run_id)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(status, "rejected");
}

#[tokio::test]
async fn extracted_count_outside_tolerance_rejects_the_run() {
    let server = MockServer::start().await;
    mount_metadata(
        &server,
        &["OBJECTID", "KID", "API_NUM_NODASH", "COUNTY"],
        1000,
    )
    .await;
    // The layer claims 100 records exist, but only 1 object ID is ever
    // returned — far outside any reasonable tolerance.
    mount_count(&server, 100).await;

    Mock::given(method("GET"))
        .and(path("/0/query"))
        .and(query_param("returnIdsOnly", "true"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "objectIdFieldName": "OBJECTID", "objectIds": [301], "exceededTransferLimit": false
        })))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/0/query"))
        .and(query_param("objectIds", "301"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "features": [sample_feature(301, KS_ATTRS.0, KS_ATTRS.1, KS_ATTRS.2, KS_ATTRS.3)]
        })))
        .mount(&server)
        .await;

    let pool = test_pool().await;
    let client = reqwest::Client::new();
    let layer_url = format!("{}/0", server.uri());

    let outcome = run_arcgis_snapshot_import(
        &pool,
        &client,
        &layer_url,
        ImportOptions {
            count_tolerance_ratio: 0.02,
            retry_policy: fast_retry_policy(),
            limit_object_ids: None,
        },
    )
    .await
    .unwrap();

    assert_eq!(outcome.status, ImportStatus::Rejected);
}

#[tokio::test]
async fn identical_object_id_snapshot_is_skipped_on_a_second_run() {
    let server = MockServer::start().await;
    mount_metadata(
        &server,
        &["OBJECTID", "KID", "API_NUM_NODASH", "COUNTY"],
        1000,
    )
    .await;
    mount_count(&server, 1).await;

    Mock::given(method("GET"))
        .and(path("/0/query"))
        .and(query_param("returnIdsOnly", "true"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "objectIdFieldName": "OBJECTID", "objectIds": [401], "exceededTransferLimit": false
        })))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/0/query"))
        .and(query_param("objectIds", "401"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "features": [sample_feature(401, KS_ATTRS.0, KS_ATTRS.1, KS_ATTRS.2, KS_ATTRS.3)]
        })))
        .mount(&server)
        .await;

    let pool = test_pool().await;
    let client = reqwest::Client::new();
    let layer_url = format!("{}/0", server.uri());
    let options = || ImportOptions {
        count_tolerance_ratio: 0.5,
        retry_policy: fast_retry_policy(),
        limit_object_ids: None,
    };

    let first = run_arcgis_snapshot_import(&pool, &client, &layer_url, options())
        .await
        .unwrap();
    assert_eq!(first.status, ImportStatus::Completed);

    let second = run_arcgis_snapshot_import(&pool, &client, &layer_url, options())
        .await
        .unwrap();
    assert_eq!(second.status, ImportStatus::UnchangedSkipped);
    assert_eq!(
        second.run_id, first.run_id,
        "the skip must resolve to the earlier completed run"
    );
}
