//! KS-08 acceptance criteria: fixture tests for the KGS field importer.
//! Everything here runs against `wiremock`, never the live service — see
//! `docs/ops/ks-08-kgs-fields-live-smoke.md` for the separate, manual,
//! live-service check (KGS, unlike kcc.ks.gov, is confirmed reachable —
//! this importer was built against real live data, but its automated
//! tests still don't touch the network).

use serde_json::json;
use sqlx::PgPool;
use std::io::Write;
use std::time::Duration;
use wiremock::matchers::{method, path, query_param};
use wiremock::{Mock, MockServer, ResponseTemplate};

use future_wells_kansas::parse::kgs_fields::{run_field_import, ImportOptions, ImportStatus};
use future_wells_kansas::source::retry::RetryPolicy;

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

async fn mount_metadata(
    server: &MockServer,
    field_names: &'static [&'static str],
    data_last_edit_date: i64,
) {
    Mock::given(method("GET"))
        .and(path("/0"))
        .and(query_param("f", "json"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "maxRecordCount": 2000,
            "editingInfo": {"dataLastEditDate": data_last_edit_date, "schemaLastEditDate": data_last_edit_date},
            "fields": field_names.iter().map(|n| json!({"name": n, "type": "esriFieldTypeString"})).collect::<Vec<_>>(),
        })))
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

async fn mount_object_ids(server: &MockServer, ids: &[i64]) {
    Mock::given(method("GET"))
        .and(path("/0/query"))
        .and(query_param("returnIdsOnly", "true"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(json!({"objectIds": ids, "exceededTransferLimit": false})),
        )
        .mount(server)
        .await;
}

async fn mount_geojson_features(server: &MockServer, features: Vec<serde_json::Value>) {
    Mock::given(method("GET"))
        .and(path("/0/query"))
        .and(query_param("f", "geojson"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "type": "FeatureCollection",
            "features": features,
        })))
        .mount(server)
        .await;
}

fn geojson_field_feature(
    object_id: i64,
    field_kid: &str,
    field_name: &str,
    status: &str,
) -> serde_json::Value {
    json!({
        "type": "Feature",
        "properties": {
            "OBJECTID": object_id,
            "FIELD_KID": field_kid,
            "FIELD_NAME": field_name,
            "STATUS": status,
            "FIELD_TYPE": "Oil",
            "FORMATIONS": "Mississippian",
            "APPROXACRE": 640,
        },
        "geometry": {
            "type": "Polygon",
            "coordinates": [[[-98.0, 38.0], [-98.0, 38.1], [-97.9, 38.1], [-97.9, 38.0], [-98.0, 38.0]]],
        },
    })
}

/// Local copy of a minimal-valid-xlsx builder (integration tests can't
/// see the lib's own `#[cfg(test)]` helper — same precedent as
/// `tests/kgs_wells_zip_import.rs`'s local `build_fixture_zip`).
fn build_fixture_xlsx(headers: &[&str], rows: &[Vec<&str>]) -> Vec<u8> {
    fn column_letter(mut i: usize) -> String {
        let mut s = String::new();
        loop {
            s.insert(0, (b'A' + (i % 26) as u8) as char);
            if i < 26 {
                break;
            }
            i = i / 26 - 1;
        }
        s
    }
    fn row_xml(row_num: usize, cells: &[String]) -> String {
        let mut xml = format!(r#"<row r="{row_num}">"#);
        for (i, value) in cells.iter().enumerate() {
            let col = column_letter(i);
            let escaped = value
                .replace('&', "&amp;")
                .replace('<', "&lt;")
                .replace('>', "&gt;");
            xml.push_str(&format!(
                r#"<c r="{col}{row_num}" t="inlineStr"><is><t>{escaped}</t></is></c>"#
            ));
        }
        xml.push_str("</row>");
        xml
    }

    let mut sheet_rows = row_xml(
        1,
        &headers.iter().map(|s| s.to_string()).collect::<Vec<_>>(),
    );
    for (i, row) in rows.iter().enumerate() {
        sheet_rows.push_str(&row_xml(
            i + 2,
            &row.iter().map(|s| s.to_string()).collect::<Vec<_>>(),
        ));
    }
    let sheet_xml = format!(
        r#"<?xml version="1.0" encoding="UTF-8" standalone="yes"?><worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main"><sheetData>{sheet_rows}</sheetData></worksheet>"#
    );
    let content_types = r#"<?xml version="1.0" encoding="UTF-8" standalone="yes"?><Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types"><Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/><Default Extension="xml" ContentType="application/xml"/><Override PartName="/xl/workbook.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/><Override PartName="/xl/worksheets/sheet1.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/></Types>"#;
    let root_rels = r#"<?xml version="1.0" encoding="UTF-8" standalone="yes"?><Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships"><Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" Target="xl/workbook.xml"/></Relationships>"#;
    let workbook_xml = r#"<?xml version="1.0" encoding="UTF-8" standalone="yes"?><workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships"><sheets><sheet name="Sheet1" sheetId="1" r:id="rId1"/></sheets></workbook>"#;
    let workbook_rels = r#"<?xml version="1.0" encoding="UTF-8" standalone="yes"?><Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships"><Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" Target="worksheets/sheet1.xml"/></Relationships>"#;

    let mut buf = Vec::new();
    {
        let mut zip = zip::ZipWriter::new(std::io::Cursor::new(&mut buf));
        let options = zip::write::SimpleFileOptions::default();
        zip.start_file("[Content_Types].xml", options).unwrap();
        zip.write_all(content_types.as_bytes()).unwrap();
        zip.start_file("_rels/.rels", options).unwrap();
        zip.write_all(root_rels.as_bytes()).unwrap();
        zip.start_file("xl/workbook.xml", options).unwrap();
        zip.write_all(workbook_xml.as_bytes()).unwrap();
        zip.start_file("xl/_rels/workbook.xml.rels", options)
            .unwrap();
        zip.write_all(workbook_rels.as_bytes()).unwrap();
        zip.start_file("xl/worksheets/sheet1.xml", options).unwrap();
        zip.write_all(sheet_xml.as_bytes()).unwrap();
        zip.finish().unwrap();
    }
    buf
}

async fn mount_xlsx(server: &MockServer, route_path: &str, bytes: Vec<u8>) {
    Mock::given(method("GET"))
        .and(path(route_path))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(bytes))
        .mount(server)
        .await;
}

fn empty_xlsx() -> Vec<u8> {
    build_fixture_xlsx(
        &[
            "FIELD_NAME",
            "PROD_ZONES",
            "TOTAL_OIL_PROD",
            "TOTAL_GAS_PROD",
        ],
        &[],
    )
}

#[tokio::test]
async fn full_import_stages_arcgis_and_file_rows() {
    let pool = test_pool().await;
    let server = MockServer::start().await;
    mount_metadata(
        &server,
        &["OBJECTID", "FIELD_KID", "FIELD_NAME", "STATUS"],
        1000,
    )
    .await;
    mount_count(&server, 2).await;
    mount_object_ids(&server, &[1, 2]).await;
    mount_geojson_features(
        &server,
        vec![
            geojson_field_feature(1, "9001", "TEST FIELD IMPORT A", "Active"),
            geojson_field_feature(2, "9002", "TEST FIELD IMPORT B", "Abandoned"),
        ],
    )
    .await;

    let active_file_urls = vec![format!("{}/active1.xlsx", server.uri())];
    mount_xlsx(
        &server,
        "/active1.xlsx",
        build_fixture_xlsx(
            &[
                "FIELD_NAME",
                "PROD_ZONES",
                "TOTAL_OIL_PROD",
                "TOTAL_GAS_PROD",
                "2020_OIL_PROD",
                "2020_OIL_WELLS",
            ],
            &[vec![
                "TEST FIELD IMPORT A",
                "MISSISSIPPIAN",
                "500",
                "0",
                "50",
                "3",
            ]],
        ),
    )
    .await;
    mount_xlsx(&server, "/abandoned.xlsx", empty_xlsx()).await;

    let client = reqwest::Client::new();
    let arcgis_layer_url = format!("{}/0", server.uri());
    let abandoned_url = format!("{}/abandoned.xlsx", server.uri());
    let options = ImportOptions {
        retry_policy: fast_retry_policy(),
        count_tolerance_ratio: 0.02,
        limit_object_ids: None,
    };

    let outcome = run_field_import(
        &pool,
        &client,
        &arcgis_layer_url,
        &active_file_urls,
        &abandoned_url,
        options,
    )
    .await
    .unwrap();

    assert_eq!(outcome.status, ImportStatus::Completed);
    assert_eq!(outcome.arcgis_count, 2);
    assert_eq!(outcome.active_file_row_count, 1);
    assert_eq!(outcome.abandoned_file_row_count, 0);

    let staged: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM ks_field_raw_snapshot WHERE ingestion_run_id = $1",
    )
    .bind(outcome.run_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(staged, 3);
}

#[tokio::test]
async fn source_schema_change_during_import_rejects_the_run() {
    let pool = test_pool().await;
    let server = MockServer::start().await;

    let call_count = std::sync::Arc::new(std::sync::atomic::AtomicU32::new(0));
    let call_count_clone = call_count.clone();
    Mock::given(method("GET"))
        .and(path("/0"))
        .and(query_param("f", "json"))
        .respond_with(move |_: &wiremock::Request| {
            let n = call_count_clone.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            let fields: &[&str] = if n == 0 { &["OBJECTID", "FIELD_NAME"] } else { &["OBJECTID", "FIELD_NAME", "NEW_FIELD"] };
            ResponseTemplate::new(200).set_body_json(json!({
                "maxRecordCount": 2000,
                "editingInfo": {"dataLastEditDate": 1000, "schemaLastEditDate": 1000},
                "fields": fields.iter().map(|n| json!({"name": n, "type": "esriFieldTypeString"})).collect::<Vec<_>>(),
            }))
        })
        .mount(&server)
        .await;
    mount_count(&server, 1).await;
    mount_object_ids(&server, &[101]).await;
    mount_geojson_features(
        &server,
        vec![geojson_field_feature(
            101,
            "9101",
            "SCHEMA CHANGE FIELD",
            "Active",
        )],
    )
    .await;

    let active_file_urls = vec![format!("{}/active1.xlsx", server.uri())];
    mount_xlsx(&server, "/active1.xlsx", empty_xlsx()).await;
    mount_xlsx(&server, "/abandoned.xlsx", empty_xlsx()).await;

    let client = reqwest::Client::new();
    let arcgis_layer_url = format!("{}/0", server.uri());
    let abandoned_url = format!("{}/abandoned.xlsx", server.uri());
    let options = ImportOptions {
        retry_policy: fast_retry_policy(),
        count_tolerance_ratio: 0.02,
        limit_object_ids: None,
    };

    let outcome = run_field_import(
        &pool,
        &client,
        &arcgis_layer_url,
        &active_file_urls,
        &abandoned_url,
        options,
    )
    .await
    .unwrap();
    assert_eq!(outcome.status, ImportStatus::Rejected);
}

#[tokio::test]
async fn identical_inputs_are_skipped_on_a_second_run() {
    let pool = test_pool().await;
    let server = MockServer::start().await;
    mount_metadata(&server, &["OBJECTID", "FIELD_NAME"], 1000).await;
    mount_count(&server, 1).await;
    mount_object_ids(&server, &[201]).await;
    mount_geojson_features(
        &server,
        vec![geojson_field_feature(
            201,
            "9201",
            "REPEAT RUN FIELD",
            "Active",
        )],
    )
    .await;

    let active_file_urls = vec![format!("{}/active1.xlsx", server.uri())];
    mount_xlsx(&server, "/active1.xlsx", empty_xlsx()).await;
    mount_xlsx(&server, "/abandoned.xlsx", empty_xlsx()).await;

    let client = reqwest::Client::new();
    let arcgis_layer_url = format!("{}/0", server.uri());
    let abandoned_url = format!("{}/abandoned.xlsx", server.uri());
    let options = ImportOptions {
        retry_policy: fast_retry_policy(),
        count_tolerance_ratio: 0.02,
        limit_object_ids: None,
    };

    let first = run_field_import(
        &pool,
        &client,
        &arcgis_layer_url,
        &active_file_urls,
        &abandoned_url,
        options,
    )
    .await
    .unwrap();
    assert_eq!(first.status, ImportStatus::Completed);

    let second = run_field_import(
        &pool,
        &client,
        &arcgis_layer_url,
        &active_file_urls,
        &abandoned_url,
        options,
    )
    .await
    .unwrap();
    assert_eq!(second.status, ImportStatus::UnchangedSkipped);
    assert_eq!(second.run_id, first.run_id);
}
