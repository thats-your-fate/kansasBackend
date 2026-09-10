//! KS-04: materializer tests. Stages realistic raw rows directly (rather
//! than re-running the full KS-03 ArcGIS/ZIP pipeline, which is already
//! covered by its own tests) so these stay focused on materialize logic:
//! identity resolution, status derivation, and the tombstone rule.
//!
//! **Every test here is `#[serial]`, listed BEFORE `#[tokio::test]`.**
//! The tombstone rule (`materialize_wells_from_run`) correctly scopes
//! "which wells are missing this run" *globally* across every `ks_well`
//! row with the matching `source_format` — the right behavior in
//! production (a full snapshot run must compare against every
//! previously-known well of that source, not an artificially narrowed
//! subset). The real consequence discovered while writing this file:
//! with tests running in parallel against the same shared database (the
//! default), one test's `materialize_wells_from_run` call treats *every
//! other test's already-materialized well* as "missing this run" too,
//! incrementing/tombstoning them as a real side effect — not just
//! polluting the calling run's own aggregate counts, but actually
//! mutating other tests' rows out from under them. Scoping assertions to
//! each test's own `api14` wasn't enough to fix that on its own (still
//! flaky). `#[serial]` (from the `serial_test` crate) is the actual fix —
//! but attribute order matters: `#[tokio::test]` then `#[serial]` (the
//! order that reads naturally) compiled fine but did **not** actually
//! serialize the tests (confirmed empirically: still flaky across
//! repeated runs); `#[serial]` listed first, above `#[tokio::test]`, is
//! what actually works. If you add a new test here, copy that order.

use chrono::Utc;
use future_wells_kansas::event;
use future_wells_kansas::materialize::kgs_wells::{
    materialize_wells_from_run, unknown_status_report, MaterializeError,
};
use future_wells_kansas::source::ingestion_run::{self, DownloadedPayload};
use future_wells_kansas::source::registry;
use serde_json::{json, Value};
use serial_test::serial;
use sqlx::PgPool;
use uuid::Uuid;

async fn event_count(pool: &PgPool, event_type: &str, well_api14: &str) -> i64 {
    sqlx::query_scalar(
        r#"
        SELECT count(*) FROM ks_event e
        JOIN ks_well w ON w.internal_well_id = e.related_well_id
        WHERE e.event_type = $1 AND w.api14 = $2
        "#,
    )
    .bind(event_type)
    .bind(well_api14)
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

/// Creates a `completed` ingestion run for `KGS_WELLS_ARCGIS` with the
/// given raw attribute rows already staged as `included`, mirroring what
/// a real KS-03 run leaves behind. Each row's OBJECTID is read back out
/// of its own JSON rather than passed separately, since `well_attrs`
/// already embeds it.
async fn stage_completed_run(pool: &PgPool, rows: &[Value]) -> Uuid {
    let source = registry::find_source_by_code(pool, "KGS_WELLS_ARCGIS")
        .await
        .unwrap()
        .unwrap();
    let checksum = format!("checksum-{}", Uuid::new_v4());
    let run_id = ingestion_run::start_run(
        pool,
        DownloadedPayload {
            source_id: source.id,
            source_url: "https://example.invalid/wells",
            retrieved_at: Utc::now(),
            source_modified_at: None,
            payload_checksum: &checksum,
            schema_fingerprint: None,
            parser_version: "test",
        },
    )
    .await
    .unwrap();

    for attrs in rows {
        let object_id = attrs
            .get("OBJECTID")
            .and_then(|v| v.as_i64())
            .expect("well_attrs always sets OBJECTID");
        sqlx::query(
            r#"
            INSERT INTO ks_well_raw_snapshot (ingestion_run_id, source_object_id, source_format, raw_data, kansas_membership_status)
            VALUES ($1, $2, 'arcgis', $3, 'included')
            "#,
        )
        .bind(run_id)
        .bind(object_id)
        .bind(attrs)
        .execute(pool)
        .await
        .unwrap();
    }

    ingestion_run::complete_run(pool, run_id, rows.len() as i64, 0, None, None, json!({}))
        .await
        .unwrap();
    run_id
}

fn well_attrs(
    object_id: i64,
    api_num_nodash: &str,
    api_number: &str,
    well_class: &str,
    status: &str,
    status2: &str,
) -> Value {
    json!({
        "OBJECTID": object_id,
        "KID": 1_000_000 + object_id,
        "API_NUMBER": api_number,
        "API_NUM_NODASH": api_num_nodash,
        "FIELD_NAME": "Test Field",
        "LEASE_NAME": "Test Lease",
        "WELL_NAME": "1",
        "WELL_CLASS": well_class,
        "STATUS": status,
        "STATUS2": status2,
        "OPERATOR_NAME": "Original Operator LLC",
        "CURR_OPERATOR": "Current Operator LLC",
        "COUNTY": "Barton",
        "NAD27_LATITUDE": 38.5,
        "NAD27_LONGITUDE": -98.5,
        "LONGITUDE_LATITUDE_SOURCE": "FOOTAGES",
        "PERMIT_DATE_TXT": "07/12/1983",
        "SPUD_DATE_TXT": "07/12/1983",
    })
}

#[serial]
#[tokio::test]
async fn materialize_creates_wells_from_a_completed_run() {
    let pool = test_pool().await;
    let object_id = 900_001;
    let api_nodash = format!("1500{object_id}0000");
    let run_id = stage_completed_run(
        &pool,
        &[well_attrs(
            object_id,
            &api_nodash,
            "15-00-x",
            "Active",
            "Active",
            "Active Oil Well",
        )],
    )
    .await;

    let outcome = materialize_wells_from_run(&pool, run_id).await.unwrap();
    assert_eq!(outcome.created, 1);
    assert_eq!(outcome.updated, 0);

    let row: (String, String, Option<chrono::NaiveDate>) = sqlx::query_as(
        "SELECT lifecycle_status, well_identity_match_method, permit_date FROM ks_well WHERE api14 = $1",
    )
    .bind(&api_nodash)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(row.0, "producing");
    assert_eq!(row.1, "api14");
    assert_eq!(
        row.2,
        Some(chrono::NaiveDate::from_ymd_opt(1983, 7, 12).unwrap())
    );
}

#[serial]
#[tokio::test]
async fn materialize_refuses_a_non_completed_run() {
    let pool = test_pool().await;
    let source = registry::find_source_by_code(&pool, "KGS_WELLS_ARCGIS")
        .await
        .unwrap()
        .unwrap();
    let checksum = format!("checksum-{}", Uuid::new_v4());
    let run_id = ingestion_run::start_run(
        &pool,
        DownloadedPayload {
            source_id: source.id,
            source_url: "https://example.invalid",
            retrieved_at: Utc::now(),
            source_modified_at: None,
            payload_checksum: &checksum,
            schema_fingerprint: None,
            parser_version: "test",
        },
    )
    .await
    .unwrap();
    // run stays 'running' — never completed.

    let result = materialize_wells_from_run(&pool, run_id).await;
    assert!(
        matches!(result, Err(MaterializeError::RunNotCompleted(_, status)) if status == "running")
    );
}

#[serial]
#[tokio::test]
async fn two_consecutive_misses_tombstone_a_well_and_reappearance_untombstones_it() {
    let pool = test_pool().await;
    let object_id = 900_100;
    let api_nodash = format!("1500{object_id}0000");
    let attrs = well_attrs(
        object_id,
        &api_nodash,
        "15-00-y",
        "Active",
        "Active",
        "Active Oil Well",
    );

    // Run 1: well present.
    let run1 = stage_completed_run(&pool, std::slice::from_ref(&attrs)).await;
    let outcome1 = materialize_wells_from_run(&pool, run1).await.unwrap();
    assert_eq!(outcome1.created, 1);

    let is_tombstoned: bool =
        sqlx::query_scalar("SELECT is_tombstoned FROM ks_well WHERE api14 = $1")
            .bind(&api_nodash)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert!(!is_tombstoned);

    // Run 2: well absent (only an unrelated well present) — must NOT be
    // tombstoned yet, only flagged as missing once.
    //
    // Note: this test asserts only on *this test's specific well*
    // (queried by its own unique api14), never on `outcome.
    // missing_incremented`/`tombstoned` aggregate counts — those counts
    // are correctly computed globally across every `ks_well` row sharing
    // `source_format = 'arcgis'` (the right behavior in production: a
    // full snapshot run must compare against every previously-known well
    // of that source), which means they also count other test functions'
    // wells when this file's tests run in parallel against the same
    // shared database. Asserting on this well specifically keeps the
    // test correct regardless of test execution order/parallelism.
    let unrelated_api_nodash = format!("1500{}0000", 900_101);
    let unrelated = well_attrs(
        900_101,
        &unrelated_api_nodash,
        "15-00-z",
        "Active",
        "Active",
        "Active Oil Well",
    );
    let run2 = stage_completed_run(&pool, std::slice::from_ref(&unrelated)).await;
    materialize_wells_from_run(&pool, run2).await.unwrap();

    let (missing_count, is_tombstoned): (i32, bool) = sqlx::query_as(
        "SELECT consecutive_missing_snapshot_count, is_tombstoned FROM ks_well WHERE api14 = $1",
    )
    .bind(&api_nodash)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(missing_count, 1);
    assert!(!is_tombstoned);

    // Run 3: well absent again (second consecutive miss) — now tombstoned.
    let run3 = stage_completed_run(&pool, std::slice::from_ref(&unrelated)).await;
    materialize_wells_from_run(&pool, run3).await.unwrap();

    let (missing_count, is_tombstoned): (i32, bool) = sqlx::query_as(
        "SELECT consecutive_missing_snapshot_count, is_tombstoned FROM ks_well WHERE api14 = $1",
    )
    .bind(&api_nodash)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(missing_count, 2);
    assert!(is_tombstoned);

    // Run 4: well reappears — untombstoned, counter reset to 0.
    let run4 = stage_completed_run(&pool, std::slice::from_ref(&attrs)).await;
    let outcome4 = materialize_wells_from_run(&pool, run4).await.unwrap();
    assert_eq!(outcome4.updated, 1);

    let (missing_count, is_tombstoned): (i32, bool) = sqlx::query_as(
        "SELECT consecutive_missing_snapshot_count, is_tombstoned FROM ks_well WHERE api14 = $1",
    )
    .bind(&api_nodash)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(missing_count, 0);
    assert!(!is_tombstoned);
}

#[serial]
#[tokio::test]
async fn unknown_status_is_surfaced_in_the_qa_report_not_hidden_as_inactive() {
    let pool = test_pool().await;
    let object_id = 900_200;
    let api_nodash = format!("1500{object_id}0000");
    let run_id = stage_completed_run(
        &pool,
        &[well_attrs(
            object_id,
            &api_nodash,
            "15-00-w",
            "Xyzzy Class 7",
            "Q9",
            "",
        )],
    )
    .await;

    materialize_wells_from_run(&pool, run_id).await.unwrap();

    let lifecycle: String =
        sqlx::query_scalar("SELECT lifecycle_status FROM ks_well WHERE api14 = $1")
            .bind(&api_nodash)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(lifecycle, "unknown");

    let report = unknown_status_report(&pool).await.unwrap();
    assert!(report
        .iter()
        .any(|entry| entry.well_class_raw.as_deref() == Some("Xyzzy Class 7")));
}

#[serial]
#[tokio::test]
async fn row_with_no_valid_api_and_no_kid_is_rejected_not_materialized() {
    let pool = test_pool().await;
    let object_id = 900_300;
    let mut attrs = well_attrs(
        object_id,
        "not-a-number",
        "garbage",
        "Active",
        "Active",
        "Active",
    );
    attrs.as_object_mut().unwrap().remove("KID");

    let run_id = stage_completed_run(&pool, &[attrs]).await;
    let outcome = materialize_wells_from_run(&pool, run_id).await.unwrap();
    assert_eq!(outcome.rejected_no_identity, 1);
    assert_eq!(outcome.created, 0);

    let rejection_count: i64 =
        sqlx::query_scalar("SELECT count(*) FROM ks_source_rejection WHERE ingestion_run_id = $1")
            .bind(run_id)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(rejection_count, 1);
}

#[serial]
#[tokio::test]
async fn kid_fallback_identity_is_used_when_api_is_malformed() {
    let pool = test_pool().await;
    let object_id = 900_400;
    let attrs = well_attrs(
        object_id,
        "not-numeric",
        "garbage",
        "Active",
        "Active",
        "Active",
    );

    let run_id = stage_completed_run(&pool, &[attrs]).await;
    let outcome = materialize_wells_from_run(&pool, run_id).await.unwrap();
    assert_eq!(outcome.created, 1);
    assert_eq!(outcome.rejected_no_identity, 0);

    let match_method: String =
        sqlx::query_scalar("SELECT well_identity_match_method FROM ks_well WHERE kgs_kid = $1")
            .bind((1_000_000 + object_id).to_string())
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(match_method, "kid_fallback");
}

/// KS-14: a well's very first materialize (no prior state to compare
/// against) must never emit a transition event — otherwise every one of
/// Kansas's ~180,000 wells would fire `WELL_STATUS_CHANGED` etc. on day
/// one, exactly the "hundreds of thousands of events" flood the task doc
/// warns against, independent of the backfill flag entirely.
#[serial]
#[tokio::test]
async fn a_wells_first_ever_materialize_never_emits_a_transition_event() {
    let pool = test_pool().await;
    let object_id = 900_500;
    let api_nodash = format!("1500{object_id}0000");
    let run_id = stage_completed_run(
        &pool,
        &[well_attrs(
            object_id,
            &api_nodash,
            "15-00-a",
            "Active",
            "Active",
            "Active Oil Well",
        )],
    )
    .await;

    materialize_wells_from_run(&pool, run_id).await.unwrap();

    assert_eq!(
        event_count(&pool, event::SPUD_RECORDED, &api_nodash).await,
        0
    );
    assert_eq!(
        event_count(&pool, event::WELL_STATUS_CHANGED, &api_nodash).await,
        0
    );
}

/// KS-14 acceptance criteria: revision-safe dedupe. A live (non-backfill)
/// re-materialize with the *same* spud date/status must not duplicate an
/// event; a later correction to a genuinely different date/status must
/// produce a new one.
#[serial]
#[tokio::test]
async fn revision_safe_dedupe_for_spud_and_status_transitions() {
    let pool = test_pool().await;
    let object_id = 900_600;
    let api_nodash = format!("1500{object_id}0000");

    let run1 = stage_completed_run(
        &pool,
        &[well_attrs(
            object_id,
            &api_nodash,
            "15-00-b",
            "Active",
            "Active",
            "Active Oil Well",
        )],
    )
    .await;
    materialize_wells_from_run(&pool, run1).await.unwrap();
    assert_eq!(
        event_count(&pool, event::SPUD_RECORDED, &api_nodash).await,
        0,
        "no prior state to transition from on the first run"
    );

    // Run 2: status changes from producing to plugged, plug date newly
    // recorded — both a WELL_STATUS_CHANGED and a PLUGGING_RECORDED.
    let mut plugged = well_attrs(
        object_id,
        &api_nodash,
        "15-00-b",
        "Active",
        "Active",
        "Plugged",
    );
    plugged
        .as_object_mut()
        .unwrap()
        .insert("PLUG_DATE_TXT".to_string(), json!("03/04/2020"));
    let run2 = stage_completed_run(&pool, &[plugged.clone()]).await;
    materialize_wells_from_run(&pool, run2).await.unwrap();
    assert_eq!(
        event_count(&pool, event::WELL_STATUS_CHANGED, &api_nodash).await,
        1
    );
    assert_eq!(
        event_count(&pool, event::PLUGGING_RECORDED, &api_nodash).await,
        1
    );

    // Run 3: identical re-observation — no new events (dedup, not a
    // duplicate notification after an importer rerun).
    let run3 = stage_completed_run(&pool, &[plugged]).await;
    materialize_wells_from_run(&pool, run3).await.unwrap();
    assert_eq!(
        event_count(&pool, event::WELL_STATUS_CHANGED, &api_nodash).await,
        1
    );
    assert_eq!(
        event_count(&pool, event::PLUGGING_RECORDED, &api_nodash).await,
        1
    );

    // Run 4: the plug date is later corrected to a different date — a
    // genuine revision must produce a new PLUGGING_RECORDED event, not be
    // silently absorbed by the dedup key from run 2/3.
    let mut corrected = well_attrs(
        object_id,
        &api_nodash,
        "15-00-b",
        "Active",
        "Active",
        "Plugged",
    );
    corrected
        .as_object_mut()
        .unwrap()
        .insert("PLUG_DATE_TXT".to_string(), json!("03/05/2020"));
    let run4 = stage_completed_run(&pool, &[corrected]).await;
    materialize_wells_from_run(&pool, run4).await.unwrap();
    assert_eq!(
        event_count(&pool, event::PLUGGING_RECORDED, &api_nodash).await,
        2,
        "a genuine date correction must produce a new event"
    );
    assert_eq!(
        event_count(&pool, event::WELL_STATUS_CHANGED, &api_nodash).await,
        1,
        "status did not change again, so no new status-change event"
    );
}

/// KS-14 acceptance criteria: baseline-event suppression. A run explicitly
/// marked as a backfill must not emit transition events for historical
/// (pre-cutoff) dates, while an ordinary live run for the identical
/// transition does.
#[serial]
#[tokio::test]
async fn baseline_backfill_run_suppresses_historical_transition_events() {
    let pool = test_pool().await;
    let object_id = 900_700;
    let api_nodash = format!("1500{object_id}0000");

    let run1 = stage_completed_run(
        &pool,
        &[well_attrs(
            object_id,
            &api_nodash,
            "15-00-c",
            "Active",
            "Active",
            "Active Oil Well",
        )],
    )
    .await;
    materialize_wells_from_run(&pool, run1).await.unwrap();

    // Run 2, marked as a backfill: a historical plug date (well before the
    // fixed live-monitoring cutoff) must be suppressed.
    let mut plugged = well_attrs(
        object_id,
        &api_nodash,
        "15-00-c",
        "Active",
        "Active",
        "Plugged",
    );
    plugged
        .as_object_mut()
        .unwrap()
        .insert("PLUG_DATE_TXT".to_string(), json!("03/04/1990"));
    let run2 = stage_completed_run(&pool, &[plugged]).await;
    ingestion_run::mark_as_backfill(&pool, run2).await.unwrap();
    materialize_wells_from_run(&pool, run2).await.unwrap();
    assert_eq!(
        event_count(&pool, event::PLUGGING_RECORDED, &api_nodash).await,
        0,
        "a backfill run must suppress a historical transition event"
    );
    assert_eq!(
        event_count(&pool, event::WELL_STATUS_CHANGED, &api_nodash).await,
        0
    );

    // Run 3, an ordinary live run: a *different* subsequent correction to
    // the same well must emit normally — suppression is per-run, not a
    // permanent mark on the well.
    let mut corrected = well_attrs(
        object_id,
        &api_nodash,
        "15-00-c",
        "Active",
        "Active",
        "Plugged",
    );
    corrected
        .as_object_mut()
        .unwrap()
        .insert("PLUG_DATE_TXT".to_string(), json!("03/05/1990"));
    let run3 = stage_completed_run(&pool, &[corrected]).await;
    materialize_wells_from_run(&pool, run3).await.unwrap();
    assert_eq!(
        event_count(&pool, event::PLUGGING_RECORDED, &api_nodash).await,
        1,
        "a live run must emit even for a historically-dated correction"
    );
}

/// The "deliberate historical timeline job" escape hatch: a backfill run
/// explicitly also marked `force_historical` publishes its events anyway.
#[serial]
#[tokio::test]
async fn force_historical_backfill_run_publishes_its_events() {
    let pool = test_pool().await;
    let object_id = 900_800;
    let api_nodash = format!("1500{object_id}0000");

    let run1 = stage_completed_run(
        &pool,
        &[well_attrs(
            object_id,
            &api_nodash,
            "15-00-d",
            "Active",
            "Active",
            "Active Oil Well",
        )],
    )
    .await;
    materialize_wells_from_run(&pool, run1).await.unwrap();

    let mut plugged = well_attrs(
        object_id,
        &api_nodash,
        "15-00-d",
        "Active",
        "Active",
        "Plugged",
    );
    plugged
        .as_object_mut()
        .unwrap()
        .insert("PLUG_DATE_TXT".to_string(), json!("03/04/1990"));
    let run2 = stage_completed_run(&pool, &[plugged]).await;
    ingestion_run::mark_as_backfill(&pool, run2).await.unwrap();
    ingestion_run::mark_as_force_historical(&pool, run2)
        .await
        .unwrap();
    materialize_wells_from_run(&pool, run2).await.unwrap();
    assert_eq!(
        event_count(&pool, event::PLUGGING_RECORDED, &api_nodash).await,
        1,
        "force_historical must publish even a historically-dated backfill event"
    );
}

/// Every emitted event carries its source ingestion run — KS-14
/// acceptance criteria: "every event links back to its official source or
/// source snapshot."
#[serial]
#[tokio::test]
async fn every_emitted_event_links_back_to_its_source_ingestion_run() {
    let pool = test_pool().await;
    let object_id = 900_900;
    let api_nodash = format!("1500{object_id}0000");

    let run1 = stage_completed_run(
        &pool,
        &[well_attrs(
            object_id,
            &api_nodash,
            "15-00-e",
            "Active",
            "Active",
            "Active Oil Well",
        )],
    )
    .await;
    materialize_wells_from_run(&pool, run1).await.unwrap();
    let mut plugged = well_attrs(
        object_id,
        &api_nodash,
        "15-00-e",
        "Active",
        "Active",
        "Plugged",
    );
    plugged
        .as_object_mut()
        .unwrap()
        .insert("PLUG_DATE_TXT".to_string(), json!("03/04/2020"));
    let run2 = stage_completed_run(&pool, &[plugged]).await;
    materialize_wells_from_run(&pool, run2).await.unwrap();

    let source_run_id: Uuid = sqlx::query_scalar(
        r#"
        SELECT e.source_ingestion_run_id FROM ks_event e
        JOIN ks_well w ON w.internal_well_id = e.related_well_id
        WHERE e.event_type = $1 AND w.api14 = $2
        "#,
    )
    .bind(event::PLUGGING_RECORDED)
    .bind(&api_nodash)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(source_run_id, run2);
}
