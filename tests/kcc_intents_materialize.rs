//! KS-06/KS-14: materializer tests — well-linking tiers, event generation
//! and dedup, the unresolved-intents report, and intent expiration.
//! Stages raw rows directly (bypassing the HTML-table pipeline, already
//! covered by `tests/kcc_intents_import.rs`) so these stay focused on
//! materialize logic.
//!
//! **Every test here is `#[serial]`, listed BEFORE `#[tokio::test]`** —
//! the same requirement `tests/kgs_wells_materialize.rs` documents at
//! length for its own tombstone rule applies here verbatim: KS-14's
//! `expire_stale_intents` scans and increments *every* not-yet-expired
//! intent in the database that this run didn't touch, globally, not
//! scoped to this test's own fixtures — the right behavior in
//! production, but real cross-test mutation when tests run in parallel
//! against the shared database (confirmed empirically while writing
//! these tests: without `#[serial]`, other tests' still-unresolved
//! intents got expired mid-run, and this file's own target intent was
//! sometimes expired a full run early). Attribute order matters, per the
//! same lesson: `#[serial]` above `#[tokio::test]`.

use future_wells_kansas::event;
use future_wells_kansas::materialize::kcc_intents::{
    materialize_intents_from_run, MaterializeError,
};
use future_wells_kansas::source::ingestion_run::{self, DownloadedPayload};
use future_wells_kansas::source::registry;
use serial_test::serial;
use sqlx::PgPool;
use uuid::Uuid;

async fn test_pool() -> PgPool {
    let database_url =
        std::env::var("DATABASE_URL").expect("DATABASE_URL must be set to run these tests");
    let pool = future_wells_kansas::db::connect(&database_url)
        .await
        .expect("failed to connect to test database");
    sqlx::migrate!("./migrations").run(&pool).await.unwrap();
    pool
}

async fn any_ingestion_run_id(pool: &PgPool) -> Uuid {
    let source = registry::find_source_by_code(pool, "KGS_WELLS_ARCGIS")
        .await
        .unwrap()
        .unwrap();
    ingestion_run::start_run(
        pool,
        DownloadedPayload {
            source_id: source.id,
            source_url: "test-fixture",
            retrieved_at: chrono::Utc::now(),
            source_modified_at: None,
            payload_checksum: &format!("checksum-{}", Uuid::new_v4()),
            schema_fingerprint: None,
            parser_version: "test-fixture",
        },
    )
    .await
    .unwrap()
}

#[allow(clippy::too_many_arguments)]
async fn insert_well(
    pool: &PgPool,
    api14: Option<&str>,
    county: Option<&str>,
    township: Option<i16>,
    range: Option<i16>,
    range_direction: Option<&str>,
    section: Option<i16>,
    lease_name: Option<&str>,
    operator_name: Option<&str>,
) -> Uuid {
    let run_id = any_ingestion_run_id(pool).await;
    sqlx::query_scalar(
        r#"
        INSERT INTO ks_well (
            api14, api_validation_status, well_identity_match_method, lifecycle_status,
            source_format, source_ingestion_run_id, county, township, range, range_direction,
            section, lease_name, operator_name_kgs_reported_current
        )
        VALUES ($1, 'valid', 'api14', 'unknown', 'arcgis', $2, $3, $4, $5, $6, $7, $8, $9)
        RETURNING internal_well_id
        "#,
    )
    .bind(api14)
    .bind(run_id)
    .bind(county)
    .bind(township)
    .bind(range)
    .bind(range_direction)
    .bind(section)
    .bind(lease_name)
    .bind(operator_name)
    .fetch_one(pool)
    .await
    .unwrap()
}

async fn insert_operator(pool: &PgPool, license: &str, normalized_name: &str) {
    let run_id = any_ingestion_run_id(pool).await;
    sqlx::query(
        r#"
        INSERT INTO ks_operator (kcc_license_number, reported_company_name, normalized_company_name, active_status, source_record_hash, source_ingestion_run_id)
        VALUES ($1, $2, $3, 'active', 'test-hash', $4)
        "#,
    )
    .bind(license)
    .bind(normalized_name)
    .bind(normalized_name)
    .bind(run_id)
    .execute(pool)
    .await
    .unwrap();
}

#[allow(clippy::too_many_arguments)]
async fn stage_intent_run(
    pool: &PgPool,
    source_filename: &str,
    county: Option<&str>,
    township: Option<i16>,
    range: Option<i16>,
    range_direction: Option<&str>,
    section: Option<i16>,
    operator_license: Option<&str>,
    lease_or_well_name: Option<&str>,
) -> Uuid {
    let source = registry::find_source_by_code(pool, "KCC_INTENTS")
        .await
        .unwrap()
        .unwrap();
    let checksum = format!("checksum-{}", Uuid::new_v4());
    let run_id = ingestion_run::start_run(
        pool,
        DownloadedPayload {
            source_id: source.id,
            source_url: "https://www.kcc.ks.gov/notices-of-intents-search",
            retrieved_at: chrono::Utc::now(),
            source_modified_at: None,
            payload_checksum: &checksum,
            schema_fingerprint: None,
            parser_version: "test-fixture",
        },
    )
    .await
    .unwrap();

    sqlx::query(
        r#"
        INSERT INTO ks_intent_raw_snapshot (
            ingestion_run_id, source_filename, permit_date, county, township, range, range_direction,
            section, operator_license, reported_lease_or_well_name, raw_data, source_record_hash, observed_at
        )
        VALUES ($1, $2, '2026-01-15', $3, $4, $5, $6, $7, $8, $9, '{}'::jsonb, $10, now())
        "#,
    )
    .bind(run_id)
    .bind(source_filename)
    .bind(county)
    .bind(township)
    .bind(range)
    .bind(range_direction)
    .bind(section)
    .bind(operator_license)
    .bind(lease_or_well_name)
    .bind(format!("hash-{}", Uuid::new_v4()))
    .execute(pool)
    .await
    .unwrap();

    ingestion_run::complete_run(pool, run_id, 1, 0, None, None, serde_json::json!({}))
        .await
        .unwrap();
    run_id
}

#[serial]
#[tokio::test]
async fn tier1_matches_by_api14_embedded_in_source_filename() {
    let pool = test_pool().await;
    let well_id = insert_well(
        &pool,
        Some("15009999990000"),
        None,
        None,
        None,
        None,
        None,
        None,
        None,
    )
    .await;
    let run_id = stage_intent_run(
        &pool,
        "15009999990000.pdf",
        None,
        None,
        None,
        None,
        None,
        None,
        None,
    )
    .await;

    let outcome = materialize_intents_from_run(&pool, run_id).await.unwrap();
    assert_eq!(outcome.exact_matches, 1);
    assert_eq!(outcome.created, 1);

    let (matched, method): (Option<Uuid>, String) =
        sqlx::query_as("SELECT matched_well_id, match_method FROM ks_drilling_intent WHERE source_filename = '15009999990000.pdf'").fetch_one(&pool).await.unwrap();
    assert_eq!(matched, Some(well_id));
    assert_eq!(method, "exact_permit_or_api");
}

#[serial]
#[tokio::test]
async fn tier2_matches_by_license_plss_and_name() {
    let pool = test_pool().await;
    insert_operator(&pool, "K5001", "TIER TWO OPERATING LLC").await;
    let well_id = insert_well(
        &pool,
        None,
        Some("Barton"),
        Some(15),
        Some(10),
        Some("E"),
        Some(4),
        Some("Tier Two Lease"),
        Some("Tier Two Operating LLC"),
    )
    .await;

    let run_id = stage_intent_run(
        &pool,
        "no-id-in-filename.pdf",
        Some("Barton"),
        Some(15),
        Some(10),
        Some("E"),
        Some(4),
        Some("K5001"),
        Some("Tier Two Lease"),
    )
    .await;

    let outcome = materialize_intents_from_run(&pool, run_id).await.unwrap();
    assert_eq!(outcome.exact_matches, 1);

    let (matched, method): (Option<Uuid>, String) =
        sqlx::query_as("SELECT matched_well_id, match_method FROM ks_drilling_intent WHERE source_filename = 'no-id-in-filename.pdf'").fetch_one(&pool).await.unwrap();
    assert_eq!(matched, Some(well_id));
    assert_eq!(method, "license_plss_name");
}

#[serial]
#[tokio::test]
async fn tier3_probabilistic_match_requires_a_confidence_value() {
    let pool = test_pool().await;
    let well_id = insert_well(
        &pool,
        None,
        Some("Reno"),
        None,
        None,
        None,
        None,
        Some("Anderson Lease 12"),
        None,
    )
    .await;
    // No operator_license provided, so tier 2 can't apply — only the
    // fuzzy tier can resolve this one.
    let run_id = stage_intent_run(
        &pool,
        "fuzzy-only.pdf",
        Some("Reno"),
        None,
        None,
        None,
        None,
        None,
        Some("Anderson Lease 1-2"),
    )
    .await;

    let outcome = materialize_intents_from_run(&pool, run_id).await.unwrap();
    assert_eq!(outcome.probabilistic_matches, 1);

    let (matched, method, confidence): (Option<Uuid>, String, Option<f64>) =
        sqlx::query_as("SELECT matched_well_id, match_method, match_confidence::float8 FROM ks_drilling_intent WHERE source_filename = 'fuzzy-only.pdf'")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(matched, Some(well_id));
    assert_eq!(method, "probabilistic");
    assert!(confidence.unwrap() >= 0.5);
}

#[serial]
#[tokio::test]
async fn tier4_unresolved_when_nothing_matches() {
    let pool = test_pool().await;
    let run_id = stage_intent_run(
        &pool,
        "nothing-matches.pdf",
        Some("Reno"),
        None,
        None,
        None,
        None,
        None,
        Some("Totally Different Name"),
    )
    .await;

    let outcome = materialize_intents_from_run(&pool, run_id).await.unwrap();
    assert_eq!(outcome.unresolved, 1);

    let (matched, method): (Option<Uuid>, String) =
        sqlx::query_as("SELECT matched_well_id, match_method FROM ks_drilling_intent WHERE source_filename = 'nothing-matches.pdf'").fetch_one(&pool).await.unwrap();
    assert_eq!(matched, None);
    assert_eq!(method, "unresolved");
}

#[serial]
#[tokio::test]
async fn re_materializing_the_same_run_deduplicates_the_event_but_stays_idempotent() {
    let pool = test_pool().await;
    let run_id = stage_intent_run(
        &pool,
        "dedup-check.pdf",
        None,
        None,
        None,
        None,
        None,
        None,
        None,
    )
    .await;

    let first = materialize_intents_from_run(&pool, run_id).await.unwrap();
    assert_eq!(first.created, 1);
    assert_eq!(first.events_emitted, 1);

    let second = materialize_intents_from_run(&pool, run_id).await.unwrap();
    assert_eq!(second.created, 0);
    assert_eq!(second.updated, 1);
    assert_eq!(
        second.events_emitted, 0,
        "the event must not be duplicated on re-materialization"
    );

    let intent_id: Uuid = sqlx::query_scalar(
        "SELECT id FROM ks_drilling_intent WHERE source_filename = 'dedup-check.pdf'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    // Scoped to this test's own dedup_key, not a table-wide count — other
    // tests in this file run concurrently against the same database and
    // emit their own INTENT_POSTED events.
    let event_count: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM ks_event WHERE event_type = 'INTENT_POSTED' AND dedup_key = $1",
    )
    .bind(intent_id.to_string())
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(event_count, 1);
}

#[serial]
#[tokio::test]
async fn materialize_refuses_a_non_completed_run() {
    let pool = test_pool().await;
    let source = registry::find_source_by_code(&pool, "KCC_INTENTS")
        .await
        .unwrap()
        .unwrap();
    let checksum = format!("checksum-{}", Uuid::new_v4());
    let run_id = ingestion_run::start_run(
        &pool,
        DownloadedPayload {
            source_id: source.id,
            source_url: "https://www.kcc.ks.gov/notices-of-intents-search",
            retrieved_at: chrono::Utc::now(),
            source_modified_at: None,
            payload_checksum: &checksum,
            schema_fingerprint: None,
            parser_version: "test-fixture",
        },
    )
    .await
    .unwrap();

    let err = materialize_intents_from_run(&pool, run_id)
        .await
        .unwrap_err();
    assert!(
        matches!(err, MaterializeError::RunNotCompleted(id, status) if id == run_id && status == "running")
    );
}

/// KS-14: an unresolved (never-spudded) intent that falls out of two
/// consecutive fresh snapshots of the rolling window is flagged
/// `INTENT_EXPIRED` — mirroring `ks_well`'s own two-consecutive-misses
/// tombstone rule.
#[serial]
#[tokio::test]
async fn unresolved_intent_expires_after_two_consecutive_misses() {
    let pool = test_pool().await;
    let unique = Uuid::new_v4();
    let target_filename = format!("expiring-{unique}.pdf");
    let run1 = stage_intent_run(
        &pool,
        &target_filename,
        Some("Reno"),
        None,
        None,
        None,
        None,
        None,
        Some("Never Spudded Lease"),
    )
    .await;
    let outcome1 = materialize_intents_from_run(&pool, run1).await.unwrap();
    assert_eq!(outcome1.unresolved, 1, "no operator/PLSS/name match set up for this fixture, so it stays unresolved — expiration must still work for an unresolved intent");

    let intent_id: Uuid =
        sqlx::query_scalar("SELECT id FROM ks_drilling_intent WHERE source_filename = $1")
            .bind(&target_filename)
            .fetch_one(&pool)
            .await
            .unwrap();

    // Two subsequent runs that report only an unrelated intent — the
    // target intent is absent from both fresh snapshots.
    //
    // Note: assertions below check only *this test's own* intent (by id),
    // never `outcome.expired` (a table-wide aggregate) — the expiration
    // scan legitimately operates globally across every not-yet-expired
    // intent in the database (the same "real production behavior, real
    // cross-test aggregate pollution" tradeoff `tests/kgs_wells_materialize.rs`
    // documents at length for its own tombstone rule), so another test's
    // intent reaching its own two-miss threshold during this test's run
    // would otherwise make this assertion flaky for reasons that have
    // nothing to do with this test's own correctness.
    let other1 = format!("unrelated-1-{unique}.pdf");
    let run2 = stage_intent_run(&pool, &other1, None, None, None, None, None, None, None).await;
    materialize_intents_from_run(&pool, run2).await.unwrap();
    let is_expired: bool =
        sqlx::query_scalar("SELECT is_expired FROM ks_drilling_intent WHERE id = $1")
            .bind(intent_id)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert!(!is_expired, "one miss is not yet two consecutive misses");

    let other2 = format!("unrelated-2-{unique}.pdf");
    let run3 = stage_intent_run(&pool, &other2, None, None, None, None, None, None, None).await;
    materialize_intents_from_run(&pool, run3).await.unwrap();
    let is_expired: bool =
        sqlx::query_scalar("SELECT is_expired FROM ks_drilling_intent WHERE id = $1")
            .bind(intent_id)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert!(is_expired);

    let expired_event_count: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM ks_event WHERE event_type = $1 AND dedup_key = $2",
    )
    .bind(event::INTENT_EXPIRED)
    .bind(intent_id.to_string())
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(expired_event_count, 1);
}

/// An intent whose matched well has actually been spudded must never be
/// flagged as expired, even after aging out of the window — it fell off
/// the page because drilling succeeded, not because the permit lapsed.
#[serial]
#[tokio::test]
async fn a_spudded_wells_intent_never_expires() {
    let pool = test_pool().await;
    let unique = Uuid::new_v4();
    let api14 = format!("150099{}", &unique.simple().to_string()[..8]);
    let well_id = insert_well(
        &pool,
        Some(&api14),
        None,
        None,
        None,
        None,
        None,
        None,
        None,
    )
    .await;
    sqlx::query("UPDATE ks_well SET spud_date = '2024-01-01' WHERE internal_well_id = $1")
        .bind(well_id)
        .execute(&pool)
        .await
        .unwrap();

    let target_filename = format!("{api14}.pdf");
    let run1 = stage_intent_run(
        &pool,
        &target_filename,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
    )
    .await;
    let outcome1 = materialize_intents_from_run(&pool, run1).await.unwrap();
    assert_eq!(outcome1.exact_matches, 1);
    let intent_id: Uuid =
        sqlx::query_scalar("SELECT id FROM ks_drilling_intent WHERE source_filename = $1")
            .bind(&target_filename)
            .fetch_one(&pool)
            .await
            .unwrap();

    let other1 = format!("unrelated-a-{unique}.pdf");
    let run2 = stage_intent_run(&pool, &other1, None, None, None, None, None, None, None).await;
    materialize_intents_from_run(&pool, run2).await.unwrap();
    let other2 = format!("unrelated-b-{unique}.pdf");
    let run3 = stage_intent_run(&pool, &other2, None, None, None, None, None, None, None).await;
    materialize_intents_from_run(&pool, run3).await.unwrap();

    // Scoped to this test's own intent, not `outcome.expired` (a
    // table-wide aggregate) — see the sibling test's comment for why.
    let is_expired: bool =
        sqlx::query_scalar("SELECT is_expired FROM ks_drilling_intent WHERE id = $1")
            .bind(intent_id)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert!(!is_expired);
}
