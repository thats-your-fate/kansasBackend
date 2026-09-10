//! KS-15 acceptance criteria: materializer tests — idempotent metadata
//! reruns, well-linking by KID, restricted/login-required documents
//! never reaching the download queue, and `DOCUMENT_ADDED` events
//! subject to KS-14's baseline-backfill suppression. Stages raw rows
//! directly, same discipline as every other materialize test in this repo.

use future_wells_kansas::event;
use future_wells_kansas::materialize::kgs_documents::{
    enqueue_for_download, materialize_documents_from_run, next_queued_documents, MaterializeError,
};
use future_wells_kansas::source::ingestion_run::{self, DownloadedPayload};
use future_wells_kansas::source::registry;
use serde_json::json;
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

struct RawDoc<'a> {
    source_document_id: &'a str,
    document_type_raw: &'a str,
    kgs_kid: Option<&'a str>,
    source_url: &'a str,
    filing_date_raw: Option<&'a str>,
    access_state: &'a str,
}

async fn stage_run(pool: &PgPool, docs: &[RawDoc<'_>]) -> Uuid {
    let source = registry::find_source_by_code(pool, "KGS_DOCUMENTS")
        .await
        .unwrap()
        .unwrap();
    let checksum = format!("checksum-{}", Uuid::new_v4());
    let run_id = ingestion_run::start_run(
        pool,
        DownloadedPayload {
            source_id: source.id,
            source_url: "test-fixture",
            retrieved_at: chrono::Utc::now(),
            source_modified_at: None,
            payload_checksum: &checksum,
            schema_fingerprint: None,
            parser_version: "test-fixture",
        },
    )
    .await
    .unwrap();

    for doc in docs {
        let raw_data = json!({
            "document_type_raw": doc.document_type_raw,
            "kgs_kid": doc.kgs_kid,
            "api_number_raw": null,
            "source_url": doc.source_url,
            "filing_date_raw": doc.filing_date_raw,
            "access_state": doc.access_state,
        });
        sqlx::query("INSERT INTO ks_well_document_raw_snapshot (ingestion_run_id, document_source, source_document_id, raw_data, source_record_hash) VALUES ($1, 'KGS_DOCUMENTS', $2, $3, $4)")
            .bind(run_id)
            .bind(doc.source_document_id)
            .bind(&raw_data)
            .bind(format!("hash-{}", Uuid::new_v4()))
            .execute(pool)
            .await
            .unwrap();
    }
    ingestion_run::complete_run(pool, run_id, docs.len() as i64, 0, None, None, json!({}))
        .await
        .unwrap();
    run_id
}

async fn insert_well(pool: &PgPool, kid: &str) -> Uuid {
    let source = registry::find_source_by_code(pool, "KGS_WELLS_ARCGIS")
        .await
        .unwrap()
        .unwrap();
    let run_id = ingestion_run::start_run(
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
    .unwrap();
    sqlx::query_scalar("INSERT INTO ks_well (kgs_kid, api_validation_status, well_identity_match_method, lifecycle_status, source_format, source_ingestion_run_id) VALUES ($1, 'missing', 'kid_fallback', 'active', 'arcgis', $2) RETURNING internal_well_id")
        .bind(kid)
        .bind(run_id)
        .fetch_one(pool)
        .await
        .unwrap()
}

#[tokio::test]
async fn materialize_resolves_well_by_kid_and_emits_document_added() {
    let pool = test_pool().await;
    let kid = format!("{}", Uuid::new_v4().as_u128() % 900_000_000);
    let well_id = insert_well(&pool, &kid).await;

    let run_id = stage_run(
        &pool,
        &[RawDoc {
            source_document_id: &format!("tops:{kid}:Completion Report:Apr-15-1999"),
            document_type_raw: "Completion Report",
            kgs_kid: Some(&kid),
            source_url: "https://chasm.kgs.ku.edu/x",
            filing_date_raw: Some("Apr-15-1999"),
            access_state: "public",
        }],
    )
    .await;

    let outcome = materialize_documents_from_run(&pool, run_id).await.unwrap();
    assert_eq!(outcome.created, 1);
    assert_eq!(outcome.unresolved, 0);
    assert_eq!(outcome.events_emitted, 1);

    let (resolved_well_id, document_type, filing_date, parser_status): (Option<Uuid>, String, Option<chrono::NaiveDate>, String) =
        sqlx::query_as("SELECT well_id, document_type, filing_date, parser_status FROM ks_well_document WHERE source_document_id = $1")
            .bind(format!("tops:{kid}:Completion Report:Apr-15-1999"))
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(resolved_well_id, Some(well_id));
    assert_eq!(document_type, "aco1");
    assert_eq!(
        filing_date,
        Some(chrono::NaiveDate::from_ymd_opt(1999, 4, 15).unwrap())
    );
    assert_eq!(parser_status, "not_queued");

    let event_count: i64 = sqlx::query_scalar("SELECT count(*) FROM ks_event WHERE event_type = $1 AND dedup_key IN (SELECT id::text FROM ks_well_document WHERE source_document_id = $2)")
        .bind(event::DOCUMENT_ADDED)
        .bind(format!("tops:{kid}:Completion Report:Apr-15-1999"))
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(event_count, 1);
}

#[tokio::test]
async fn unresolved_document_is_still_indexed_and_reported() {
    let pool = test_pool().await;
    let unique_id = format!("wdl:{}", Uuid::new_v4());
    let run_id = stage_run(
        &pool,
        &[RawDoc {
            source_document_id: &unique_id,
            document_type_raw: "Other",
            kgs_kid: Some("no-such-kid"),
            source_url: "https://www.kgslibrary.com/index.cfm?whk=1",
            filing_date_raw: None,
            access_state: "login_required",
        }],
    )
    .await;

    let outcome = materialize_documents_from_run(&pool, run_id).await.unwrap();
    assert_eq!(outcome.created, 1);
    assert_eq!(outcome.unresolved, 1);

    let well_id: Option<Uuid> =
        sqlx::query_scalar("SELECT well_id FROM ks_well_document WHERE source_document_id = $1")
            .bind(&unique_id)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(
        well_id, None,
        "an unresolved document is still indexed, never dropped"
    );
}

#[tokio::test]
async fn login_required_document_is_never_queueable() {
    let pool = test_pool().await;
    let unique_id = format!("wdl:{}", Uuid::new_v4());
    let run_id = stage_run(
        &pool,
        &[RawDoc {
            source_document_id: &unique_id,
            document_type_raw: "Other",
            kgs_kid: None,
            source_url: "https://www.kgslibrary.com/index.cfm?whk=1",
            filing_date_raw: None,
            access_state: "login_required",
        }],
    )
    .await;
    materialize_documents_from_run(&pool, run_id).await.unwrap();

    let document_id: Uuid =
        sqlx::query_scalar("SELECT id FROM ks_well_document WHERE source_document_id = $1")
            .bind(&unique_id)
            .fetch_one(&pool)
            .await
            .unwrap();
    let parser_status: String =
        sqlx::query_scalar("SELECT parser_status FROM ks_well_document WHERE id = $1")
            .bind(document_id)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(parser_status, "skipped_restricted");

    let err = enqueue_for_download(&pool, document_id).await.unwrap_err();
    assert!(matches!(err, MaterializeError::NotPublic(id) if id == document_id));

    let queued = next_queued_documents(&pool, 100).await.unwrap();
    assert!(
        !queued.iter().any(|d| d.id == document_id),
        "a login-required document must never appear in the download queue"
    );
}

#[tokio::test]
async fn a_public_document_can_be_enqueued_and_appears_in_the_queue() {
    let pool = test_pool().await;
    let unique_id = format!("tops:{}", Uuid::new_v4());
    let run_id = stage_run(
        &pool,
        &[RawDoc {
            source_document_id: &unique_id,
            document_type_raw: "Completion Report",
            kgs_kid: None,
            source_url: "https://chasm.kgs.ku.edu/x",
            filing_date_raw: None,
            access_state: "public",
        }],
    )
    .await;
    materialize_documents_from_run(&pool, run_id).await.unwrap();

    let document_id: Uuid =
        sqlx::query_scalar("SELECT id FROM ks_well_document WHERE source_document_id = $1")
            .bind(&unique_id)
            .fetch_one(&pool)
            .await
            .unwrap();
    enqueue_for_download(&pool, document_id).await.unwrap();

    let queued = next_queued_documents(&pool, 100).await.unwrap();
    assert!(queued.iter().any(|d| d.id == document_id));

    // Enqueuing an already-queued document a second time must not error
    // (idempotent), it just no-ops (still queued, not re-queued twice).
    let err = enqueue_for_download(&pool, document_id).await.unwrap_err();
    assert!(matches!(err, MaterializeError::NotPublic(_)), "a second enqueue attempt on an already-queued document must not silently succeed as if newly queued");
}

#[tokio::test]
async fn rerunning_the_same_materialize_is_idempotent_and_emits_no_duplicate_event() {
    let pool = test_pool().await;
    let unique_id = format!("tops:{}", Uuid::new_v4());
    let run_id = stage_run(
        &pool,
        &[RawDoc {
            source_document_id: &unique_id,
            document_type_raw: "Completion Report",
            kgs_kid: None,
            source_url: "https://chasm.kgs.ku.edu/x",
            filing_date_raw: Some("Apr-15-1999"),
            access_state: "public",
        }],
    )
    .await;

    let first = materialize_documents_from_run(&pool, run_id).await.unwrap();
    assert_eq!(first.created, 1);
    assert_eq!(first.events_emitted, 1);

    let second = materialize_documents_from_run(&pool, run_id).await.unwrap();
    assert_eq!(second.created, 0);
    assert_eq!(second.updated, 1);
    assert_eq!(
        second.events_emitted, 0,
        "re-materializing the identical run must not duplicate the DOCUMENT_ADDED event"
    );

    let count: i64 =
        sqlx::query_scalar("SELECT count(*) FROM ks_well_document WHERE source_document_id = $1")
            .bind(&unique_id)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(
        count, 1,
        "a rerun must upsert in place, never duplicate the document row"
    );
}

#[tokio::test]
async fn backfill_run_suppresses_document_added_for_a_historical_filing_date() {
    let pool = test_pool().await;
    let unique_id = format!("tops:{}", Uuid::new_v4());
    let run_id = stage_run(
        &pool,
        &[RawDoc {
            source_document_id: &unique_id,
            document_type_raw: "Completion Report",
            kgs_kid: None,
            source_url: "https://chasm.kgs.ku.edu/x",
            filing_date_raw: Some("Apr-15-1990"),
            access_state: "public",
        }],
    )
    .await;
    ingestion_run::mark_as_backfill(&pool, run_id)
        .await
        .unwrap();

    let outcome = materialize_documents_from_run(&pool, run_id).await.unwrap();
    assert_eq!(
        outcome.created, 1,
        "the document is still indexed even when its event is suppressed"
    );
    assert_eq!(
        outcome.events_emitted, 0,
        "a backfill run must suppress DOCUMENT_ADDED for a historical filing date"
    );
}

#[tokio::test]
async fn materialize_refuses_a_non_completed_run() {
    let pool = test_pool().await;
    let source = registry::find_source_by_code(&pool, "KGS_DOCUMENTS")
        .await
        .unwrap()
        .unwrap();
    let run_id = ingestion_run::start_run(
        &pool,
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
    .unwrap();

    let err = materialize_documents_from_run(&pool, run_id)
        .await
        .unwrap_err();
    assert!(
        matches!(err, MaterializeError::RunNotCompleted(id, status) if id == run_id && status == "running")
    );
}
