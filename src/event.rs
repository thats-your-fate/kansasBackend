//! KS-06/KS-07/KS-14: the shared event log (`ks_event`). See migration
//! `0014_ks_event.sql` for the scope note on why this is Kansas-local
//! rather than a confirmed `futureBackend` shared event system, and
//! `docs/ops/ks-12-13-state-export-live-smoke.md` for how these events
//! reach the shared platform anyway (`state_export::exporter::export_events`).

use chrono::{DateTime, Utc};
use serde_json::Value;
use sqlx::PgPool;
use uuid::Uuid;

pub const INTENT_POSTED: &str = "INTENT_POSTED";
pub const INTENT_EXPIRED: &str = "INTENT_EXPIRED";
pub const SPUD_RECORDED: &str = "SPUD_RECORDED";
pub const COMPLETION_RECORDED: &str = "COMPLETION_RECORDED";
pub const WELL_STATUS_CHANGED: &str = "WELL_STATUS_CHANGED";
pub const PLUGGING_RECORDED: &str = "PLUGGING_RECORDED";
pub const OPERATOR_TRANSFER_APPROVED: &str = "OPERATOR_TRANSFER_APPROVED";
pub const FIRST_LEASE_PRODUCTION_REPORTED: &str = "FIRST_LEASE_PRODUCTION_REPORTED";
pub const LEASE_PRODUCTION_UPDATED: &str = "LEASE_PRODUCTION_UPDATED";
/// KS-15 emits this once a document metadata index exists — the type is
/// reserved here (and valid per the `ks_event_type_check` constraint,
/// migration `0023`) so KS-14's schema work doesn't need to be revisited
/// when KS-15 lands.
pub const DOCUMENT_ADDED: &str = "DOCUMENT_ADDED";

/// Reserved, not yet emitted by any importer in this repo (KS-14 task
/// doc: "Reserve future event types"). Valid per the database CHECK
/// constraint so a future importer can start emitting them without a
/// migration, and so nothing else can collide with these names meanwhile.
pub const CONSERVATION_DOCKET_FILED: &str = "CONSERVATION_DOCKET_FILED";
pub const CONSERVATION_ORDER_ISSUED: &str = "CONSERVATION_ORDER_ISSUED";
pub const UIC_REPORT_UPDATED: &str = "UIC_REPORT_UPDATED";

pub struct NewEvent<'a> {
    pub event_type: &'static str,
    /// Natural per-event identity: the dedup mechanism keys on
    /// `(event_type, dedup_key)`, so this must be stable across repeated
    /// daily runs that re-observe the same source record (task doc
    /// acceptance criteria: "event deduplication"). KS-14's "revision-safe
    /// dedupe" acceptance criterion is satisfied by embedding the
    /// observed *value* into this key where a record can be corrected
    /// (e.g. a well-status-change event's key includes both the old and
    /// new status) — a genuine correction produces a new key and a new
    /// event; re-observing the same value produces the same key and is
    /// silently absorbed by `ON CONFLICT DO NOTHING`.
    pub dedup_key: &'a str,
    pub occurred_at: Option<DateTime<Utc>>,
    pub published_at: Option<DateTime<Utc>>,
    pub observed_at: DateTime<Utc>,
    pub payload: Value,
    pub source_ingestion_run_id: Uuid,
    /// KS-14: "Connect events to watch areas, saved counties, operators,
    /// fields and wells." The common single-well case; an event covering
    /// several wells (e.g. one multi-well operator transfer) also uses
    /// `related_well_ids` below instead of/in addition to this field.
    pub related_well_id: Option<Uuid>,
    pub related_operator_id: Option<Uuid>,
    pub related_field_id: Option<Uuid>,
    pub related_lease_id: Option<Uuid>,
    pub related_county: Option<&'a str>,
    /// Additional wells beyond (or instead of) `related_well_id`, recorded
    /// in `ks_event_well_link`. Empty for every single-well event type.
    pub related_well_ids: &'a [Uuid],
}

/// Records an event, deduplicated by `(event_type, dedup_key)`. Returns
/// `true` if this call actually inserted a new event, `false` if an event
/// with this key already existed (the caller re-observed the same source
/// record on a later run — not an error, just a no-op). Well links
/// (`related_well_ids`) are only ever written the first time an event is
/// inserted — an event's relationships don't change after the fact.
pub async fn record_event(pool: &PgPool, event: NewEvent<'_>) -> Result<bool, sqlx::Error> {
    let inserted_id: Option<Uuid> = sqlx::query_scalar(
        r#"
        INSERT INTO ks_event (
            event_type, dedup_key, occurred_at, published_at, observed_at, payload, source_ingestion_run_id,
            related_well_id, related_operator_id, related_field_id, related_lease_id, related_county
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
        ON CONFLICT (event_type, dedup_key) DO NOTHING
        RETURNING id
        "#,
    )
    .bind(event.event_type)
    .bind(event.dedup_key)
    .bind(event.occurred_at)
    .bind(event.published_at)
    .bind(event.observed_at)
    .bind(&event.payload)
    .bind(event.source_ingestion_run_id)
    .bind(event.related_well_id)
    .bind(event.related_operator_id)
    .bind(event.related_field_id)
    .bind(event.related_lease_id)
    .bind(event.related_county)
    .fetch_optional(pool)
    .await?;

    if let Some(event_id) = inserted_id {
        for well_id in event.related_well_ids {
            sqlx::query("INSERT INTO ks_event_well_link (event_id, well_id) VALUES ($1, $2) ON CONFLICT DO NOTHING")
                .bind(event_id)
                .bind(well_id)
                .execute(pool)
                .await?;
        }
    }
    Ok(inserted_id.is_some())
}
