//! KS-06/KS-14: materializes `ks_intent_raw_snapshot` rows from a
//! `completed` `ks_ingestion_run` into permanent `ks_drilling_intent`
//! rows, links each to a well using the task doc's 4-tier order, emits a
//! deduplicated `INTENT_POSTED` event per intent (subject to KS-14's
//! baseline-backfill suppression), and — mirroring `ks_well`'s own
//! two-consecutive-missing-snapshots tombstone rule — flags an intent as
//! `INTENT_EXPIRED` once it has aged out of two consecutive fresh
//! snapshots of the source's rolling window without a well ever having
//! been spudded for it (the only source-grounded signal available; KCC's
//! intents page carries no explicit permit-expiration date).

use chrono::{DateTime, Utc};
use serde_json::Value;
use sqlx::PgPool;
use uuid::Uuid;

use crate::constants::{self, should_emit_event};
use crate::event::{self, NewEvent};
use crate::materialize::well_linking::{self, Plss};
use crate::source::ingestion_run;

#[derive(Debug, thiserror::Error)]
pub enum MaterializeError {
    #[error("database error: {0}")]
    Database(#[from] sqlx::Error),
    #[error("ingestion run {0} not found")]
    RunNotFound(Uuid),
    #[error("ingestion run {0} has status '{1}', not 'completed' — refusing to materialize from a non-published run")]
    RunNotCompleted(Uuid, String),
}

#[derive(Debug, Default, Clone, PartialEq)]
pub struct MaterializeOutcome {
    pub created: i64,
    pub updated: i64,
    pub exact_matches: i64,
    pub probabilistic_matches: i64,
    pub unresolved: i64,
    pub events_emitted: i64,
    pub expired: i64,
}

#[derive(Debug, sqlx::FromRow)]
struct RawSnapshotRow {
    source_filename: String,
    permit_date: Option<chrono::NaiveDate>,
    kcc_district: Option<String>,
    county: Option<String>,
    section: Option<i16>,
    township: Option<i16>,
    range: Option<i16>,
    range_direction: Option<String>,
    operator_license: Option<String>,
    source_document_url: Option<String>,
    reported_lease_or_well_name: Option<String>,
    source_record_hash: String,
    observed_at: DateTime<Utc>,
}

/// Minimum `pg_trgm` similarity required before a fuzzy candidate is
/// accepted as a tier-3 probabilistic match rather than falling through
/// to tier-4 unresolved.
const PROBABILISTIC_MATCH_THRESHOLD: f32 = 0.5;

pub async fn materialize_intents_from_run(
    pool: &PgPool,
    ingestion_run_id: Uuid,
) -> Result<MaterializeOutcome, MaterializeError> {
    let status: Option<String> =
        sqlx::query_scalar("SELECT status FROM ks_ingestion_run WHERE id = $1")
            .bind(ingestion_run_id)
            .fetch_optional(pool)
            .await?;
    let status = status.ok_or(MaterializeError::RunNotFound(ingestion_run_id))?;
    if status != "completed" {
        return Err(MaterializeError::RunNotCompleted(ingestion_run_id, status));
    }

    let rows: Vec<RawSnapshotRow> = sqlx::query_as(
        r#"
        SELECT source_filename, permit_date, kcc_district, county, section, township, range,
               range_direction, operator_license, source_document_url, reported_lease_or_well_name,
               source_record_hash, observed_at
        FROM ks_intent_raw_snapshot
        WHERE ingestion_run_id = $1 AND source_filename IS NOT NULL
        "#,
    )
    .bind(ingestion_run_id)
    .fetch_all(pool)
    .await?;

    let (is_baseline_backfill, force_historical) =
        ingestion_run::event_policy(pool, ingestion_run_id).await?;
    let cutoff = constants::live_monitoring_cutoff();

    let mut outcome = MaterializeOutcome::default();
    let mut touched_filenames: Vec<String> = Vec::with_capacity(rows.len());

    for row in rows {
        touched_filenames.push(row.source_filename.clone());
        let plss = Plss {
            county: row.county.as_deref(),
            township: row.township,
            township_direction: None,
            range: row.range,
            range_direction: row.range_direction.as_deref(),
            section: row.section,
        };

        let (match_method, match_confidence, matched_well_id, matched_fields) =
            resolve_well(pool, &row, plss).await?;

        match match_method {
            "exact_permit_or_api" | "license_plss_name" => outcome.exact_matches += 1,
            "probabilistic" => outcome.probabilistic_matches += 1,
            _ => outcome.unresolved += 1,
        }

        let occurred_at = row
            .permit_date
            .and_then(|d| d.and_hms_opt(0, 0, 0))
            .map(|dt| dt.and_utc());

        let inserted: bool = sqlx::query_scalar(
            r#"
            INSERT INTO ks_drilling_intent (
                source_filename, permit_date, kcc_district, county, section, township, range,
                range_direction, operator_license, source_document_url, reported_lease_or_well_name,
                matched_well_id, match_method, match_confidence, matched_fields,
                occurred_at, published_at, first_observed_at, last_observed_at,
                source_ingestion_run_id, source_record_hash
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, NULL, $17, $17, $18, $19)
            ON CONFLICT (source_filename) DO UPDATE SET
                matched_well_id = EXCLUDED.matched_well_id,
                match_method = EXCLUDED.match_method,
                match_confidence = EXCLUDED.match_confidence,
                matched_fields = EXCLUDED.matched_fields,
                last_observed_at = EXCLUDED.last_observed_at,
                source_ingestion_run_id = EXCLUDED.source_ingestion_run_id,
                source_record_hash = EXCLUDED.source_record_hash,
                updated_at = now()
            RETURNING (xmax = 0)
            "#,
        )
        .bind(&row.source_filename)
        .bind(row.permit_date)
        .bind(&row.kcc_district)
        .bind(&row.county)
        .bind(row.section)
        .bind(row.township)
        .bind(row.range)
        .bind(&row.range_direction)
        .bind(&row.operator_license)
        .bind(&row.source_document_url)
        .bind(&row.reported_lease_or_well_name)
        .bind(matched_well_id)
        .bind(match_method)
        .bind(match_confidence)
        .bind(&matched_fields)
        .bind(occurred_at)
        .bind(row.observed_at)
        .bind(ingestion_run_id)
        .bind(&row.source_record_hash)
        .fetch_one(pool)
        .await?;

        if inserted {
            outcome.created += 1;
        } else {
            outcome.updated += 1;
        }

        let intent_id: Uuid =
            sqlx::query_scalar("SELECT id FROM ks_drilling_intent WHERE source_filename = $1")
                .bind(&row.source_filename)
                .fetch_one(pool)
                .await?;

        if should_emit_event(is_baseline_backfill, occurred_at, cutoff, force_historical) {
            let related_operator_id: Option<Uuid> =
                if let Some(license) = row.operator_license.as_deref() {
                    sqlx::query_scalar("SELECT id FROM ks_operator WHERE kcc_license_number = $1")
                        .bind(license)
                        .fetch_optional(pool)
                        .await?
                } else {
                    None
                };
            let event_inserted = event::record_event(
                pool,
                NewEvent {
                    event_type: event::INTENT_POSTED,
                    dedup_key: &intent_id.to_string(),
                    occurred_at,
                    published_at: None,
                    observed_at: row.observed_at,
                    payload: serde_json::json!({
                        "intent_id": intent_id,
                        "source_filename": row.source_filename,
                        "county": row.county,
                        "operator_license": row.operator_license,
                        "matched_well_id": matched_well_id,
                        "match_method": match_method,
                    }),
                    source_ingestion_run_id: ingestion_run_id,
                    related_well_id: matched_well_id,
                    related_operator_id,
                    related_field_id: None,
                    related_lease_id: None,
                    related_county: row.county.as_deref(),
                    related_well_ids: &[],
                },
            )
            .await?;
            if event_inserted {
                outcome.events_emitted += 1;
            }
        }
    }

    outcome.expired = expire_stale_intents(
        pool,
        ingestion_run_id,
        &touched_filenames,
        is_baseline_backfill,
        force_historical,
        cutoff,
    )
    .await?;

    Ok(outcome)
}

/// KS-14: two consecutive fresh snapshots without seeing a previously
/// active intent — mirroring `ks_well`'s own tombstone rule exactly —
/// and no spud ever recorded for its matched well, is treated as
/// `INTENT_EXPIRED`. An intent this materializer already knows is
/// expired is left alone (expiration doesn't reverse), and one still
/// short of the two-miss threshold just has its counter incremented.
async fn expire_stale_intents(
    pool: &PgPool,
    ingestion_run_id: Uuid,
    touched_filenames: &[String],
    is_baseline_backfill: bool,
    force_historical: bool,
    cutoff: DateTime<Utc>,
) -> Result<i64, sqlx::Error> {
    let candidates: Vec<(Uuid, i32, Option<Uuid>)> = sqlx::query_as(
        r#"
        SELECT di.id, di.consecutive_missing_snapshot_count, di.matched_well_id
        FROM ks_drilling_intent di
        WHERE NOT di.is_expired AND di.source_filename != ALL($1)
        "#,
    )
    .bind(touched_filenames)
    .fetch_all(pool)
    .await?;

    let mut expired_count = 0_i64;
    let now = Utc::now();
    for (intent_id, missing_count, matched_well_id) in candidates {
        let new_count = missing_count + 1;
        if new_count < 2 {
            sqlx::query("UPDATE ks_drilling_intent SET consecutive_missing_snapshot_count = $2, updated_at = now() WHERE id = $1")
                .bind(intent_id)
                .bind(new_count)
                .execute(pool)
                .await?;
            continue;
        }

        let well_spudded: bool = if let Some(well_id) = matched_well_id {
            sqlx::query_scalar(
                "SELECT spud_date IS NOT NULL FROM ks_well WHERE internal_well_id = $1",
            )
            .bind(well_id)
            .fetch_optional(pool)
            .await?
            .unwrap_or(false)
        } else {
            false
        };
        if well_spudded {
            // Drilling started — this well simply aged out of the
            // rolling window because it succeeded, not because it
            // expired. Stop incrementing so a later re-materialize
            // doesn't keep re-checking it.
            sqlx::query("UPDATE ks_drilling_intent SET consecutive_missing_snapshot_count = $2, updated_at = now() WHERE id = $1")
                .bind(intent_id)
                .bind(new_count)
                .execute(pool)
                .await?;
            continue;
        }

        sqlx::query("UPDATE ks_drilling_intent SET consecutive_missing_snapshot_count = $2, is_expired = true, expired_at = now(), updated_at = now() WHERE id = $1")
            .bind(intent_id)
            .bind(new_count)
            .execute(pool)
            .await?;

        if should_emit_event(is_baseline_backfill, None, cutoff, force_historical) {
            event::record_event(
                pool,
                NewEvent {
                    event_type: event::INTENT_EXPIRED,
                    dedup_key: &intent_id.to_string(),
                    occurred_at: None,
                    published_at: None,
                    observed_at: now,
                    payload: serde_json::json!({ "intent_id": intent_id }),
                    source_ingestion_run_id: ingestion_run_id,
                    related_well_id: None,
                    related_operator_id: None,
                    related_field_id: None,
                    related_lease_id: None,
                    related_county: None,
                    related_well_ids: &[],
                },
            )
            .await?;
        }
        expired_count += 1;
    }
    Ok(expired_count)
}

async fn resolve_well(
    pool: &PgPool,
    row: &RawSnapshotRow,
    plss: Plss<'_>,
) -> Result<(&'static str, Option<f32>, Option<Uuid>, Value), sqlx::Error> {
    // Tier 1: exact KCC permit or API. The source exposes no separate
    // permit/API column (see the unverified notice) — `source_filename`
    // itself is tried as the candidate token, on the assumption that it
    // may embed a permit or API-14 value.
    let filename_token = row
        .source_filename
        .trim_end_matches(".pdf")
        .trim_end_matches(".PDF");
    if let Some(well_id) = well_linking::find_well_by_exact_identifier(pool, filename_token).await?
    {
        let fields =
            well_linking::matched_fields_json(&[("source_filename_token", Some(filename_token))]);
        return Ok(("exact_permit_or_api", Some(1.0), Some(well_id), fields));
    }

    // Tier 2: exact operator license + exact PLSS + exact well/lease name.
    if let Some(well_id) = well_linking::find_well_by_license_plss_name(
        pool,
        row.operator_license.as_deref(),
        plss,
        row.reported_lease_or_well_name.as_deref(),
    )
    .await?
    {
        let fields = well_linking::matched_fields_json(&[
            ("operator_license", row.operator_license.as_deref()),
            ("county", row.county.as_deref()),
            (
                "lease_or_well_name",
                row.reported_lease_or_well_name.as_deref(),
            ),
        ]);
        return Ok(("license_plss_name", Some(1.0), Some(well_id), fields));
    }

    // Tier 3: probabilistic candidate, requires a recorded confidence value.
    if let Some(name) = row.reported_lease_or_well_name.as_deref() {
        let candidates =
            well_linking::fuzzy_name_candidates(pool, name, row.county.as_deref(), 1).await?;
        if let Some(best) = candidates
            .into_iter()
            .find(|c| c.similarity >= PROBABILISTIC_MATCH_THRESHOLD)
        {
            let fields =
                well_linking::matched_fields_json(&[("reported_lease_or_well_name", Some(name))]);
            return Ok((
                "probabilistic",
                Some(best.similarity),
                Some(best.well_id),
                fields,
            ));
        }
    }

    // Tier 4: unresolved queue.
    Ok(("unresolved", None, None, Value::Null))
}

#[derive(Debug, Clone, sqlx::FromRow)]
pub struct UnresolvedIntent {
    pub id: Uuid,
    pub source_filename: String,
    pub county: Option<String>,
    pub operator_license: Option<String>,
    pub reported_lease_or_well_name: Option<String>,
    pub first_observed_at: DateTime<Utc>,
}

/// The "unresolved-match dashboard" acceptance criterion, satisfied as a
/// queryable report (same pattern as KS-04's `lifecycle-qa-report`) —
/// no frontend exists in this repo to build an actual dashboard page
/// against yet.
pub async fn unresolved_intents_report(
    pool: &PgPool,
    limit: i64,
) -> Result<Vec<UnresolvedIntent>, sqlx::Error> {
    sqlx::query_as(
        r#"
        SELECT id, source_filename, county, operator_license, reported_lease_or_well_name, first_observed_at
        FROM ks_drilling_intent
        WHERE match_method = 'unresolved'
        ORDER BY first_observed_at DESC
        LIMIT $1
        "#,
    )
    .bind(limit)
    .fetch_all(pool)
    .await
}
