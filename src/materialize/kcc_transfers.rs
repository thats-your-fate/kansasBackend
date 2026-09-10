//! KS-07: materializes `ks_transfer_raw_snapshot` rows from a `completed`
//! `ks_ingestion_run` into immutable `ks_operator_transfer` rows, links
//! each to zero or more wells (a transfer may legitimately link to
//! several), and emits a deduplicated `OPERATOR_TRANSFER_APPROVED` event.
//!
//! Fuzzy lease-name candidates are only ever considered when every exact
//! tier found nothing, are always stored with `match_method =
//! 'fuzzy_candidate'`, and are never used anywhere in this codebase to
//! write to `ks_well` or any operator-history table — satisfying the
//! task doc's "must not directly overwrite a well's current operator"
//! structurally (there is nothing here that overwrites operator fields at
//! all).

use std::collections::HashSet;

use chrono::{DateTime, NaiveDate, Utc};
use serde_json::Value;
use sha2::{Digest, Sha256};
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
    pub well_links_created: i64,
    pub unmatched: i64,
    pub events_emitted: i64,
}

#[derive(Debug, sqlx::FromRow)]
struct RawSnapshotRow {
    source_file: String,
    past_operator_name: Option<String>,
    past_operator_license: Option<String>,
    new_operator_name: Option<String>,
    new_operator_license: Option<String>,
    received_date: Option<NaiveDate>,
    effective_date: Option<NaiveDate>,
    approved_date: Option<NaiveDate>,
    lease_or_well_label: Option<String>,
    section: Option<i16>,
    township: Option<i16>,
    range: Option<i16>,
    range_direction: Option<String>,
    district: Option<String>,
    county: Option<String>,
    observed_at: DateTime<Utc>,
}

pub async fn materialize_transfers_from_run(
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
        SELECT source_file, past_operator_name, past_operator_license, new_operator_name, new_operator_license,
               received_date, effective_date, approved_date, lease_or_well_label, section, township, range,
               range_direction, district, county, observed_at
        FROM ks_transfer_raw_snapshot
        WHERE ingestion_run_id = $1
        "#,
    )
    .bind(ingestion_run_id)
    .fetch_all(pool)
    .await?;

    let (is_baseline_backfill, force_historical) =
        ingestion_run::event_policy(pool, ingestion_run_id).await?;
    let cutoff = constants::live_monitoring_cutoff();

    let mut outcome = MaterializeOutcome::default();

    for row in rows {
        let inserted: bool = sqlx::query_scalar(
            r#"
            INSERT INTO ks_operator_transfer (
                source_file, past_operator_name, past_operator_license, new_operator_name, new_operator_license,
                received_date, effective_date, approved_date, lease_or_well_label, section, township, range,
                range_direction, district, county, source_ingestion_run_id, source_record_hash,
                first_observed_at, last_observed_at
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $18)
            ON CONFLICT (
                (COALESCE(past_operator_license, '')),
                (COALESCE(new_operator_license, '')),
                (COALESCE(effective_date, '0001-01-01'::date)),
                (COALESCE(lease_or_well_label, '')),
                (COALESCE(county, ''))
            ) DO UPDATE SET
                last_observed_at = EXCLUDED.last_observed_at,
                source_ingestion_run_id = EXCLUDED.source_ingestion_run_id
            RETURNING (xmax = 0)
            "#,
        )
        .bind(&row.source_file)
        .bind(&row.past_operator_name)
        .bind(&row.past_operator_license)
        .bind(&row.new_operator_name)
        .bind(&row.new_operator_license)
        .bind(row.received_date)
        .bind(row.effective_date)
        .bind(row.approved_date)
        .bind(&row.lease_or_well_label)
        .bind(row.section)
        .bind(row.township)
        .bind(row.range)
        .bind(&row.range_direction)
        .bind(&row.district)
        .bind(&row.county)
        .bind(ingestion_run_id)
        .bind(source_record_hash(&row))
        .bind(row.observed_at)
        .fetch_one(pool)
        .await?;

        if inserted {
            outcome.created += 1;
        } else {
            outcome.updated += 1;
        }

        let transfer_id: Uuid = sqlx::query_scalar(
            r#"
            SELECT id FROM ks_operator_transfer
            WHERE past_operator_license IS NOT DISTINCT FROM $1
              AND new_operator_license IS NOT DISTINCT FROM $2
              AND effective_date IS NOT DISTINCT FROM $3
              AND lease_or_well_label IS NOT DISTINCT FROM $4
              AND county IS NOT DISTINCT FROM $5
            "#,
        )
        .bind(&row.past_operator_license)
        .bind(&row.new_operator_license)
        .bind(row.effective_date)
        .bind(&row.lease_or_well_label)
        .bind(&row.county)
        .fetch_one(pool)
        .await?;

        let plss = Plss {
            county: row.county.as_deref(),
            township: row.township,
            township_direction: None,
            range: row.range,
            range_direction: row.range_direction.as_deref(),
            section: row.section,
        };
        let links = resolve_wells_for_transfer(pool, &row, plss).await?;

        if links.is_empty() {
            outcome.unmatched += 1;
        }

        for (well_id, method, confidence, matched_fields) in &links {
            let result = sqlx::query(
                r#"
                INSERT INTO ks_operator_transfer_well (transfer_id, well_id, match_method, match_confidence, matched_fields)
                VALUES ($1, $2, $3, $4, $5)
                ON CONFLICT (transfer_id, well_id) DO NOTHING
                "#,
            )
            .bind(transfer_id)
            .bind(well_id)
            .bind(*method)
            .bind(confidence)
            .bind(matched_fields)
            .execute(pool)
            .await?;
            if result.rows_affected() > 0 {
                outcome.well_links_created += 1;
            }
        }

        let occurred_at = row
            .effective_date
            .or(row.approved_date)
            .and_then(|d| d.and_hms_opt(0, 0, 0))
            .map(|dt| dt.and_utc());

        if should_emit_event(is_baseline_backfill, occurred_at, cutoff, force_historical) {
            let related_operator_id: Option<Uuid> =
                if let Some(license) = row.new_operator_license.as_deref() {
                    sqlx::query_scalar("SELECT id FROM ks_operator WHERE kcc_license_number = $1")
                        .bind(license)
                        .fetch_optional(pool)
                        .await?
                } else {
                    None
                };
            // Fuzzy candidates are internal QA/dashboard evidence only
            // (see this module's own doc comment) — a watch on a specific
            // well must never fire off a match this codebase itself
            // doesn't trust enough to write anywhere else.
            let confirmed_well_ids: Vec<Uuid> = links
                .iter()
                .filter(|(_, method, _, _)| *method != "fuzzy_candidate")
                .map(|(id, _, _, _)| *id)
                .collect();

            let event_inserted = event::record_event(
                pool,
                NewEvent {
                    event_type: event::OPERATOR_TRANSFER_APPROVED,
                    dedup_key: &transfer_id.to_string(),
                    occurred_at,
                    published_at: None,
                    observed_at: row.observed_at,
                    payload: serde_json::json!({
                        "transfer_id": transfer_id,
                        "past_operator_license": row.past_operator_license,
                        "new_operator_license": row.new_operator_license,
                        "lease_or_well_label": row.lease_or_well_label,
                        "matched_well_ids": links.iter().map(|(id, _, _, _)| *id).collect::<Vec<_>>(),
                    }),
                    source_ingestion_run_id: ingestion_run_id,
                    related_well_id: None,
                    related_operator_id,
                    related_field_id: None,
                    related_lease_id: None,
                    related_county: row.county.as_deref(),
                    related_well_ids: &confirmed_well_ids,
                },
            )
            .await?;
            if event_inserted {
                outcome.events_emitted += 1;
            }
        }
    }

    Ok(outcome)
}

fn source_record_hash(row: &RawSnapshotRow) -> String {
    let mut hasher = Sha256::new();
    hasher.update(row.past_operator_license.as_deref().unwrap_or(""));
    hasher.update(b"|");
    hasher.update(row.new_operator_license.as_deref().unwrap_or(""));
    hasher.update(b"|");
    hasher.update(
        row.effective_date
            .map(|d| d.to_string())
            .unwrap_or_default(),
    );
    hasher.update(b"|");
    hasher.update(row.lease_or_well_label.as_deref().unwrap_or(""));
    hasher.update(b"|");
    hasher.update(row.county.as_deref().unwrap_or(""));
    format!("{:x}", hasher.finalize())
}

/// Runs every exact tier (not just the first hit) so a transfer can
/// legitimately collect links to more than one well from different
/// evidence — then falls back to fuzzy lease-name candidates only when no
/// exact tier found anything at all.
async fn resolve_wells_for_transfer(
    pool: &PgPool,
    row: &RawSnapshotRow,
    plss: Plss<'_>,
) -> Result<Vec<(Uuid, &'static str, Option<f32>, Value)>, sqlx::Error> {
    let mut links = Vec::new();
    let mut seen = HashSet::new();

    // "KCC/DOR identifiers" — the source exposes no separate identifier
    // column (see the unverified notice); the lease/well label is tried
    // as a candidate token on the chance it embeds one.
    if let Some(label) = row.lease_or_well_label.as_deref() {
        if let Some(id) = well_linking::find_well_by_exact_identifier(pool, label).await? {
            if seen.insert(id) {
                links.push((
                    id,
                    "kcc_dor_identifier",
                    Some(1.0),
                    well_linking::matched_fields_json(&[("lease_or_well_label", Some(label))]),
                ));
            }
        }
    }

    if let Some(id) = well_linking::find_well_by_exact_plss(pool, plss).await? {
        if seen.insert(id) {
            let fields = well_linking::matched_fields_json(&[("county", row.county.as_deref())]);
            links.push((id, "exact_plss", Some(1.0), fields));
        }
    }

    if let Some(label) = row.lease_or_well_label.as_deref() {
        if let Some(id) =
            well_linking::find_well_by_exact_lease_name(pool, label, row.county.as_deref()).await?
        {
            if seen.insert(id) {
                links.push((
                    id,
                    "exact_lease_name",
                    Some(1.0),
                    well_linking::matched_fields_json(&[("lease_or_well_label", Some(label))]),
                ));
            }
        }
    }

    if let Some(license) = row.new_operator_license.as_deref() {
        if let Some(id) = well_linking::find_well_by_operator_license(pool, license).await? {
            if seen.insert(id) {
                links.push((
                    id,
                    "exact_operator_license",
                    Some(1.0),
                    well_linking::matched_fields_json(&[("new_operator_license", Some(license))]),
                ));
            }
        }
    }

    if links.is_empty() {
        if let Some(label) = row.lease_or_well_label.as_deref() {
            let candidates =
                well_linking::fuzzy_name_candidates(pool, label, row.county.as_deref(), 3).await?;
            for candidate in candidates {
                if seen.insert(candidate.well_id) {
                    let fields =
                        well_linking::matched_fields_json(&[("lease_or_well_label", Some(label))]);
                    links.push((
                        candidate.well_id,
                        "fuzzy_candidate",
                        Some(candidate.similarity),
                        fields,
                    ));
                }
            }
        }
    }

    Ok(links)
}

#[derive(Debug, Clone, sqlx::FromRow)]
pub struct UnmatchedTransfer {
    pub id: Uuid,
    pub past_operator_license: Option<String>,
    pub new_operator_license: Option<String>,
    pub lease_or_well_label: Option<String>,
    pub county: Option<String>,
    pub first_observed_at: DateTime<Utc>,
}

/// "Unmatched transfers remain searchable internally" — a queryable
/// report, same pattern as KS-06's unresolved-intents report.
pub async fn unmatched_transfers_report(
    pool: &PgPool,
    limit: i64,
) -> Result<Vec<UnmatchedTransfer>, sqlx::Error> {
    sqlx::query_as(
        r#"
        SELECT t.id, t.past_operator_license, t.new_operator_license, t.lease_or_well_label, t.county, t.first_observed_at
        FROM ks_operator_transfer t
        WHERE NOT EXISTS (SELECT 1 FROM ks_operator_transfer_well w WHERE w.transfer_id = t.id)
        ORDER BY t.first_observed_at DESC
        LIMIT $1
        "#,
    )
    .bind(limit)
    .fetch_all(pool)
    .await
}
