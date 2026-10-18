//! KS-10/KS-14: materializes `ks_lease_production_raw_snapshot` rows from
//! a `completed` `ks_ingestion_run` into `ks_lease_production_month`.
//! Processes in pages (keyset pagination on `id`) with batched
//! multi-row upserts, the same throughput discipline as the parser —
//! real files have hundreds of thousands of rows, so a per-row upsert
//! loop (KS-08/KS-09's documented-but-unfixed pattern) is not an option
//! here. KS-14's event emission preserves that discipline: on a
//! `is_baseline_backfill` run (the only case with millions of rows in
//! practice) it is skipped entirely — no per-row cost is added to the
//! proven 11.68M-row batch path — and only runs its own small per-page
//! existence check on an ordinary live/incremental run, whose volume is
//! always small in practice (a monthly update file, never a full archive).

use std::collections::HashSet;

use sqlx::{PgPool, Row};
use uuid::Uuid;

use crate::constants::{self, classify_production_month, should_emit_event};
use crate::event::{self, NewEvent};
use crate::normalize::kgs_production::parse_product;
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
    pub skipped_unmapped_month: i64,
    pub skipped_product_mismatch: i64,
    pub events_emitted: i64,
}

#[derive(Debug, Clone, sqlx::FromRow)]
struct RawSnapshotRow {
    id: Uuid,
    lease_kid: String,
    reported_lease_name: Option<String>,
    dor_code: Option<String>,
    api_number_raw: Option<String>,
    field_name: Option<String>,
    producing_zone: Option<String>,
    operator: Option<String>,
    county: Option<String>,
    township: Option<i16>,
    township_direction: Option<String>,
    range: Option<i16>,
    range_direction: Option<String>,
    section: Option<i16>,
    spot: Option<String>,
    source_latitude: Option<f64>,
    source_longitude: Option<f64>,
    year: Option<i32>,
    month: Option<i16>,
    product_raw: Option<String>,
    reported_contributing_wells: Option<i32>,
    production_volume: Option<f64>,
    source_url: Option<String>,
    source_record_hash: String,
}

const PAGE_SIZE: i64 = 1000;

/// `(id, lease_kid, product, year, month, record_kind, production_volume, was_inserted)`.
type UpsertedProductionRow = (Uuid, String, String, i32, i16, String, f64, bool);

pub async fn materialize_production_from_run(
    pool: &PgPool,
    ingestion_run_id: Uuid,
) -> Result<MaterializeOutcome, MaterializeError> {
    materialize_production_from_run_for_year(pool, ingestion_run_id, None).await
}

/// Restrict a staged archive to one reporting year without processing its history.
pub async fn materialize_production_from_run_for_year(
    pool: &PgPool,
    ingestion_run_id: Uuid,
    year: Option<i32>,
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

    let (is_baseline_backfill, force_historical) =
        ingestion_run::event_policy(pool, ingestion_run_id).await?;
    if is_baseline_backfill {
        // Historical archives are already validated in staging. Keep the transfer
        // inside Postgres instead of shipping millions of rows out and back.
        sqlx::query("ANALYZE ks_lease_production_raw_snapshot")
            .execute(pool)
            .await?;
        let counts = sqlx::query(include_str!("production_backfill.sql"))
            .bind(ingestion_run_id)
            .bind(year)
            .fetch_one(pool)
            .await?;
        return Ok(MaterializeOutcome {
            created: counts.get("created"),
            updated: counts.get("updated"),
            skipped_unmapped_month: counts.get("skipped_unmapped_month"),
            skipped_product_mismatch: counts.get("skipped_product_mismatch"),
            events_emitted: 0,
        });
    }
    let cutoff = constants::live_monitoring_cutoff();

    let mut outcome = MaterializeOutcome::default();
    let mut after_id: Option<Uuid> = None;

    loop {
        let rows: Vec<RawSnapshotRow> = sqlx::query_as(
            r#"
            SELECT id, lease_kid, reported_lease_name, dor_code, api_number_raw, field_name, producing_zone,
                   operator, county, township, township_direction, range, range_direction, section, spot,
                   source_latitude, source_longitude, year, month, product_raw, reported_contributing_wells,
                   production_volume, source_url, source_record_hash
            FROM ks_lease_production_raw_snapshot
            WHERE ingestion_run_id = $1 AND lease_kid IS NOT NULL AND rejection_reason IS NULL
              AND ($2::uuid IS NULL OR id > $2)
              AND ($4::integer IS NULL OR year = $4)
            ORDER BY id
            LIMIT $3
            "#,
        )
        .bind(ingestion_run_id)
        .bind(after_id)
        .bind(PAGE_SIZE)
        .bind(year)
        .fetch_all(pool)
        .await?;

        if rows.is_empty() {
            break;
        }
        after_id = rows.last().map(|r| r.id);

        let lease_kids: Vec<&str> = rows.iter().map(|r| r.lease_kid.as_str()).collect();
        let lease_id_pairs: Vec<(String, Uuid)> =
            sqlx::query_as("SELECT lease_kid, id FROM ks_lease WHERE lease_kid = ANY($1)")
                .bind(&lease_kids)
                .fetch_all(pool)
                .await?;
        let lease_ids: std::collections::HashMap<String, Uuid> =
            lease_id_pairs.into_iter().collect();

        materialize_page(
            pool,
            ingestion_run_id,
            &rows,
            &lease_ids,
            is_baseline_backfill,
            force_historical,
            cutoff,
            &mut outcome,
        )
        .await?;
    }

    Ok(outcome)
}

#[allow(clippy::too_many_arguments)]
async fn materialize_page(
    pool: &PgPool,
    ingestion_run_id: Uuid,
    rows: &[RawSnapshotRow],
    lease_ids: &std::collections::HashMap<String, Uuid>,
    is_baseline_backfill: bool,
    force_historical: bool,
    cutoff: chrono::DateTime<chrono::Utc>,
    outcome: &mut MaterializeOutcome,
) -> Result<(), sqlx::Error> {
    struct PreparedRow<'a> {
        row: &'a RawSnapshotRow,
        lease_id: Option<Uuid>,
        record_kind: &'static str,
        product: &'static str,
    }

    let mut prepared = Vec::with_capacity(rows.len());
    for row in rows {
        let Some(month) = row.month else { continue };
        let Some(record_kind) = classify_production_month(month) else {
            outcome.skipped_unmapped_month += 1;
            continue;
        };
        let Some(product) = parse_product(row.product_raw.as_deref()) else {
            outcome.skipped_product_mismatch += 1;
            continue;
        };
        prepared.push(PreparedRow {
            row,
            lease_id: lease_ids.get(&row.lease_kid).copied(),
            record_kind: match record_kind {
                crate::constants::ProductionRecordKind::Monthly => "monthly",
                crate::constants::ProductionRecordKind::YearlyTotal => "yearly_total",
                crate::constants::ProductionRecordKind::StartingCumulative => "starting_cumulative",
            },
            product: product.as_db_str(),
        });
    }

    if prepared.is_empty() {
        return Ok(());
    }

    let mut qb = sqlx::QueryBuilder::new(
        "INSERT INTO ks_lease_production_month (
            lease_id, lease_kid, reported_lease_name, dor_code, api_number_raw, field_name, producing_zone,
            operator, county, township, township_direction, range, range_direction, section, spot,
            source_latitude, source_longitude, year, month, record_kind, product, reported_contributing_wells,
            production_volume, source_url, source_ingestion_run_id, source_record_hash
        ) ",
    );
    qb.push_values(prepared.iter(), |mut b, p| {
        b.push_bind(p.lease_id)
            .push_bind(&p.row.lease_kid)
            .push_bind(&p.row.reported_lease_name)
            .push_bind(&p.row.dor_code)
            .push_bind(&p.row.api_number_raw)
            .push_bind(&p.row.field_name)
            .push_bind(&p.row.producing_zone)
            .push_bind(&p.row.operator)
            .push_bind(&p.row.county)
            .push_bind(p.row.township)
            .push_bind(&p.row.township_direction)
            .push_bind(p.row.range)
            .push_bind(&p.row.range_direction)
            .push_bind(p.row.section)
            .push_bind(&p.row.spot)
            .push_bind(p.row.source_latitude)
            .push_bind(p.row.source_longitude)
            .push_bind(p.row.year)
            .push_bind(p.row.month)
            .push_bind(p.record_kind)
            .push_bind(p.product)
            .push_bind(p.row.reported_contributing_wells)
            .push_bind(p.row.production_volume)
            .push_bind(&p.row.source_url)
            .push_bind(ingestion_run_id)
            .push_bind(&p.row.source_record_hash);
    });
    qb.push(
        r#"
        ON CONFLICT (lease_kid, product, year, month) DO UPDATE SET
            lease_id = COALESCE(EXCLUDED.lease_id, ks_lease_production_month.lease_id),
            reported_lease_name = EXCLUDED.reported_lease_name,
            dor_code = EXCLUDED.dor_code,
            api_number_raw = EXCLUDED.api_number_raw,
            field_name = EXCLUDED.field_name,
            producing_zone = EXCLUDED.producing_zone,
            operator = EXCLUDED.operator,
            county = EXCLUDED.county,
            township = EXCLUDED.township,
            township_direction = EXCLUDED.township_direction,
            range = EXCLUDED.range,
            range_direction = EXCLUDED.range_direction,
            section = EXCLUDED.section,
            spot = EXCLUDED.spot,
            source_latitude = EXCLUDED.source_latitude,
            source_longitude = EXCLUDED.source_longitude,
            record_kind = EXCLUDED.record_kind,
            reported_contributing_wells = EXCLUDED.reported_contributing_wells,
            production_volume = EXCLUDED.production_volume,
            source_url = EXCLUDED.source_url,
            source_ingestion_run_id = EXCLUDED.source_ingestion_run_id,
            source_record_hash = EXCLUDED.source_record_hash,
            last_observed_at = now(),
            updated_at = now()
        RETURNING id, lease_kid, product, year, month, record_kind, production_volume, (xmax = 0)
        "#,
    );

    let upserted: Vec<UpsertedProductionRow> = qb.build_query_as().fetch_all(pool).await?;

    let mut monthly_keys: HashSet<(String, String)> = HashSet::new();
    for (_, lease_kid, product, _, _, record_kind, _, inserted) in &upserted {
        if *inserted {
            outcome.created += 1;
        } else {
            outcome.updated += 1;
        }
        if record_kind == "monthly" {
            monthly_keys.insert((lease_kid.clone(), product.clone()));
        }
    }

    if is_baseline_backfill {
        // Skip the event step entirely on the proven large-scale path —
        // see this module's own doc comment.
        return Ok(());
    }

    // Two things must both be idempotent across repeated/replayed runs,
    // and they are NOT the same question: (1) has this *exact* fact
    // (lease_kid, product, year, month, volume) already been notified
    // about, under *either* event type — if so, skip entirely, this is a
    // pure re-observation, not news; (2) if it's genuinely new (a
    // different volume than whatever was last recorded for that month,
    // or a month that's never been recorded before), is this
    // (lease_kid, product)'s very first fact ever, or a later one. An
    // earlier version of this used a per-fact dedup key that could
    // legitimately collide across event types (the same volume observed
    // once as "first" and later re-observed would get reclassified as
    // "updated" with the identical dedup string, which — being a
    // different event_type — was NOT caught by `(event_type, dedup_key)`
    // uniqueness and produced a spurious extra notification). Resolving
    // (1) before ever deciding (2) avoids that: a full-fact duplicate is
    // filtered out before an event_type is even chosen for it.
    let mut full_facts_seen: HashSet<String> = HashSet::new();
    let mut lease_product_has_any_event: HashSet<(String, String)> = HashSet::new();
    if !monthly_keys.is_empty() {
        let lease_kids: Vec<String> = monthly_keys.iter().map(|(k, _)| k.clone()).collect();
        let existing: Vec<(String, String)> = sqlx::query_as(
            "SELECT event_type, dedup_key FROM ks_event WHERE event_type = ANY($1) AND split_part(dedup_key, ':', 1) = ANY($2)",
        )
        .bind([event::FIRST_LEASE_PRODUCTION_REPORTED, event::LEASE_PRODUCTION_UPDATED])
        .bind(&lease_kids)
        .fetch_all(pool)
        .await?;
        for (_, dedup_key) in existing {
            let mut parts = dedup_key.splitn(3, ':');
            if let (Some(lease_kid), Some(product)) = (parts.next(), parts.next()) {
                lease_product_has_any_event.insert((lease_kid.to_owned(), product.to_owned()));
            }
            full_facts_seen.insert(dedup_key);
        }
    }

    let observed_at = chrono::Utc::now();
    for (id, lease_kid, product, year, month, record_kind, volume, _inserted) in upserted {
        if record_kind != "monthly" {
            continue;
        }
        let dedup_key = format!("{lease_kid}:{product}:{year}:{month}:{volume}");
        if full_facts_seen.contains(&dedup_key) {
            // The exact same fact was already reported (whether as the
            // original "first" or a prior "update") — nothing changed,
            // so there is nothing new to notify about.
            continue;
        }
        let key = (lease_kid.clone(), product.clone());
        let event_type = if lease_product_has_any_event.contains(&key) {
            event::LEASE_PRODUCTION_UPDATED
        } else {
            event::FIRST_LEASE_PRODUCTION_REPORTED
        };
        lease_product_has_any_event.insert(key);
        full_facts_seen.insert(dedup_key.clone());

        let Some(occurred_at) = chrono::NaiveDate::from_ymd_opt(year, month as u32, 1)
            .and_then(|d| d.and_hms_opt(0, 0, 0))
            .map(|dt| dt.and_utc())
        else {
            continue;
        };
        if !should_emit_event(
            is_baseline_backfill,
            Some(occurred_at),
            cutoff,
            force_historical,
        ) {
            continue;
        }
        let inserted_event = event::record_event(
            pool,
            NewEvent {
                event_type,
                dedup_key: &dedup_key,
                occurred_at: Some(occurred_at),
                published_at: None,
                observed_at,
                payload: serde_json::json!({
                    "production_row_id": id,
                    "lease_kid": lease_kid,
                    "product": product,
                    "year": year,
                    "month": month,
                    "production_volume": volume,
                }),
                source_ingestion_run_id: ingestion_run_id,
                related_well_id: None,
                related_operator_id: None,
                related_field_id: None,
                related_lease_id: lease_ids.get(&lease_kid).copied(),
                related_county: None,
                related_well_ids: &[],
            },
        )
        .await?;
        if inserted_event {
            outcome.events_emitted += 1;
        }
    }

    Ok(())
}

#[derive(Debug, Clone, sqlx::FromRow)]
pub struct CoverageSummary {
    pub latest_oil_year: Option<i32>,
    pub latest_oil_month: Option<i16>,
    pub latest_gas_year: Option<i32>,
    pub latest_gas_month: Option<i16>,
    pub last_successful_import: Option<chrono::DateTime<chrono::Utc>>,
}

/// Task doc: "Build source-coverage metadata such as: latest_oil_month,
/// latest_gas_month, source_publication_date, last_successful_import."
/// Computed on demand from the tables that already carry this
/// information, rather than a separately-maintained summary table that
/// could drift out of sync.
pub async fn coverage_summary(pool: &PgPool) -> Result<CoverageSummary, sqlx::Error> {
    let latest_oil: Option<(i32, i16)> = sqlx::query_as(
        "SELECT year, month FROM ks_lease_production_month WHERE product = 'oil' AND record_kind = 'monthly' ORDER BY year DESC, month DESC LIMIT 1",
    )
    .fetch_optional(pool)
    .await?;
    let latest_gas: Option<(i32, i16)> = sqlx::query_as(
        "SELECT year, month FROM ks_lease_production_month WHERE product = 'gas' AND record_kind = 'monthly' ORDER BY year DESC, month DESC LIMIT 1",
    )
    .fetch_optional(pool)
    .await?;
    let last_successful_import: Option<chrono::DateTime<chrono::Utc>> = sqlx::query_scalar(
        r#"
        SELECT max(run.completed_at) FROM ks_ingestion_run run
        JOIN ks_source s ON s.id = run.source_id
        WHERE s.code IN ('KGS_PRODUCTION_OIL', 'KGS_PRODUCTION_GAS') AND run.status = 'completed'
        "#,
    )
    .fetch_one(pool)
    .await?;

    Ok(CoverageSummary {
        latest_oil_year: latest_oil.map(|(y, _)| y),
        latest_oil_month: latest_oil.map(|(_, m)| m),
        latest_gas_year: latest_gas.map(|(y, _)| y),
        latest_gas_month: latest_gas.map(|(_, m)| m),
        last_successful_import,
    })
}

#[derive(Debug, Clone, sqlx::FromRow)]
pub struct ReconciliationRow {
    /// `None` for a county/field grouping when the underlying rows have
    /// no county/field recorded — a real possibility, not a query bug.
    /// Always `Some("KS")` for the state grouping.
    pub group_value: Option<String>,
    pub product: String,
    pub year: i32,
    pub monthly_sum: f64,
    pub reported_yearly_total: Option<f64>,
    pub difference: Option<f64>,
    pub within_tolerance: Option<bool>,
}

#[derive(Debug, Clone, Copy)]
pub enum ReconciliationGrouping {
    State,
    County,
    Field,
}

/// Task doc: "Reconcile imported totals against KGS state, county and
/// field totals with configurable tolerances." No separate "official
/// state/county/field total" feed is confirmed to exist for this MVP —
/// this reconciles KGS's own redundant reporting instead: the sum of
/// real monthly (`record_kind = 'monthly'`) rows for a group/product/year
/// against that same group's KGS-reported yearly-total (`record_kind =
/// 'yearly_total'`, month = 0) rows for the same period. A discrepancy
/// here means KGS's own monthly and annual figures disagree, which is
/// exactly the kind of thing this report exists to surface.
pub async fn reconciliation_report(
    pool: &PgPool,
    grouping: ReconciliationGrouping,
    tolerance_ratio: f64,
    limit: i64,
) -> Result<Vec<ReconciliationRow>, sqlx::Error> {
    let group_expr = match grouping {
        ReconciliationGrouping::State => "'KS'",
        ReconciliationGrouping::County => "county",
        ReconciliationGrouping::Field => "field_name",
    };

    let sql = format!(
        r#"
        WITH monthly AS (
            SELECT {group_expr} AS group_value, product, year, sum(production_volume) AS monthly_sum
            FROM ks_lease_production_month
            WHERE record_kind = 'monthly'
            GROUP BY {group_expr}, product, year
        ),
        yearly AS (
            SELECT {group_expr} AS group_value, product, year, sum(production_volume) AS reported_yearly_total
            FROM ks_lease_production_month
            WHERE record_kind = 'yearly_total'
            GROUP BY {group_expr}, product, year
        )
        SELECT
            monthly.group_value,
            monthly.product,
            monthly.year,
            monthly.monthly_sum,
            yearly.reported_yearly_total,
            (monthly.monthly_sum - yearly.reported_yearly_total) AS difference,
            CASE WHEN yearly.reported_yearly_total IS NULL THEN NULL
                 ELSE abs(monthly.monthly_sum - yearly.reported_yearly_total) <= (abs(yearly.reported_yearly_total) * $1)
            END AS within_tolerance
        FROM monthly
        LEFT JOIN yearly ON yearly.group_value = monthly.group_value AND yearly.product = monthly.product AND yearly.year = monthly.year
        ORDER BY monthly.product, monthly.year, monthly.group_value
        LIMIT $2
        "#
    );

    sqlx::query_as(&sql)
        .bind(tolerance_ratio)
        .bind(limit)
        .fetch_all(pool)
        .await
}
