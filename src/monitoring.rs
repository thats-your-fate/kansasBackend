//! KS-17: Kansas freshness, QA, and source-health monitoring. Every
//! check here is a read-only query against tables this repo already
//! maintains (`ks_ingestion_run`/`ks_source` lineage, and the canonical
//! entity tables built by KS-04 through KS-15) — this module computes
//! and reports, it does not itself gate anything; `publication_gate`
//! (the two hard rules the task doc requires — pre-permitted-year
//! production and unclassified contact-field leakage) is the one part
//! of this module callers should treat as pass/fail, exposed via
//! `ks_ingest publication-gate-check` with a real nonzero exit code on
//! violation, the same "hard rule enforced structurally" discipline this
//! repo uses everywhere else (e.g. `ks_well_document`'s CHECK constraint
//! from KS-15).
//!
//! "Alerts route through existing monitoring" (task doc) is explicitly
//! **not** built here — this repo has no visibility into what "existing
//! monitoring" means outside `kansasBackend`, the same reasoning KS-12/13
//! already established for cross-repo UI work. What this module provides
//! is the real, queryable data a future alerting integration would
//! consume; see `docs/ops/ks-17-monitoring-handoff.md` for exactly what
//! that integration needs from here.

use chrono::{DateTime, Utc};
use sqlx::PgPool;

use crate::normalize::kgs_documents::redact_personal_info;

// ---------------------------------------------------------------------
// Source freshness (per-source last-successful-import / staleness /
// failed-vs-stale distinction)
// ---------------------------------------------------------------------

/// A cadence-to-staleness-threshold mapping matching the task doc's own
/// recommended expectations table. `irregular`/anything unrecognized
/// never reports stale — there is no source-grounded expectation to
/// compare against, so silence is more honest than an invented
/// threshold. Matches on prefix, not just exact string, because
/// `ks_source.expected_cadence` records real per-source variants like
/// `daily_rolling_365`/`daily_rolling_90` (KS-06/KS-07's rolling-window
/// sources) and `continuous` (KCC_OPERATORS) — treating those as "no
/// threshold" (as an exact-match-only version of this function once did)
/// silently made three real sources with a defined cadence never report
/// stale at all, which defeats the point of tracking cadence for them.
fn staleness_threshold_hours(expected_cadence: &str) -> Option<i64> {
    if expected_cadence.starts_with("nightly")
        || expected_cadence.starts_with("daily")
        || expected_cadence == "continuous"
    {
        Some(30) // a bit more than 24h, tolerating a late run
    } else if expected_cadence.starts_with("weekly") {
        Some(24 * 9)
    } else if expected_cadence.starts_with("monthly") {
        Some(24 * 40)
    } else {
        None
    }
}

#[derive(Debug, Clone, sqlx::FromRow)]
pub struct SourceFreshness {
    pub source_code: String,
    pub expected_cadence: String,
    pub last_successful_import: Option<DateTime<Utc>>,
    pub most_recent_run_status: Option<String>,
    pub most_recent_run_at: Option<DateTime<Utc>>,
}

impl SourceFreshness {
    /// Independent of `is_failed` — a source can be stale without its
    /// most recent run having failed (e.g. no run was attempted at all
    /// during a maintenance window), and a source can have a freshly
    /// failed run while its *last successful* import is still well
    /// within its expected cadence. Task doc: "stale and failed are
    /// separate states."
    pub fn is_stale(&self) -> bool {
        let Some(threshold_hours) = staleness_threshold_hours(&self.expected_cadence) else {
            return false;
        };
        match self.last_successful_import {
            None => true,
            Some(last) => (Utc::now() - last).num_hours() > threshold_hours,
        }
    }

    pub fn is_failed(&self) -> bool {
        matches!(
            self.most_recent_run_status.as_deref(),
            Some("failed") | Some("rejected")
        )
    }
}

pub async fn source_freshness_report(pool: &PgPool) -> Result<Vec<SourceFreshness>, sqlx::Error> {
    sqlx::query_as(
        r#"
        SELECT
            s.code AS source_code,
            s.expected_cadence,
            (SELECT max(r.completed_at) FROM ks_ingestion_run r WHERE r.source_id = s.id AND r.status = 'completed') AS last_successful_import,
            latest.status AS most_recent_run_status,
            latest.started_at AS most_recent_run_at
        FROM ks_source s
        LEFT JOIN LATERAL (
            SELECT status, started_at FROM ks_ingestion_run r WHERE r.source_id = s.id ORDER BY r.started_at DESC LIMIT 1
        ) latest ON true
        ORDER BY s.code
        "#,
    )
    .fetch_all(pool)
    .await
}

// ---------------------------------------------------------------------
// Row-count change and schema-fingerprint change (per source, comparing
// the two most recent completed runs)
// ---------------------------------------------------------------------

#[derive(Debug, Clone, sqlx::FromRow)]
pub struct RowCountChange {
    pub source_code: String,
    /// `None` when this source has no completed run at all.
    pub current_run_id: Option<uuid::Uuid>,
    pub current_row_count: Option<i64>,
    pub previous_row_count: Option<i64>,
}

impl RowCountChange {
    /// `None` when there is no previous completed run to compare
    /// against (a source's first-ever run) — never fabricated as 0%.
    pub fn change_ratio(&self) -> Option<f64> {
        let (current, previous) = (self.current_row_count?, self.previous_row_count?);
        if previous == 0 {
            return None;
        }
        Some((current - previous) as f64 / previous as f64)
    }
}

/// `current_run_id` is `None` for a source with no completed run at all
/// — such a source must still appear in this report (it is exactly the
/// "no data at all" anomaly a coverage panel needs to show), so this
/// starts from `ks_source` and `LEFT JOIN`s its run history, rather than
/// starting from `ks_ingestion_run` and silently omitting a source with
/// zero rows there (a real bug this module's own tests caught: a source
/// with no importer yet, like `KCC_CONSERVATION_DOCKETS`, disappeared
/// from the report entirely instead of showing up with `None`s).
pub async fn row_count_change_report(pool: &PgPool) -> Result<Vec<RowCountChange>, sqlx::Error> {
    sqlx::query_as(
        r#"
        WITH ranked AS (
            SELECT s.id AS source_uuid, s.code AS source_code, r.id, r.row_count,
                   row_number() OVER (PARTITION BY s.id ORDER BY r.completed_at DESC) AS rn
            FROM ks_source s
            LEFT JOIN ks_ingestion_run r ON r.source_id = s.id AND r.status = 'completed'
        )
        SELECT cur.source_code, cur.id AS current_run_id, cur.row_count AS current_row_count, prev.row_count AS previous_row_count
        FROM ranked cur
        LEFT JOIN ranked prev ON prev.source_uuid = cur.source_uuid AND prev.rn = cur.rn + 1
        WHERE cur.rn = 1
        ORDER BY cur.source_code
        "#,
    )
    .fetch_all(pool)
    .await
}

#[derive(Debug, Clone, sqlx::FromRow)]
pub struct SchemaFingerprintChange {
    pub source_code: String,
    pub current_fingerprint: Option<String>,
    pub previous_fingerprint: Option<String>,
}

impl SchemaFingerprintChange {
    /// `None` (not just `false`) when either side is unknown — most
    /// importers do not populate `schema_fingerprint` yet, and treating
    /// "unknown" the same as "unchanged" would hide a real gap in
    /// coverage rather than surface it.
    pub fn changed(&self) -> Option<bool> {
        Some(self.current_fingerprint.as_ref()? != self.previous_fingerprint.as_ref()?)
    }
}

pub async fn schema_fingerprint_change_report(
    pool: &PgPool,
) -> Result<Vec<SchemaFingerprintChange>, sqlx::Error> {
    sqlx::query_as(
        r#"
        WITH ranked AS (
            SELECT s.id AS source_uuid, s.code AS source_code, r.schema_fingerprint,
                   row_number() OVER (PARTITION BY s.id ORDER BY r.completed_at DESC) AS rn
            FROM ks_source s
            LEFT JOIN ks_ingestion_run r ON r.source_id = s.id AND r.status = 'completed'
        )
        SELECT cur.source_code, cur.schema_fingerprint AS current_fingerprint, prev.schema_fingerprint AS previous_fingerprint
        FROM ranked cur
        LEFT JOIN ranked prev ON prev.source_uuid = cur.source_uuid AND prev.rn = cur.rn + 1
        WHERE cur.rn = 1
        ORDER BY cur.source_code
        "#,
    )
    .fetch_all(pool)
    .await
}

/// KS-17: "source HTTP/content-type changes." Uses the same two-most-
/// recent-runs comparison as the fingerprint check above, over
/// `response_content_type` (migration `0029`) — populated only by
/// importers updated to call `ingestion_run::record_response_content_type`
/// so far (`parse::kgs_documents`); every other source reports `None` on
/// both sides, which `changed()` correctly treats as "unknown," not "no
/// change."
#[derive(Debug, Clone, sqlx::FromRow)]
pub struct ContentTypeChange {
    pub source_code: String,
    pub current_content_type: Option<String>,
    pub previous_content_type: Option<String>,
}

impl ContentTypeChange {
    pub fn changed(&self) -> Option<bool> {
        Some(self.current_content_type.as_ref()? != self.previous_content_type.as_ref()?)
    }
}

pub async fn content_type_change_report(
    pool: &PgPool,
) -> Result<Vec<ContentTypeChange>, sqlx::Error> {
    sqlx::query_as(
        r#"
        WITH ranked AS (
            SELECT s.id AS source_uuid, s.code AS source_code, r.response_content_type,
                   row_number() OVER (PARTITION BY s.id ORDER BY r.completed_at DESC) AS rn
            FROM ks_source s
            LEFT JOIN ks_ingestion_run r ON r.source_id = s.id AND r.status = 'completed'
        )
        SELECT cur.source_code, cur.response_content_type AS current_content_type, prev.response_content_type AS previous_content_type
        FROM ranked cur
        LEFT JOIN ranked prev ON prev.source_uuid = cur.source_uuid AND prev.rn = cur.rn + 1
        WHERE cur.rn = 1
        ORDER BY cur.source_code
        "#,
    )
    .fetch_all(pool)
    .await
}

// ---------------------------------------------------------------------
// Zero-result / rejected-run anomalies
// ---------------------------------------------------------------------

#[derive(Debug, Clone, sqlx::FromRow)]
pub struct RejectedRunCount {
    pub source_code: String,
    pub rejected_run_count: i64,
}

/// Counts `rejected` runs (this repo's existing zero-result/majority-
/// unparseable safeguards all reject via this status) per source within
/// the last `days` days. Not filtered to zero-result specifically —
/// `ks_ingestion_run.failure_message` is free text, not a coded reason —
/// so this is "rejected for any safeguard reason," documented as such
/// rather than claiming more precision than the data supports.
pub async fn rejected_run_anomalies_report(
    pool: &PgPool,
    days: i32,
) -> Result<Vec<RejectedRunCount>, sqlx::Error> {
    sqlx::query_as(
        r#"
        SELECT s.code AS source_code, count(*) AS rejected_run_count
        FROM ks_ingestion_run r
        JOIN ks_source s ON s.id = r.source_id
        WHERE r.status = 'rejected' AND r.started_at >= now() - make_interval(days => $1)
        GROUP BY s.code
        ORDER BY rejected_run_count DESC
        "#,
    )
    .bind(days)
    .fetch_all(pool)
    .await
}

// ---------------------------------------------------------------------
// Well data quality (duplicate/invalid API, missing geometry, out-of-
// state exclusions, unknown status combinations)
// ---------------------------------------------------------------------

#[derive(Debug, Clone, Default, sqlx::FromRow)]
pub struct WellDataQuality {
    /// Duplicate API values *within a single run's raw snapshot* — never
    /// possible in the canonical `ks_well` table itself, which has a
    /// real unique index on `api14`; this counts the source's own raw
    /// report before materialization's upsert absorbs it, since a source
    /// reporting the same API twice in one snapshot is a real anomaly
    /// worth surfacing even though it can never corrupt canonical data.
    pub duplicate_api_count_latest_wells_run: i64,
    pub invalid_api_count: i64,
    pub missing_geometry_count: i64,
    pub out_of_state_excluded_count_latest_run: i64,
    pub unknown_status_combination_count: i64,
}

pub async fn well_data_quality_report(pool: &PgPool) -> Result<WellDataQuality, sqlx::Error> {
    let latest_wells_run: Option<uuid::Uuid> = sqlx::query_scalar(
        r#"
        SELECT r.id FROM ks_ingestion_run r
        JOIN ks_source s ON s.id = r.source_id
        WHERE s.code IN ('KGS_WELLS_ARCGIS', 'KGS_WELLS_ZIP') AND r.status = 'completed'
        ORDER BY r.completed_at DESC LIMIT 1
        "#,
    )
    .fetch_optional(pool)
    .await?;

    let duplicate_api_count_latest_wells_run: i64 = match latest_wells_run {
        Some(run_id) => {
            sqlx::query_scalar(
                r#"
                SELECT count(*) FROM (
                    SELECT raw_data->>'API_NUMBER' AS api
                    FROM ks_well_raw_snapshot
                    WHERE ingestion_run_id = $1 AND kansas_membership_status = 'included' AND raw_data->>'API_NUMBER' IS NOT NULL
                    GROUP BY raw_data->>'API_NUMBER'
                    HAVING count(*) > 1
                ) dupes
                "#,
            )
            .bind(run_id)
            .fetch_one(pool)
            .await?
        }
        None => 0,
    };

    let invalid_api_count: i64 = sqlx::query_scalar("SELECT count(*) FROM ks_well WHERE api_validation_status NOT IN ('valid', 'valid_short_form')").fetch_one(pool).await?;
    let missing_geometry_count: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM ks_well WHERE display_geometry_4326 IS NULL AND NOT is_tombstoned",
    )
    .fetch_one(pool)
    .await?;
    let out_of_state_excluded_count_latest_run: i64 = match latest_wells_run {
        Some(run_id) => sqlx::query_scalar("SELECT count(*) FROM ks_well_raw_snapshot WHERE ingestion_run_id = $1 AND kansas_membership_status = 'excluded'").bind(run_id).fetch_one(pool).await?,
        None => 0,
    };
    let unknown_status_combination_count: i64 =
        sqlx::query_scalar("SELECT count(DISTINCT (well_class_raw, status_raw, status2_raw)) FROM ks_well WHERE lifecycle_status = 'unknown'").fetch_one(pool).await?;

    Ok(WellDataQuality {
        duplicate_api_count_latest_wells_run,
        invalid_api_count,
        missing_geometry_count,
        out_of_state_excluded_count_latest_run,
        unknown_status_combination_count,
    })
}

// ---------------------------------------------------------------------
// Link confidence (operator/intent/transfer/lease match-method
// distributions) — "operator-link confidence," "lease-link confidence"
// ---------------------------------------------------------------------

#[derive(Debug, Clone, sqlx::FromRow)]
pub struct LinkConfidenceBucket {
    pub category: String,
    pub method: String,
    pub count: i64,
}

pub async fn link_confidence_report(
    pool: &PgPool,
) -> Result<Vec<LinkConfidenceBucket>, sqlx::Error> {
    let mut out = Vec::new();
    let intents: Vec<(String, i64)> = sqlx::query_as(
        "SELECT match_method, count(*) FROM ks_drilling_intent GROUP BY match_method",
    )
    .fetch_all(pool)
    .await?;
    out.extend(
        intents
            .into_iter()
            .map(|(method, count)| LinkConfidenceBucket {
                category: "intent_well_link".to_owned(),
                method,
                count,
            }),
    );

    let transfers: Vec<(String, i64)> = sqlx::query_as(
        "SELECT match_method, count(*) FROM ks_operator_transfer_well GROUP BY match_method",
    )
    .fetch_all(pool)
    .await?;
    out.extend(
        transfers
            .into_iter()
            .map(|(method, count)| LinkConfidenceBucket {
                category: "transfer_well_link".to_owned(),
                method,
                count,
            }),
    );

    let leases: Vec<(String, i64)> =
        sqlx::query_as("SELECT link_method, count(*) FROM ks_well_lease_link GROUP BY link_method")
            .fetch_all(pool)
            .await?;
    out.extend(
        leases
            .into_iter()
            .map(|(method, count)| LinkConfidenceBucket {
                category: "lease_well_link".to_owned(),
                method,
                count,
            }),
    );

    let unresolved_leases: i64 = sqlx::query_scalar("SELECT count(*) FROM ks_lease l WHERE NOT EXISTS (SELECT 1 FROM ks_well_lease_link k WHERE k.lease_id = l.id)").fetch_one(pool).await?;
    out.push(LinkConfidenceBucket {
        category: "lease_well_link".to_owned(),
        method: "unresolved".to_owned(),
        count: unresolved_leases,
    });

    Ok(out)
}

// ---------------------------------------------------------------------
// Production reconciliation summary — reuses KS-10's own reconciliation
// query rather than re-deriving it.
// ---------------------------------------------------------------------

pub async fn production_reconciliation_out_of_tolerance_count(
    pool: &PgPool,
    tolerance_ratio: f64,
) -> Result<i64, sqlx::Error> {
    let report = crate::materialize::kgs_production::reconciliation_report(
        pool,
        crate::materialize::kgs_production::ReconciliationGrouping::State,
        tolerance_ratio,
        10_000,
    )
    .await?;
    Ok(report
        .iter()
        .filter(|row| row.within_tolerance == Some(false))
        .count() as i64)
}

// ---------------------------------------------------------------------
// Publication gates (hard rules — task doc: "any production row before
// the permitted year should fail publication"; "any newly detected
// contact/address field should fail indexing until explicitly
// classified"). These are the two checks `ks_ingest publication-gate-check`
// treats as pass/fail, not just informational.
// ---------------------------------------------------------------------

#[derive(Debug, Clone, sqlx::FromRow)]
pub struct RestrictedYearViolation {
    pub id: uuid::Uuid,
    pub lease_kid: String,
    pub year: i32,
}

/// Should always return empty — `constants::is_production_year_ingestible`
/// already blocks a pre-1990 row at staging time (KS-10). This is the
/// safety-net re-check against the *canonical* table itself, independent
/// of whether the ingest-time gate was somehow bypassed (a legal-review
/// override misused, a manual data fix, a future code change) —
/// "restricted-year violations" as a real, standing assertion rather
/// than a one-time check at import.
pub async fn restricted_year_violations(
    pool: &PgPool,
) -> Result<Vec<RestrictedYearViolation>, sqlx::Error> {
    sqlx::query_as(
        "SELECT id, lease_kid, year FROM ks_lease_production_month WHERE year < $1 ORDER BY year",
    )
    .bind(crate::constants::MIN_PRODUCTION_YEAR)
    .fetch_all(pool)
    .await
}

#[derive(Debug, Clone)]
pub struct PersonalFieldLeakageHit {
    pub table_name: &'static str,
    pub column_name: &'static str,
    pub row_id: uuid::Uuid,
    pub matched_excerpt: String,
}

/// Scans the free-text columns this repo actually indexes/exports
/// (never `ks_well.comments`, which is deliberately excluded from every
/// export already — see KS-12/13/15 — and so is not a "detected" leak
/// even if it contains contact data, it is a *known* excluded field) for
/// phone-number-shaped text, reusing KS-15's own redaction detector so
/// there is exactly one definition of "looks like personal contact
/// info" in this codebase. A hit here means a field this repo believes
/// is safe to export actually contains something that looks like contact
/// data — the "newly detected" case the task doc's hard rule is about.
pub async fn personal_field_leakage_report(
    pool: &PgPool,
) -> Result<Vec<PersonalFieldLeakageHit>, sqlx::Error> {
    let mut hits = Vec::new();

    let checks: &[(&str, &str, &str)] = &[
        ("ks_operator", "reported_company_name", "SELECT id, reported_company_name FROM ks_operator WHERE reported_company_name IS NOT NULL"),
        ("ks_operator", "source_city", "SELECT id, source_city FROM ks_operator WHERE source_city IS NOT NULL"),
        ("ks_well", "well_name", "SELECT internal_well_id, well_name FROM ks_well WHERE well_name IS NOT NULL"),
        ("ks_well", "lease_name", "SELECT internal_well_id, lease_name FROM ks_well WHERE lease_name IS NOT NULL"),
        ("ks_lease", "reported_lease_name", "SELECT id, reported_lease_name FROM ks_lease WHERE reported_lease_name IS NOT NULL"),
        ("ks_field", "official_field_name", "SELECT id, official_field_name FROM ks_field WHERE official_field_name IS NOT NULL"),
    ];

    for (table_name, column_name, sql) in checks {
        let rows: Vec<(uuid::Uuid, String)> = sqlx::query_as(sql).fetch_all(pool).await?;
        for (row_id, value) in rows {
            let redacted = redact_personal_info(&value);
            if redacted != value {
                hits.push(PersonalFieldLeakageHit {
                    table_name,
                    column_name,
                    row_id,
                    matched_excerpt: value,
                });
            }
        }
    }

    Ok(hits)
}

#[derive(Debug, Clone)]
pub struct PublicationGateResult {
    pub restricted_year_violations: Vec<RestrictedYearViolation>,
    pub personal_field_leakage: Vec<PersonalFieldLeakageHit>,
}

impl PublicationGateResult {
    pub fn passes(&self) -> bool {
        self.restricted_year_violations.is_empty() && self.personal_field_leakage.is_empty()
    }
}

pub async fn publication_gate(pool: &PgPool) -> Result<PublicationGateResult, sqlx::Error> {
    Ok(PublicationGateResult {
        restricted_year_violations: restricted_year_violations(pool).await?,
        personal_field_leakage: personal_field_leakage_report(pool).await?,
    })
}

// Re-exported so `ks_ingest health-report` can pull the documents-queue
// summary from this module alongside everything else, without the CLI
// needing to know it actually lives in `materialize::kgs_documents`.
pub use crate::materialize::kgs_documents::unresolved_documents_report;

#[derive(Debug, Clone, sqlx::FromRow)]
pub struct DocumentTypeCount {
    pub document_type: String,
    pub count: i64,
}

pub async fn document_type_distribution(
    pool: &PgPool,
) -> Result<Vec<DocumentTypeCount>, sqlx::Error> {
    sqlx::query_as("SELECT document_type, count(*) FROM ks_well_document GROUP BY document_type ORDER BY count DESC").fetch_all(pool).await
}

#[cfg(test)]
mod tests {
    use super::staleness_threshold_hours;

    // Every `expected_cadence` value actually seeded in `ks_source`
    // (migration 0002) must resolve to a real threshold unless it is
    // genuinely `irregular` — this is exactly the gap a real bug slipped
    // through: `daily_rolling_365`/`daily_rolling_90`/`continuous` don't
    // equal the literal string `"daily"`, so an exact-match version of
    // this function silently gave KCC_INTENTS, KCC_TRANSFERS, and
    // KCC_OPERATORS no staleness threshold at all.
    #[test]
    fn every_seeded_cadence_value_resolves_as_expected() {
        assert_eq!(staleness_threshold_hours("nightly"), Some(30));
        assert_eq!(staleness_threshold_hours("monthly"), Some(24 * 40));
        assert_eq!(staleness_threshold_hours("irregular"), None);
        assert_eq!(staleness_threshold_hours("continuous"), Some(30));
        assert_eq!(staleness_threshold_hours("daily_rolling_365"), Some(30));
        assert_eq!(staleness_threshold_hours("daily_rolling_90"), Some(30));
        assert_eq!(staleness_threshold_hours("daily"), Some(30));
    }
}
