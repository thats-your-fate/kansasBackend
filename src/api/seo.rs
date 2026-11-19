use axum::{
    extract::{Query, State},
    Json,
};
use serde::{Deserialize, Serialize};
use sqlx::Row;

use super::envelope::{Envelope, Meta};
use super::error::ApiError;
use super::state::AppState;

const DIRECTORY_PAGE_SIZE: i64 = 50;

/// Primary wells per well-sitemap file; well under the 50,000-URL protocol cap.
pub const WELL_SITEMAP_PAGE_SIZE: i64 = 25_000;

/// Years of dated lifecycle evidence (permit, spud, completion, plug) that
/// still make an otherwise quiet well a primary sitemap record.
const PRIMARY_EVENT_YEARS: i32 = 5;

/// Months of lease production that count as current for a well that is not
/// plugged, dry and abandoned, or an expired intent.
const LIVE_PRODUCTION_MONTHS: i32 = 24;

/// A `planned` well only counts as active while its permit is this recent;
/// most planned rows in KGS are decades-old intents that were never drilled.
const FRESH_PERMIT_YEARS: i32 = 2;

#[derive(Debug, Serialize)]
pub struct SitemapDirectories {
    pub directories: Vec<SitemapDirectory>,
}

#[derive(Debug, Serialize)]
pub struct SitemapDirectory {
    pub kind: &'static str,
    pub path: &'static str,
    pub total_items: i64,
    pub total_pages: i64,
    pub items: Vec<SitemapEntry>,
}

#[derive(Debug, Serialize)]
pub struct SitemapEntry {
    pub key: String,
    pub display_name: String,
    pub well_count: i64,
}

#[derive(Copy, Clone)]
enum EntityKind {
    County,
    Operator,
    Field,
}

pub async fn sitemap_directories(
    State(state): State<AppState>,
) -> Result<Json<Envelope<SitemapDirectories>>, ApiError> {
    let (counties, operators, fields) = tokio::try_join!(
        entity_directory(&state.pool, EntityKind::County),
        entity_directory(&state.pool, EntityKind::Operator),
        entity_directory(&state.pool, EntityKind::Field),
    )?;

    Ok(Json(Envelope::new(
        SitemapDirectories {
            directories: vec![counties, operators, fields],
        },
        Meta {
            state: crate::constants::STATE_KEY,
            source_ids: vec!["ks_well".to_string()],
            ..Default::default()
        },
    )))
}

async fn entity_directory(
    pool: &sqlx::PgPool,
    kind: EntityKind,
) -> Result<SitemapDirectory, ApiError> {
    let expr = kind.sql_expr();
    let sql = format!(
        r#"
        SELECT {expr} AS display_name, count(*)::bigint AS well_count
        FROM ks_well
        WHERE is_tombstoned = false
          AND {valid_value}
        GROUP BY 1
        ORDER BY lower({expr}) ASC, {expr} ASC
        "#,
        valid_value = valid_value_sql(kind, expr)
    );
    let rows = sqlx::query(&sql).fetch_all(pool).await?;
    let total_items = rows.len() as i64;
    let total_pages = total_pages(total_items);
    let items = rows
        .into_iter()
        .map(|row| {
            let display_name = row.get::<String, _>("display_name");
            SitemapEntry {
                key: slugify(&display_name),
                display_name,
                well_count: row.get("well_count"),
            }
        })
        .collect();

    Ok(SitemapDirectory {
        kind: kind.path_segment(),
        path: kind.public_path(),
        total_items,
        total_pages,
        items,
    })
}

#[derive(Debug, Deserialize)]
pub struct WellSitemapQuery {
    pub page: Option<i64>,
}

#[derive(Debug, Serialize)]
pub struct WellSitemapPage {
    pub tier: &'static str,
    pub page: i64,
    pub page_size: i64,
    pub total_pages: i64,
    pub total_items: i64,
    pub entries: Vec<WellSitemapEntry>,
}

/// How long one computed primary-well list is served before it is rebuilt.
/// The full classification takes several seconds and the endpoint is public
/// through the web proxy, so it is computed at most once per window.
const WELL_SITEMAP_CACHE_TTL: std::time::Duration = std::time::Duration::from_secs(6 * 60 * 60);

type PrimaryWells = std::sync::Arc<Vec<WellSitemapEntry>>;

static PRIMARY_WELL_CACHE: tokio::sync::Mutex<Option<(std::time::Instant, PrimaryWells)>> =
    tokio::sync::Mutex::const_new(None);

/// All primary wells in sitemap order. Concurrent callers wait on one
/// rebuild instead of each running the classification.
async fn primary_well_entries(pool: &sqlx::PgPool) -> Result<PrimaryWells, ApiError> {
    let mut cached = PRIMARY_WELL_CACHE.lock().await;
    if let Some((at, entries)) = cached.as_ref() {
        if at.elapsed() < WELL_SITEMAP_CACHE_TTL {
            return Ok(entries.clone());
        }
    }
    let sql = format!(
        "{scored} SELECT internal_well_id, api14, api_raw, kgs_kid, well_name, lease_name, lastmod \
         FROM scored WHERE tier = 'primary' ORDER BY api14 NULLS LAST, internal_well_id",
        scored = well_seo_scored_sql(false)
    );
    let rows = sqlx::query(&sql).fetch_all(pool).await?;
    let entries: PrimaryWells = std::sync::Arc::new(
        rows.into_iter()
            .map(|row| WellSitemapEntry {
                id: row.get("internal_well_id"),
                api14: row.get("api14"),
                api_raw: row.get("api_raw"),
                kgs_kid: row.get("kgs_kid"),
                well_name: row.get("well_name"),
                lease_name: row.get("lease_name"),
                lastmod: row.get("lastmod"),
            })
            .collect(),
    );
    *cached = Some((std::time::Instant::now(), entries.clone()));
    Ok(entries)
}

/// Identity fields the web needs to build the canonical well path itself, so
/// sitemap URLs always match the page's own canonical link.
#[derive(Clone, Debug, Serialize)]
pub struct WellSitemapEntry {
    pub id: uuid::Uuid,
    pub api14: Option<String>,
    pub api_raw: Option<String>,
    pub kgs_kid: Option<String>,
    pub well_name: Option<String>,
    pub lease_name: Option<String>,
    pub lastmod: Option<chrono::NaiveDate>,
}

/// Primary-tier wells only; secondary and thin records stay reachable through
/// entity pages but are not submitted to crawlers.
pub async fn sitemap_wells(
    State(state): State<AppState>,
    Query(query): Query<WellSitemapQuery>,
) -> Result<Json<Envelope<WellSitemapPage>>, ApiError> {
    let page = query.page.unwrap_or(1);
    if !(1..=1_000).contains(&page) {
        return Err(ApiError::bad_request("invalid sitemap page"));
    }
    let all = primary_well_entries(&state.pool).await?;
    let total_items = all.len() as i64;
    let offset = ((page - 1) * WELL_SITEMAP_PAGE_SIZE) as usize;
    let entries = all
        .iter()
        .skip(offset)
        .take(WELL_SITEMAP_PAGE_SIZE as usize)
        .cloned()
        .collect();
    Ok(Json(Envelope::new(
        WellSitemapPage {
            tier: "primary",
            page,
            page_size: WELL_SITEMAP_PAGE_SIZE,
            total_pages: well_sitemap_pages(total_items),
            total_items,
            entries,
        },
        Meta {
            state: crate::constants::STATE_KEY,
            source_ids: vec![
                "ks_well".to_string(),
                "ks_well_document".to_string(),
                "ks_lease_production_month".to_string(),
            ],
            ..Default::default()
        },
    )))
}

fn well_sitemap_pages(total_items: i64) -> i64 {
    if total_items <= 0 {
        return 0;
    }
    (total_items + WELL_SITEMAP_PAGE_SIZE - 1) / WELL_SITEMAP_PAGE_SIZE
}

/// One definition of a Kansas well's search tier and evidence date, shared by
/// the well sitemap and the well page's robots policy.
///
/// * primary: public documents, lease production in the last
///   LIVE_PRODUCTION_MONTHS for a well that is not plugged/dry/expired, a
///   `planned` permit within FRESH_PERMIT_YEARS, or a permit/spud/completion/
///   plug date within PRIMARY_EVENT_YEARS -> submitted in the sitemap.
/// * secondary: older lifecycle dates or past lease production only ->
///   indexable, not submitted.
/// * thin: none of the above -> the web serves `noindex, follow`.
///
/// Production is lease-level in KGS, so it is never on its own a reason to
/// promote a retired well. `lastmod` is the newest real evidence date
/// (document filing or first observation, latest producing lease month,
/// lifecycle dates), capped at today; import bookkeeping columns are not used.
///
/// With `one_well`, every source is narrowed to the well bound as `$1`.
pub(super) fn well_seo_scored_sql(one_well: bool) -> String {
    let (doc_filter, link_filter, well_filter) = if one_well {
        (
            "AND d.well_id = $1",
            "WHERE l.well_id = $1",
            "AND w.internal_well_id = $1",
        )
    } else {
        ("", "", "")
    };
    format!(
        r#"WITH docs AS (
            SELECT d.well_id, count(*) AS n, max(d.filing_date) AS latest_filing,
                max(d.first_observed_at) AS latest_observed
            FROM ks_well_document d
            WHERE d.well_id IS NOT NULL AND d.public_access_state = 'public'
              AND d.current_version_id IS NOT NULL {doc_filter}
            GROUP BY d.well_id
        ), links AS (
            SELECT l.well_id, l.lease_id FROM ks_well_lease_link l {link_filter}
        ), lease_latest AS (
            SELECT p.lease_id, max(make_date(p.year, p.month, 1)) AS latest_month
            FROM ks_lease_production_month p
            WHERE p.lease_id IS NOT NULL AND p.record_kind = 'monthly' AND p.production_volume > 0
              AND p.lease_id IN (SELECT lease_id FROM links)
            GROUP BY p.lease_id
        ), prod AS (
            SELECT links.well_id, max(ll.latest_month) AS latest_month
            FROM links JOIN lease_latest ll ON ll.lease_id = links.lease_id
            GROUP BY links.well_id
        ), signals AS (
            SELECT w.internal_well_id, w.api14, w.api_raw, w.kgs_kid, w.well_name, w.lease_name,
                COALESCE(docs.n, 0) AS document_count,
                prod.latest_month AS latest_production_month,
                COALESCE(prod.latest_month >= (CURRENT_DATE - make_interval(months => {live_months}))
                    AND w.lifecycle_status NOT IN ('plugged', 'dry_and_abandoned', 'intent_expired'), false)
                    AS has_live_production,
                COALESCE(w.lifecycle_status = 'planned'
                    AND w.permit_date >= (CURRENT_DATE - make_interval(years => {permit_years})), false)
                    AS has_fresh_permit,
                GREATEST(w.permit_date, w.spud_date, w.completion_date, w.plug_date) AS latest_event,
                GREATEST(docs.latest_filing, (docs.latest_observed AT TIME ZONE 'UTC')::date,
                    prod.latest_month, w.permit_date, w.spud_date, w.completion_date, w.plug_date)
                    AS evidence_date
            FROM ks_well w
            LEFT JOIN docs ON docs.well_id = w.internal_well_id
            LEFT JOIN prod ON prod.well_id = w.internal_well_id
            WHERE w.is_tombstoned = false {well_filter}
        ), scored AS (
            SELECT *,
                CASE
                    WHEN document_count > 0 OR has_live_production OR has_fresh_permit
                        OR latest_event >= (CURRENT_DATE - make_interval(years => {event_years})) THEN 'primary'
                    WHEN latest_event IS NOT NULL OR latest_production_month IS NOT NULL THEN 'secondary'
                    ELSE 'thin'
                END AS tier,
                CASE WHEN evidence_date IS NULL THEN NULL ELSE LEAST(evidence_date, CURRENT_DATE) END AS lastmod
            FROM signals
        )"#,
        live_months = LIVE_PRODUCTION_MONTHS,
        permit_years = FRESH_PERMIT_YEARS,
        event_years = PRIMARY_EVENT_YEARS,
    )
}

#[derive(Debug, Serialize)]
pub struct WellSeo {
    pub tier: String,
    pub lastmod: Option<chrono::NaiveDate>,
}

/// Search tier and evidence date for one well, by internal id.
pub(super) async fn well_seo(
    pool: &sqlx::PgPool,
    internal_well_id: uuid::Uuid,
) -> Result<Option<WellSeo>, ApiError> {
    let sql = format!(
        "{scored} SELECT tier, lastmod FROM scored",
        scored = well_seo_scored_sql(true)
    );
    let row = sqlx::query(&sql)
        .bind(internal_well_id)
        .fetch_optional(pool)
        .await?;
    Ok(row.map(|row| WellSeo {
        tier: row.get("tier"),
        lastmod: row.get("lastmod"),
    }))
}

impl EntityKind {
    fn path_segment(self) -> &'static str {
        match self {
            EntityKind::County => "counties",
            EntityKind::Operator => "operators",
            EntityKind::Field => "fields",
        }
    }

    fn public_path(self) -> &'static str {
        match self {
            EntityKind::County => "/counties",
            EntityKind::Operator => "/operators",
            EntityKind::Field => "/fields",
        }
    }

    fn sql_expr(self) -> &'static str {
        match self {
            EntityKind::County => "county",
            EntityKind::Operator => "COALESCE(NULLIF(btrim(operator_name_kgs_reported_current), ''), NULLIF(btrim(operator_name_kgs_original), ''))",
            EntityKind::Field => "field_name",
        }
    }
}

fn valid_value_sql(kind: EntityKind, expr: &str) -> String {
    let base = format!("{expr} IS NOT NULL AND btrim({expr}) <> ''");
    match kind {
        EntityKind::Operator => format!(
            "{base} AND lower(btrim({expr})) NOT IN ('unavailable', 'unknown', 'n/a', 'na')"
        ),
        EntityKind::Field => {
            format!("{base} AND lower(btrim({expr})) NOT IN ('unknown', 'unnamed')")
        }
        EntityKind::County => base,
    }
}

fn total_pages(total_items: i64) -> i64 {
    if total_items <= 0 {
        return 0;
    }
    (total_items + DIRECTORY_PAGE_SIZE - 1) / DIRECTORY_PAGE_SIZE
}

fn slugify(value: &str) -> String {
    let mut slug = String::new();
    let mut previous_dash = false;
    for ch in value.trim().to_lowercase().replace('&', " and ").chars() {
        if ch.is_ascii_alphanumeric() {
            slug.push(ch);
            previous_dash = false;
        } else if !previous_dash && !slug.is_empty() {
            slug.push('-');
            previous_dash = true;
        }
    }
    while slug.ends_with('-') {
        slug.pop();
    }
    slug
}

#[cfg(test)]
mod tests {
    use super::{slugify, total_pages, well_seo_scored_sql, well_sitemap_pages};

    #[test]
    fn sitemap_slug_matches_public_directory_paths() {
        assert_eq!(slugify("Barton County"), "barton-county");
        assert_eq!(slugify("A&W"), "a-and-w");
        assert_eq!(slugify("14Start Energy LLC"), "14start-energy-llc");
    }

    #[test]
    fn sitemap_pagination_uses_public_directory_page_size() {
        assert_eq!(total_pages(0), 0);
        assert_eq!(total_pages(1), 1);
        assert_eq!(total_pages(50), 1);
        assert_eq!(total_pages(51), 2);
    }

    #[test]
    fn well_sitemap_files_stay_under_protocol_cap() {
        assert_eq!(well_sitemap_pages(0), 0);
        assert_eq!(well_sitemap_pages(25_000), 1);
        assert_eq!(well_sitemap_pages(100_554), 5);
    }

    #[test]
    fn single_well_tier_sql_narrows_every_source() {
        let sql = well_seo_scored_sql(true);
        assert!(sql.contains("d.well_id = $1"));
        assert!(sql.contains("l.well_id = $1"));
        assert!(sql.contains("w.internal_well_id = $1"));
        assert!(!well_seo_scored_sql(false).contains("$1"));
    }
}
