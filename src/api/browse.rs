use axum::{
    extract::{Path, State},
    Json,
};
use chrono::{DateTime, Utc};
use serde::Serialize;
use sqlx::Row;
use uuid::Uuid;

use super::envelope::{Envelope, Meta};
use super::error::ApiError;
use super::state::AppState;

const SUMMARY_LIMIT: i64 = 8;
const ACTIVITY_LIMIT: i64 = 8;

#[derive(Debug, Serialize)]
pub struct BrowseStartPage {
    pub recent_activity: Vec<BrowseActivityEvent>,
    pub top_counties: Vec<BrowseSummaryEntry>,
    pub top_operators: Vec<BrowseSummaryEntry>,
    pub top_fields: Vec<BrowseSummaryEntry>,
}

#[derive(Debug, Serialize)]
pub struct BrowseSummaryEntry {
    pub key: String,
    pub display_name: String,
    pub well_count: i64,
}

#[derive(Debug, Serialize)]
pub struct BrowseEntityDetail {
    pub kind: &'static str,
    pub key: String,
    pub display_name: String,
    pub well_count: i64,
    pub producing_count: i64,
    pub permit_count: i64,
    pub located_well_count: i64,
    pub data_as_of: Option<DateTime<Utc>>,
    pub top_counties: Vec<BrowseSummaryEntry>,
    pub top_operators: Vec<BrowseSummaryEntry>,
    pub top_fields: Vec<BrowseSummaryEntry>,
    pub status_counts: Vec<BrowseSummaryEntry>,
    pub wells: Vec<BrowseWellCard>,
    pub permitted_wells: Vec<BrowseWellCard>,
    pub producing_wells: Vec<BrowseWellCard>,
    pub completed_wells: Vec<BrowseWellCard>,
}

#[derive(Debug, Serialize)]
pub struct BrowseWellDetail {
    pub well: BrowseWellCard,
    pub related_wells: Vec<BrowseWellCard>,
}

#[derive(Clone, Debug, Serialize)]
pub struct BrowseWellCard {
    pub id: Uuid,
    pub api14: Option<String>,
    pub api_raw: Option<String>,
    pub kgs_kid: Option<String>,
    pub kcc_permit: Option<String>,
    pub well_name: Option<String>,
    pub county: Option<String>,
    pub operator: Option<String>,
    pub field_name: Option<String>,
    pub lease_name: Option<String>,
    pub lifecycle_status: Option<String>,
    pub well_class_raw: Option<String>,
    pub status_raw: Option<String>,
    pub status2_raw: Option<String>,
    pub township: Option<i16>,
    pub township_direction: Option<String>,
    pub range: Option<i16>,
    pub range_direction: Option<String>,
    pub section: Option<i16>,
    pub subdivision_1_largest: Option<String>,
    pub subdivision_2: Option<String>,
    pub subdivision_3: Option<String>,
    pub subdivision_4_smallest: Option<String>,
    pub spot: Option<String>,
    pub feet_north_from_reference: Option<i32>,
    pub feet_east_from_reference: Option<i32>,
    pub reference_corner: Option<String>,
    pub permit_date: Option<chrono::NaiveDate>,
    pub spud_date: Option<chrono::NaiveDate>,
    pub completion_date: Option<chrono::NaiveDate>,
    pub plug_date: Option<chrono::NaiveDate>,
    pub total_depth: Option<i32>,
    pub producing_formation: Option<String>,
    pub formation_at_total_depth: Option<String>,
    pub ip_oil: Option<f64>,
    pub ip_gas: Option<f64>,
    pub ip_water: Option<f64>,
    pub comments: Option<String>,
    pub source_latitude: Option<f64>,
    pub source_longitude: Option<f64>,
    pub horizontal_or_directional: bool,
    pub trajectory_available: bool,
    pub source_format: Option<String>,
    pub source_object_id: Option<i64>,
    pub first_seen_at: Option<DateTime<Utc>>,
    pub last_seen_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Serialize)]
pub struct BrowseActivityEvent {
    pub id: Uuid,
    pub event_type: String,
    pub title: String,
    pub event_date: Option<DateTime<Utc>>,
    pub observed_at: DateTime<Utc>,
    pub api14: Option<String>,
    pub api_raw: Option<String>,
    pub kgs_kid: Option<String>,
    pub well_name: Option<String>,
    pub county: Option<String>,
    pub operator: Option<String>,
    pub field_name: Option<String>,
    pub lease_name: Option<String>,
}

pub async fn start(
    State(state): State<AppState>,
) -> Result<Json<Envelope<BrowseStartPage>>, ApiError> {
    let pool = &state.pool;
    let mut recent_activity = recent_activity(pool).await?;
    if recent_activity.is_empty() {
        recent_activity = featured_well_activity(pool).await?;
    }
    let top_counties = top_counties(pool).await?;
    let top_operators = top_operators(pool).await?;
    let top_fields = top_fields(pool).await?;

    let meta = Meta {
        state: crate::constants::STATE_KEY,
        source_ids: vec![
            "ks_well".to_string(),
            "ks_event".to_string(),
            "ks_operator".to_string(),
            "ks_field".to_string(),
        ],
        ..Default::default()
    };

    Ok(Json(Envelope::new(
        BrowseStartPage {
            recent_activity,
            top_counties,
            top_operators,
            top_fields,
        },
        meta,
    )))
}

pub async fn county_detail(
    State(state): State<AppState>,
    Path(slug): Path<String>,
) -> Result<Json<Envelope<BrowseEntityDetail>>, ApiError> {
    entity_detail_response(&state.pool, EntityKind::County, &slug).await
}

pub async fn operator_detail(
    State(state): State<AppState>,
    Path(slug): Path<String>,
) -> Result<Json<Envelope<BrowseEntityDetail>>, ApiError> {
    entity_detail_response(&state.pool, EntityKind::Operator, &slug).await
}

pub async fn field_detail(
    State(state): State<AppState>,
    Path(slug): Path<String>,
) -> Result<Json<Envelope<BrowseEntityDetail>>, ApiError> {
    entity_detail_response(&state.pool, EntityKind::Field, &slug).await
}

pub async fn well_detail(
    State(state): State<AppState>,
    Path(api): Path<String>,
) -> Result<Json<Envelope<BrowseWellDetail>>, ApiError> {
    let well = load_well_detail(&state.pool, &api).await?;
    let related_wells = if let Some(county) = well.county.as_deref() {
        entity_wells(
            &state.pool,
            EntityKind::County,
            &slugify(county),
            Some(well.id),
            6,
        )
        .await?
    } else {
        Vec::new()
    };
    Ok(Json(Envelope::new(
        BrowseWellDetail {
            well,
            related_wells,
        },
        detail_meta(),
    )))
}

async fn entity_detail_response(
    pool: &sqlx::PgPool,
    kind: EntityKind,
    slug: &str,
) -> Result<Json<Envelope<BrowseEntityDetail>>, ApiError> {
    let detail = load_entity_detail(pool, kind, slug).await?;
    Ok(Json(Envelope::new(detail, detail_meta())))
}

async fn top_counties(pool: &sqlx::PgPool) -> Result<Vec<BrowseSummaryEntry>, sqlx::Error> {
    let rows = sqlx::query(
        r#"
        SELECT county AS key, county AS display_name, count(*)::bigint AS well_count
        FROM ks_well
        WHERE county IS NOT NULL
          AND btrim(county) <> ''
          AND is_tombstoned = false
        GROUP BY county
        ORDER BY count(*) DESC, county ASC
        LIMIT $1
        "#,
    )
    .bind(SUMMARY_LIMIT)
    .fetch_all(pool)
    .await?;

    Ok(rows
        .into_iter()
        .map(|row| BrowseSummaryEntry {
            key: row.get("key"),
            display_name: row.get("display_name"),
            well_count: row.get("well_count"),
        })
        .collect())
}

async fn top_operators(pool: &sqlx::PgPool) -> Result<Vec<BrowseSummaryEntry>, sqlx::Error> {
    let rows = sqlx::query(
        r#"
        SELECT operator_name AS key, operator_name AS display_name, count(*)::bigint AS well_count
        FROM (
            SELECT COALESCE(NULLIF(btrim(operator_name_kgs_reported_current), ''), NULLIF(btrim(operator_name_kgs_original), '')) AS operator_name
            FROM ks_well
            WHERE is_tombstoned = false
        ) AS wells
        WHERE operator_name IS NOT NULL
          AND lower(operator_name) NOT IN ('unavailable', 'unknown', 'n/a', 'na')
        GROUP BY operator_name
        ORDER BY count(*) DESC, operator_name ASC
        LIMIT $1
        "#,
    )
    .bind(SUMMARY_LIMIT)
    .fetch_all(pool)
    .await?;

    Ok(rows
        .into_iter()
        .map(|row| BrowseSummaryEntry {
            key: row.get("key"),
            display_name: row.get("display_name"),
            well_count: row.get("well_count"),
        })
        .collect())
}

async fn top_fields(pool: &sqlx::PgPool) -> Result<Vec<BrowseSummaryEntry>, sqlx::Error> {
    let rows = sqlx::query(
        r#"
        SELECT field_name AS key, field_name AS display_name, count(*)::bigint AS well_count
        FROM ks_well
        WHERE field_name IS NOT NULL
          AND btrim(field_name) <> ''
          AND lower(btrim(field_name)) NOT IN ('unknown', 'unnamed')
          AND is_tombstoned = false
        GROUP BY field_name
        ORDER BY count(*) DESC, field_name ASC
        LIMIT $1
        "#,
    )
    .bind(SUMMARY_LIMIT)
    .fetch_all(pool)
    .await?;

    Ok(rows
        .into_iter()
        .map(|row| BrowseSummaryEntry {
            key: row.get("key"),
            display_name: row.get("display_name"),
            well_count: row.get("well_count"),
        })
        .collect())
}

async fn recent_activity(pool: &sqlx::PgPool) -> Result<Vec<BrowseActivityEvent>, sqlx::Error> {
    let rows = sqlx::query(
        r#"
        SELECT
            e.id,
            e.event_type,
            COALESCE(e.occurred_at, e.published_at) AS event_date,
            e.observed_at,
            w.api14,
            w.api_raw,
            w.kgs_kid,
            w.well_name,
            COALESCE(e.related_county, w.county) AS county,
            COALESCE(w.operator_name_kgs_reported_current, w.operator_name_kgs_original) AS operator,
            w.field_name,
            w.lease_name
        FROM ks_event e
        JOIN ks_well w ON w.internal_well_id = e.related_well_id
        WHERE w.source_ingestion_run_id IN (
            SELECT r.id
            FROM ks_ingestion_run r
            JOIN ks_source s ON s.id = r.source_id
            WHERE s.code IN ('KGS_WELLS_ARCGIS', 'KGS_WELLS_ZIP')
              AND r.status = 'completed'
              AND r.row_count > 100000
        )
        ORDER BY COALESCE(e.occurred_at, e.published_at, e.observed_at) DESC, e.created_at DESC
        LIMIT $1
        "#,
    )
    .bind(ACTIVITY_LIMIT)
    .fetch_all(pool)
    .await?;

    Ok(rows
        .into_iter()
        .map(|row| {
            let event_type: String = row.get("event_type");
            BrowseActivityEvent {
                id: row.get("id"),
                title: human_event_type(&event_type),
                event_type,
                event_date: row.get("event_date"),
                observed_at: row.get("observed_at"),
                api14: row.get("api14"),
                api_raw: row.get("api_raw"),
                kgs_kid: row.get("kgs_kid"),
                well_name: row.get("well_name"),
                county: row.get("county"),
                operator: row.get("operator"),
                field_name: row.get("field_name"),
                lease_name: row.get("lease_name"),
            }
        })
        .collect())
}

async fn featured_well_activity(
    pool: &sqlx::PgPool,
) -> Result<Vec<BrowseActivityEvent>, sqlx::Error> {
    let rows = sqlx::query(
        r#"
        SELECT
            internal_well_id AS id,
            CASE
                WHEN lifecycle_status = 'planned' THEN 'WELL_PERMITTED'
                WHEN lifecycle_status = 'producing' THEN 'PRODUCING_WELL'
                ELSE 'WELL_RECORD'
            END AS event_type,
            CASE
                WHEN lifecycle_status = 'planned' THEN 'Permitted Well'
                WHEN lifecycle_status = 'producing' THEN 'Producing Well'
                ELSE 'Well Record'
            END AS title,
            COALESCE(permit_date, completion_date, spud_date, last_seen_at::date)::timestamptz AS event_date,
            COALESCE(last_seen_at, updated_at, created_at) AS observed_at,
            api14,
            api_raw,
            kgs_kid,
            well_name,
            county,
            COALESCE(NULLIF(btrim(operator_name_kgs_reported_current), ''), NULLIF(btrim(operator_name_kgs_original), '')) AS operator,
            field_name,
            lease_name
        FROM ks_well
        WHERE is_tombstoned = false
          AND lifecycle_status IN ('planned', 'producing')
          AND source_ingestion_run_id IN (
              SELECT r.id
              FROM ks_ingestion_run r
              JOIN ks_source s ON s.id = r.source_id
              WHERE s.code IN ('KGS_WELLS_ARCGIS', 'KGS_WELLS_ZIP')
                AND r.status = 'completed'
                AND r.row_count > 100000
          )
        ORDER BY
            CASE WHEN COALESCE(permit_date, completion_date, spud_date) IS NULL THEN 1 ELSE 0 END,
            COALESCE(permit_date, completion_date, spud_date, last_seen_at::date) DESC NULLS LAST,
            CASE WHEN lifecycle_status = 'planned' THEN 0 ELSE 1 END,
            updated_at DESC,
            api14 ASC NULLS LAST
        LIMIT $1
        "#,
    )
    .bind(ACTIVITY_LIMIT)
    .fetch_all(pool)
    .await?;

    Ok(rows
        .into_iter()
        .map(|row| BrowseActivityEvent {
            id: row.get("id"),
            event_type: row.get("event_type"),
            title: row.get("title"),
            event_date: row.get("event_date"),
            observed_at: row.get("observed_at"),
            api14: row.get("api14"),
            api_raw: row.get("api_raw"),
            kgs_kid: row.get("kgs_kid"),
            well_name: row.get("well_name"),
            county: row.get("county"),
            operator: row.get("operator"),
            field_name: row.get("field_name"),
            lease_name: row.get("lease_name"),
        })
        .collect())
}

async fn load_entity_detail(
    pool: &sqlx::PgPool,
    kind: EntityKind,
    slug: &str,
) -> Result<BrowseEntityDetail, ApiError> {
    let summary = entity_summary(pool, kind, slug)
        .await?
        .ok_or_else(|| ApiError::NotFound(format!("Kansas {} page not found", kind.singular())))?;
    let top_counties = related_summary(pool, kind, slug, EntityKind::County).await?;
    let top_operators = related_summary(pool, kind, slug, EntityKind::Operator).await?;
    let top_fields = related_summary(pool, kind, slug, EntityKind::Field).await?;
    let status_counts = status_counts(pool, kind, slug).await?;
    let permitted_wells =
        entity_wells_by_group(pool, kind, slug, None, WellGroup::Permitted, 10).await?;
    let producing_wells =
        entity_wells_by_group(pool, kind, slug, None, WellGroup::Producing, 10).await?;
    let completed_wells =
        entity_wells_by_group(pool, kind, slug, None, WellGroup::Completed, 10).await?;
    let wells = representative_wells(&permitted_wells, &producing_wells, 24);

    Ok(BrowseEntityDetail {
        kind: kind.path_segment(),
        key: slugify(&summary.display_name),
        display_name: summary.display_name,
        well_count: summary.well_count,
        producing_count: summary.producing_count,
        permit_count: summary.permit_count,
        located_well_count: summary.located_well_count,
        data_as_of: summary.data_as_of,
        top_counties,
        top_operators,
        top_fields,
        status_counts,
        wells,
        permitted_wells,
        producing_wells,
        completed_wells,
    })
}

#[derive(Debug)]
struct EntitySummary {
    display_name: String,
    well_count: i64,
    producing_count: i64,
    permit_count: i64,
    located_well_count: i64,
    data_as_of: Option<DateTime<Utc>>,
}

async fn entity_summary(
    pool: &sqlx::PgPool,
    kind: EntityKind,
    slug: &str,
) -> Result<Option<EntitySummary>, sqlx::Error> {
    let expr = kind.sql_expr();
    let sql = format!(
        r#"
        SELECT
            {expr} AS display_name,
            count(*)::bigint AS well_count,
            count(*) FILTER (WHERE lifecycle_status = 'producing')::bigint AS producing_count,
            count(*) FILTER (WHERE lifecycle_status IN ('planned', 'intent_expired'))::bigint AS permit_count,
            count(*) FILTER (WHERE display_geometry_4326 IS NOT NULL OR (source_latitude IS NOT NULL AND source_longitude IS NOT NULL))::bigint AS located_well_count,
            max(last_seen_at) AS data_as_of
        FROM ks_well
        WHERE {entity_where}
        GROUP BY display_name
        ORDER BY count(*) DESC, display_name ASC
        LIMIT 1
        "#,
        entity_where = entity_where_sql(kind, expr)
    );
    sqlx::query(&sql)
        .bind(slug)
        .fetch_optional(pool)
        .await
        .map(|row| {
            row.map(|row| EntitySummary {
                display_name: row.get("display_name"),
                well_count: row.get("well_count"),
                producing_count: row.get("producing_count"),
                permit_count: row.get("permit_count"),
                located_well_count: row.get("located_well_count"),
                data_as_of: row.get("data_as_of"),
            })
        })
}

async fn related_summary(
    pool: &sqlx::PgPool,
    scoped_kind: EntityKind,
    slug: &str,
    related_kind: EntityKind,
) -> Result<Vec<BrowseSummaryEntry>, sqlx::Error> {
    if scoped_kind == related_kind {
        return Ok(Vec::new());
    }

    let scoped_expr = scoped_kind.sql_expr();
    let related_expr = related_kind.sql_expr();
    let sql = format!(
        r#"
        SELECT {related_expr} AS key, {related_expr} AS display_name, count(*)::bigint AS well_count
        FROM ks_well
        WHERE {scoped_where}
          AND {related_valid}
        GROUP BY key, display_name
        ORDER BY count(*) DESC, display_name ASC
        LIMIT $2
        "#,
        scoped_where = entity_where_sql(scoped_kind, scoped_expr),
        related_valid = valid_value_sql(related_kind, related_expr)
    );
    let rows = sqlx::query(&sql)
        .bind(slug)
        .bind(SUMMARY_LIMIT)
        .fetch_all(pool)
        .await?;
    Ok(rows
        .into_iter()
        .map(|row| BrowseSummaryEntry {
            key: slugify(&row.get::<String, _>("key")),
            display_name: row.get("display_name"),
            well_count: row.get("well_count"),
        })
        .collect())
}

async fn status_counts(
    pool: &sqlx::PgPool,
    scoped_kind: EntityKind,
    slug: &str,
) -> Result<Vec<BrowseSummaryEntry>, sqlx::Error> {
    let scoped_expr = scoped_kind.sql_expr();
    let sql = format!(
        r#"
        SELECT lifecycle_status AS key, lifecycle_status AS display_name, count(*)::bigint AS well_count
        FROM ks_well
        WHERE {scoped_where}
          AND lifecycle_status IS NOT NULL
          AND btrim(lifecycle_status) <> ''
        GROUP BY lifecycle_status
        ORDER BY count(*) DESC, lifecycle_status ASC
        LIMIT $2
        "#,
        scoped_where = entity_where_sql(scoped_kind, scoped_expr)
    );
    let rows = sqlx::query(&sql)
        .bind(slug)
        .bind(SUMMARY_LIMIT)
        .fetch_all(pool)
        .await?;
    Ok(rows
        .into_iter()
        .map(|row| BrowseSummaryEntry {
            key: row.get("key"),
            display_name: row.get("display_name"),
            well_count: row.get("well_count"),
        })
        .collect())
}

async fn entity_wells(
    pool: &sqlx::PgPool,
    kind: EntityKind,
    slug: &str,
    excluded_id: Option<Uuid>,
    limit: i64,
) -> Result<Vec<BrowseWellCard>, sqlx::Error> {
    let expr = kind.sql_expr();
    let sql = format!(
        r#"
        SELECT
            internal_well_id AS id,
            api14,
            api_raw,
            kgs_kid,
            kcc_permit,
            well_name,
            county,
            COALESCE(NULLIF(btrim(operator_name_kgs_reported_current), ''), NULLIF(btrim(operator_name_kgs_original), '')) AS operator,
            field_name,
            lease_name,
            lifecycle_status,
            well_class_raw,
            status_raw,
            status2_raw,
            township,
            township_direction,
            range,
            range_direction,
            section,
            subdivision_1_largest,
            subdivision_2,
            subdivision_3,
            subdivision_4_smallest,
            spot,
            feet_north_from_reference,
            feet_east_from_reference,
            reference_corner,
            permit_date,
            spud_date,
            completion_date,
            plug_date,
            total_depth,
            producing_formation,
            formation_at_total_depth,
            ip_oil,
            ip_gas,
            ip_water,
            comments,
            source_latitude,
            source_longitude,
            horizontal_or_directional,
            trajectory_available,
            source_format,
            source_object_id,
            first_seen_at,
            last_seen_at
        FROM ks_well
        WHERE {entity_where}
          AND ($2::uuid IS NULL OR internal_well_id <> $2)
        ORDER BY
            CASE WHEN lifecycle_status IN ('planned', 'producing') THEN 0 ELSE 1 END,
            CASE WHEN COALESCE(permit_date, completion_date, spud_date, plug_date) IS NULL THEN 1 ELSE 0 END,
            COALESCE(permit_date, completion_date, spud_date, plug_date, last_seen_at::date) DESC NULLS LAST,
            updated_at DESC,
            api14 ASC NULLS LAST
        LIMIT $3
        "#,
        entity_where = entity_where_sql(kind, expr)
    );
    let rows = sqlx::query(&sql)
        .bind(slug)
        .bind(excluded_id)
        .bind(limit)
        .fetch_all(pool)
        .await?;
    Ok(rows.into_iter().map(well_card_from_row).collect())
}

async fn entity_wells_by_group(
    pool: &sqlx::PgPool,
    kind: EntityKind,
    slug: &str,
    excluded_id: Option<Uuid>,
    group: WellGroup,
    limit: i64,
) -> Result<Vec<BrowseWellCard>, sqlx::Error> {
    let expr = kind.sql_expr();
    let sql = format!(
        r#"
        SELECT
            internal_well_id AS id,
            api14,
            api_raw,
            kgs_kid,
            kcc_permit,
            well_name,
            county,
            COALESCE(NULLIF(btrim(operator_name_kgs_reported_current), ''), NULLIF(btrim(operator_name_kgs_original), '')) AS operator,
            field_name,
            lease_name,
            lifecycle_status,
            well_class_raw,
            status_raw,
            status2_raw,
            township,
            township_direction,
            range,
            range_direction,
            section,
            subdivision_1_largest,
            subdivision_2,
            subdivision_3,
            subdivision_4_smallest,
            spot,
            feet_north_from_reference,
            feet_east_from_reference,
            reference_corner,
            permit_date,
            spud_date,
            completion_date,
            plug_date,
            total_depth,
            producing_formation,
            formation_at_total_depth,
            ip_oil,
            ip_gas,
            ip_water,
            comments,
            source_latitude,
            source_longitude,
            horizontal_or_directional,
            trajectory_available,
            source_format,
            source_object_id,
            first_seen_at,
            last_seen_at
        FROM ks_well
        WHERE {entity_where}
          AND ($2::uuid IS NULL OR internal_well_id <> $2)
          AND {group_where}
        ORDER BY
            CASE WHEN {group_date} IS NULL THEN 1 ELSE 0 END,
            {group_date} DESC NULLS LAST,
            updated_at DESC,
            api14 ASC NULLS LAST
        LIMIT $3
        "#,
        entity_where = entity_where_sql(kind, expr),
        group_where = group.where_sql(),
        group_date = group.date_sql()
    );
    let rows = sqlx::query(&sql)
        .bind(slug)
        .bind(excluded_id)
        .bind(limit)
        .fetch_all(pool)
        .await?;
    Ok(rows.into_iter().map(well_card_from_row).collect())
}

fn representative_wells(
    permitted_wells: &[BrowseWellCard],
    producing_wells: &[BrowseWellCard],
    limit: usize,
) -> Vec<BrowseWellCard> {
    let mut wells = permitted_wells
        .iter()
        .chain(producing_wells.iter())
        .cloned()
        .collect::<Vec<_>>();
    wells.sort_by(|left, right| {
        well_activity_date(right)
            .cmp(&well_activity_date(left))
            .then_with(|| right.last_seen_at.cmp(&left.last_seen_at))
            .then_with(|| left.api14.cmp(&right.api14))
    });
    wells.dedup_by_key(|well| well.id);
    wells.truncate(limit);
    wells
}

fn well_activity_date(well: &BrowseWellCard) -> Option<chrono::NaiveDate> {
    well.permit_date
        .or(well.completion_date)
        .or(well.spud_date)
        .or(well.plug_date)
        .or_else(|| well.last_seen_at.map(|value| value.date_naive()))
}

async fn load_well_detail(pool: &sqlx::PgPool, value: &str) -> Result<BrowseWellCard, ApiError> {
    let compact_api = value
        .chars()
        .filter(|ch| ch.is_ascii_alphanumeric())
        .collect::<String>();
    let row = sqlx::query(
        r#"
        SELECT
            internal_well_id AS id,
            api14,
            api_raw,
            kgs_kid,
            kcc_permit,
            well_name,
            county,
            COALESCE(NULLIF(btrim(operator_name_kgs_reported_current), ''), NULLIF(btrim(operator_name_kgs_original), '')) AS operator,
            field_name,
            lease_name,
            lifecycle_status,
            well_class_raw,
            status_raw,
            status2_raw,
            township,
            township_direction,
            range,
            range_direction,
            section,
            subdivision_1_largest,
            subdivision_2,
            subdivision_3,
            subdivision_4_smallest,
            spot,
            feet_north_from_reference,
            feet_east_from_reference,
            reference_corner,
            permit_date,
            spud_date,
            completion_date,
            plug_date,
            total_depth,
            producing_formation,
            formation_at_total_depth,
            ip_oil,
            ip_gas,
            ip_water,
            comments,
            source_latitude,
            source_longitude,
            horizontal_or_directional,
            trajectory_available,
            source_format,
            source_object_id,
            first_seen_at,
            last_seen_at
        FROM ks_well
        WHERE is_tombstoned = false
          AND (
              api14 = $1
              OR api_raw = $1
              OR regexp_replace(api_raw, '[^0-9A-Za-z]+', '', 'g') = $2
              OR kgs_kid = $1
              OR internal_well_id::text = $1
          )
        ORDER BY
            CASE WHEN api14 = $1 OR api_raw = $1 THEN 0 ELSE 1 END,
            last_seen_at DESC NULLS LAST,
            updated_at DESC
        LIMIT 1
        "#,
    )
    .bind(value)
    .bind(compact_api)
    .fetch_optional(pool)
    .await?;
    row.map(well_card_from_row)
        .ok_or_else(|| ApiError::NotFound("Kansas well page not found".to_string()))
}

fn well_card_from_row(row: sqlx::postgres::PgRow) -> BrowseWellCard {
    BrowseWellCard {
        id: row.get("id"),
        api14: row.get("api14"),
        api_raw: row.get("api_raw"),
        kgs_kid: row.get("kgs_kid"),
        kcc_permit: row.get("kcc_permit"),
        well_name: row.get("well_name"),
        county: row.get("county"),
        operator: row.get("operator"),
        field_name: row.get("field_name"),
        lease_name: row.get("lease_name"),
        lifecycle_status: row.get("lifecycle_status"),
        well_class_raw: row.get("well_class_raw"),
        status_raw: row.get("status_raw"),
        status2_raw: row.get("status2_raw"),
        township: row.get("township"),
        township_direction: row.get("township_direction"),
        range: row.get("range"),
        range_direction: row.get("range_direction"),
        section: row.get("section"),
        subdivision_1_largest: row.get("subdivision_1_largest"),
        subdivision_2: row.get("subdivision_2"),
        subdivision_3: row.get("subdivision_3"),
        subdivision_4_smallest: row.get("subdivision_4_smallest"),
        spot: row.get("spot"),
        feet_north_from_reference: row.get("feet_north_from_reference"),
        feet_east_from_reference: row.get("feet_east_from_reference"),
        reference_corner: row.get("reference_corner"),
        permit_date: row.get("permit_date"),
        spud_date: row.get("spud_date"),
        completion_date: row.get("completion_date"),
        plug_date: row.get("plug_date"),
        total_depth: row.get("total_depth"),
        producing_formation: row.get("producing_formation"),
        formation_at_total_depth: row.get("formation_at_total_depth"),
        ip_oil: row.get("ip_oil"),
        ip_gas: row.get("ip_gas"),
        ip_water: row.get("ip_water"),
        comments: row.get("comments"),
        source_latitude: row.get("source_latitude"),
        source_longitude: row.get("source_longitude"),
        horizontal_or_directional: row.get("horizontal_or_directional"),
        trajectory_available: row.get("trajectory_available"),
        source_format: row.get("source_format"),
        source_object_id: row.get("source_object_id"),
        first_seen_at: row.get("first_seen_at"),
        last_seen_at: row.get("last_seen_at"),
    }
}

fn detail_meta() -> Meta {
    Meta {
        state: crate::constants::STATE_KEY,
        source_ids: vec!["ks_well".to_string()],
        ..Default::default()
    }
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
enum EntityKind {
    County,
    Operator,
    Field,
}

#[derive(Copy, Clone, Debug)]
enum WellGroup {
    Permitted,
    Producing,
    Completed,
}

impl WellGroup {
    fn where_sql(self) -> &'static str {
        match self {
            WellGroup::Permitted => "lifecycle_status IN ('planned', 'intent_expired')",
            WellGroup::Producing => "lifecycle_status = 'producing'",
            WellGroup::Completed => "lifecycle_status = 'completed'",
        }
    }

    fn date_sql(self) -> &'static str {
        match self {
            WellGroup::Permitted => "COALESCE(permit_date, last_seen_at::date)",
            WellGroup::Producing => {
                "COALESCE(completion_date, spud_date, permit_date, last_seen_at::date)"
            }
            WellGroup::Completed => {
                "COALESCE(completion_date, spud_date, permit_date, last_seen_at::date)"
            }
        }
    }
}

impl EntityKind {
    fn singular(self) -> &'static str {
        match self {
            EntityKind::County => "county",
            EntityKind::Operator => "operator",
            EntityKind::Field => "field",
        }
    }

    fn path_segment(self) -> &'static str {
        match self {
            EntityKind::County => "counties",
            EntityKind::Operator => "operators",
            EntityKind::Field => "fields",
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

fn entity_where_sql(kind: EntityKind, expr: &str) -> String {
    format!(
        r#"
        is_tombstoned = false
          AND {valid_value}
          AND {slug_expr} = $1
        "#,
        valid_value = valid_value_sql(kind, expr),
        slug_expr = sql_slug_expr(expr)
    )
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

fn sql_slug_expr(expr: &str) -> String {
    format!(
        "trim(both '-' from lower(regexp_replace(regexp_replace(btrim({expr}), '&', ' and ', 'g'), '[^a-zA-Z0-9]+', '-', 'g')))"
    )
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

fn human_event_type(value: &str) -> String {
    value
        .split('_')
        .filter(|part| !part.is_empty())
        .map(|part| {
            let lower = part.to_lowercase();
            let mut chars = lower.chars();
            match chars.next() {
                Some(first) => first.to_uppercase().collect::<String>() + chars.as_str(),
                None => String::new(),
            }
        })
        .collect::<Vec<_>>()
        .join(" ")
}

#[cfg(test)]
mod tests {
    use super::{human_event_type, slugify};

    #[test]
    fn human_event_type_formats_screaming_snake_case() {
        assert_eq!(human_event_type("INTENT_POSTED"), "Intent Posted");
        assert_eq!(
            human_event_type("LEASE_PRODUCTION_UPDATED"),
            "Lease Production Updated"
        );
    }

    #[test]
    fn slugify_matches_public_entity_paths() {
        assert_eq!(slugify("Montgomery County"), "montgomery-county");
        assert_eq!(
            slugify("Eiger Operating Company, LLC"),
            "eiger-operating-company-llc"
        );
        assert_eq!(slugify("Paola-Rantoul"), "paola-rantoul");
    }
}
