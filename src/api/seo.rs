use axum::{extract::State, Json};
use serde::Serialize;
use sqlx::Row;

use super::envelope::{Envelope, Meta};
use super::error::ApiError;
use super::state::AppState;

const DIRECTORY_PAGE_SIZE: i64 = 50;

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
        GROUP BY display_name
        ORDER BY lower(display_name) ASC, display_name ASC
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
    use super::{slugify, total_pages};

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
}
