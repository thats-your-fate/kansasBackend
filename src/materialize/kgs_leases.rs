//! KS-09: materializes `ks_lease_raw_snapshot` rows from a `completed`
//! `ks_ingestion_run` into the canonical `ks_lease` dimension, then links
//! wells to leases via `ks_well_lease_link` in the task doc's explicit
//! precedence order: exact KID/DOR_ID identifiers (scoped by product, so
//! oil and gas identifiers can never collide) → exact API numbers listed
//! in the lease's own production data → name matching, only as a last
//! resort. Nothing here computes or stores any ownership/title concept —
//! see `constants::LEASE_OWNERSHIP_DISCLAIMER`.

use chrono::NaiveDate;
use serde_json::json;
use sqlx::PgPool;
use uuid::Uuid;

use crate::normalize::kgs_leases::{
    derive_product, normalize_api_token, parse_api_number_list, LeaseProduct,
};
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
    pub leases_created: i64,
    pub leases_updated: i64,
    pub links_created: i64,
    pub unresolved_api_count: i64,
}

#[derive(Debug, sqlx::FromRow)]
struct RawSnapshotRow {
    lease_kid: String,
    reported_lease_name: Option<String>,
    dor_code: Option<String>,
    api_number_raw: Option<String>,
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
    produces_raw: Option<String>,
    production: Option<f64>,
    year_start: Option<i32>,
    year_stop: Option<i32>,
    source_url: Option<String>,
    source_record_hash: String,
}

pub async fn materialize_leases_from_run(
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
        SELECT lease_kid, reported_lease_name, dor_code, api_number_raw, operator, county, township,
               township_direction, range, range_direction, section, spot, source_latitude, source_longitude,
               produces_raw, production, year_start, year_stop, source_url, source_record_hash
        FROM ks_lease_raw_snapshot
        WHERE ingestion_run_id = $1 AND lease_kid IS NOT NULL
        "#,
    )
    .bind(ingestion_run_id)
    .fetch_all(pool)
    .await?;

    let mut outcome = MaterializeOutcome::default();

    for row in rows {
        let product = derive_product(row.produces_raw.as_deref());
        let valid_from = row
            .year_start
            .and_then(|y| NaiveDate::from_ymd_opt(y, 1, 1));
        let valid_to = row
            .year_stop
            .and_then(|y| NaiveDate::from_ymd_opt(y, 12, 31));

        let inserted: bool = sqlx::query_scalar(
            r#"
            INSERT INTO ks_lease (
                lease_kid, dor_code, product, reported_lease_name, operator, county, township,
                township_direction, range, range_direction, section, spot, source_latitude, source_longitude,
                cumulative_production, year_start, year_stop, source_document_url,
                source_ingestion_run_id, source_record_hash
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20)
            ON CONFLICT (lease_kid) WHERE lease_kid IS NOT NULL DO UPDATE SET
                dor_code = EXCLUDED.dor_code,
                product = EXCLUDED.product,
                reported_lease_name = EXCLUDED.reported_lease_name,
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
                cumulative_production = EXCLUDED.cumulative_production,
                year_start = EXCLUDED.year_start,
                year_stop = EXCLUDED.year_stop,
                source_document_url = EXCLUDED.source_document_url,
                source_ingestion_run_id = EXCLUDED.source_ingestion_run_id,
                source_record_hash = EXCLUDED.source_record_hash,
                updated_at = now()
            RETURNING (xmax = 0)
            "#,
        )
        .bind(&row.lease_kid)
        .bind(&row.dor_code)
        .bind(product.as_db_str())
        .bind(&row.reported_lease_name)
        .bind(&row.operator)
        .bind(&row.county)
        .bind(row.township)
        .bind(&row.township_direction)
        .bind(row.range)
        .bind(&row.range_direction)
        .bind(row.section)
        .bind(&row.spot)
        .bind(row.source_latitude)
        .bind(row.source_longitude)
        .bind(row.production)
        .bind(row.year_start)
        .bind(row.year_stop)
        .bind(&row.source_url)
        .bind(ingestion_run_id)
        .bind(&row.source_record_hash)
        .fetch_one(pool)
        .await?;

        if inserted {
            outcome.leases_created += 1;
        } else {
            outcome.leases_updated += 1;
        }

        let lease_id: Uuid = sqlx::query_scalar("SELECT id FROM ks_lease WHERE lease_kid = $1")
            .bind(&row.lease_kid)
            .fetch_one(pool)
            .await?;

        if let Some(dor_code) = &row.dor_code {
            upsert_alias(pool, lease_id, "dor_code", dor_code).await?;
        }
        upsert_alias(pool, lease_id, "lease_kid", &row.lease_kid).await?;

        if product == LeaseProduct::Unknown {
            // No product to scope exact-identifier matching against —
            // still a real lease row, just not linkable via KID/DOR_ID
            // tiers (task doc: oil/gas identifiers are product-scoped).
            continue;
        }

        let mut linked_well_ids: std::collections::HashSet<Uuid> = std::collections::HashSet::new();

        // Tier 1: exact identifier, scoped by product so an OIL_KID can
        // never match a gas-product lease's lease_kid, and vice versa.
        let exact_wells =
            find_wells_by_exact_identifier(pool, product, &row.lease_kid, row.dor_code.as_deref())
                .await?;
        for well_id in exact_wells {
            if linked_well_ids.insert(well_id) {
                insert_link(
                    pool,
                    well_id,
                    lease_id,
                    product,
                    "OIL_KID_OR_GAS_KID",
                    "exact_identifier",
                    1.0,
                    valid_from,
                    valid_to,
                    ingestion_run_id,
                )
                .await?;
                outcome.links_created += 1;
            }
        }

        // Tier 2: API numbers listed in this lease's own production data.
        let api_tokens = parse_api_number_list(row.api_number_raw.as_deref());
        for token in &api_tokens {
            let Some(api14) = normalize_api_token(token) else {
                continue;
            };
            let well_id: Option<Uuid> =
                sqlx::query_scalar("SELECT internal_well_id FROM ks_well WHERE api14 = $1")
                    .bind(&api14)
                    .fetch_optional(pool)
                    .await?;
            match well_id {
                Some(well_id) => {
                    if linked_well_ids.insert(well_id) {
                        insert_link(
                            pool,
                            well_id,
                            lease_id,
                            product,
                            "API_NUMBER",
                            "exact_api_number",
                            1.0,
                            valid_from,
                            valid_to,
                            ingestion_run_id,
                        )
                        .await?;
                        outcome.links_created += 1;
                    }
                }
                None => {
                    ingestion_run::record_rejection(
                        pool,
                        ingestion_run_id,
                        None,
                        Some(json!({"lease_kid": row.lease_kid, "api_number_token": token})),
                        "unresolved production API — no matching ks_well.api14",
                    )
                    .await?;
                    outcome.unresolved_api_count += 1;
                }
            }
        }

        // Tier 3: name match, only when nothing above found any well at all.
        if linked_well_ids.is_empty() {
            if let Some(name) = &row.reported_lease_name {
                let name_wells =
                    find_wells_by_exact_lease_name(pool, name, row.county.as_deref()).await?;
                for well_id in name_wells {
                    if linked_well_ids.insert(well_id) {
                        insert_link(
                            pool,
                            well_id,
                            lease_id,
                            product,
                            "LEASE_NAME",
                            "name_match",
                            0.5,
                            valid_from,
                            valid_to,
                            ingestion_run_id,
                        )
                        .await?;
                        outcome.links_created += 1;
                    }
                }
            }
        }
    }

    Ok(outcome)
}

async fn find_wells_by_exact_identifier(
    pool: &PgPool,
    product: LeaseProduct,
    lease_kid: &str,
    dor_code: Option<&str>,
) -> Result<Vec<Uuid>, sqlx::Error> {
    let (kid_col, dor_col) = match product {
        LeaseProduct::Oil => ("oil_kid", "oil_dor_id"),
        LeaseProduct::Gas => ("gas_kid", "gas_dor_id"),
        LeaseProduct::Unknown => return Ok(Vec::new()),
    };
    let query = format!("SELECT internal_well_id FROM ks_well WHERE {kid_col} = $1 OR ($2::text IS NOT NULL AND {dor_col} = $2)");
    sqlx::query_scalar(&query)
        .bind(lease_kid)
        .bind(dor_code)
        .fetch_all(pool)
        .await
}

async fn find_wells_by_exact_lease_name(
    pool: &PgPool,
    lease_name: &str,
    county: Option<&str>,
) -> Result<Vec<Uuid>, sqlx::Error> {
    sqlx::query_scalar(
        r#"
        SELECT internal_well_id FROM ks_well
        WHERE upper(trim(lease_name)) = upper(trim($1))
          AND ($2::text IS NULL OR county = $2)
        "#,
    )
    .bind(lease_name)
    .bind(county)
    .fetch_all(pool)
    .await
}

async fn upsert_alias(
    pool: &PgPool,
    lease_id: Uuid,
    alias_type: &str,
    alias_value: &str,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        r#"
        INSERT INTO ks_lease_source_alias (lease_id, alias_type, alias_value)
        VALUES ($1, $2, $3)
        ON CONFLICT (lease_id, alias_type, alias_value) DO UPDATE SET last_seen_at = now()
        "#,
    )
    .bind(lease_id)
    .bind(alias_type)
    .bind(alias_value)
    .execute(pool)
    .await?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn insert_link(
    pool: &PgPool,
    well_id: Uuid,
    lease_id: Uuid,
    product: LeaseProduct,
    link_source: &str,
    link_method: &str,
    confidence: f64,
    valid_from: Option<NaiveDate>,
    valid_to: Option<NaiveDate>,
    ingestion_run_id: Uuid,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        r#"
        INSERT INTO ks_well_lease_link (well_id, lease_id, product, link_source, link_method, confidence, valid_from, valid_to, source_ingestion_run_id)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
        ON CONFLICT (well_id, lease_id, product, link_method) DO UPDATE SET
            valid_from = EXCLUDED.valid_from,
            valid_to = EXCLUDED.valid_to,
            source_ingestion_run_id = EXCLUDED.source_ingestion_run_id
        "#,
    )
    .bind(well_id)
    .bind(lease_id)
    .bind(product.as_db_str())
    .bind(link_source)
    .bind(link_method)
    .bind(confidence)
    .bind(valid_from)
    .bind(valid_to)
    .bind(ingestion_run_id)
    .execute(pool)
    .await?;
    Ok(())
}

#[derive(Debug, Clone, sqlx::FromRow)]
pub struct PublicLease {
    pub id: Uuid,
    pub lease_kid: Option<String>,
    pub reported_lease_name: Option<String>,
    pub product: String,
    pub county: Option<String>,
    pub ownership_disclaimer: String,
}

#[derive(sqlx::FromRow)]
struct LeaseIdentityRow {
    id: Uuid,
    lease_kid: Option<String>,
    reported_lease_name: Option<String>,
    product: String,
    county: Option<String>,
}

/// Exact lookup by source (`lease_kid`) or reported name (task doc:
/// "Store source aliases" / lease lookup, mirroring KS-08's `find_field`).
pub async fn find_lease(
    pool: &PgPool,
    lease_kid_or_name: &str,
) -> Result<Option<PublicLease>, sqlx::Error> {
    let row: Option<LeaseIdentityRow> = sqlx::query_as(
        "SELECT id, lease_kid, reported_lease_name, product, county FROM ks_lease WHERE lease_kid = $1 OR reported_lease_name = $1",
    )
    .bind(lease_kid_or_name)
    .fetch_optional(pool)
    .await?;
    Ok(row.map(|r| PublicLease {
        id: r.id,
        lease_kid: r.lease_kid,
        reported_lease_name: r.reported_lease_name,
        product: r.product,
        county: r.county,
        ownership_disclaimer: crate::constants::LEASE_OWNERSHIP_DISCLAIMER.to_string(),
    }))
}

#[derive(Debug, Clone, sqlx::FromRow)]
pub struct UnresolvedLeaseApi {
    pub row_number: Option<i64>,
    pub raw_data: serde_json::Value,
    pub created_at: chrono::DateTime<chrono::Utc>,
}

/// "Unresolved production APIs are reported" (task doc acceptance
/// criteria) — reuses the existing generic `ks_source_rejection`
/// mechanism rather than a new table, scoped to KGS_LEASES-sourced runs.
pub async fn unresolved_lease_apis_report(
    pool: &PgPool,
    limit: i64,
) -> Result<Vec<UnresolvedLeaseApi>, sqlx::Error> {
    sqlx::query_as(
        r#"
        SELECT r.row_number, r.raw_data, r.created_at
        FROM ks_source_rejection r
        JOIN ks_ingestion_run run ON run.id = r.ingestion_run_id
        JOIN ks_source s ON s.id = run.source_id
        WHERE s.code = 'KGS_LEASES' AND r.reason = 'unresolved production API — no matching ks_well.api14'
        ORDER BY r.created_at DESC
        LIMIT $1
        "#,
    )
    .bind(limit)
    .fetch_all(pool)
    .await
}
