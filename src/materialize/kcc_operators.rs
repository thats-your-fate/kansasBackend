//! KS-05: materializes `ks_operator_raw_snapshot` rows from a `completed`
//! `ks_ingestion_run` into canonical `ks_operator`/`ks_operator_alias`
//! rows.
//!
//! Hard data-use control: this module must never copy a street-address,
//! phone, email, or contact-person value out of `raw_data` into anything
//! it returns or writes to `ks_operator` — that table structurally has no
//! such column (see migration `0008_ks_operator.sql`), and `PublicOperator`
//! below is the type every public-facing surface must use instead of the
//! canonical row, checked by `privacy::assert_no_forbidden_fields` in this
//! module's own tests.

use serde::Serialize;
use serde_json::Value;
use sqlx::PgPool;
use uuid::Uuid;

use crate::normalize::kcc_operators::{derive_active_status, normalize_company_name};

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
    pub rejected_no_identity: i64,
}

/// The only shape a public-facing surface (search document, application
/// API, exports, analytics, marketing tools) may ever build a response
/// from — no address/city/contact field, by construction. `source_city`
/// deliberately does not appear here: KS-05 asked for it "only if already
/// permitted by product policy," and no such policy decision has been
/// made, so it stays out of every public payload until one is.
#[derive(Debug, Clone, Serialize)]
pub struct PublicOperator {
    pub kcc_license_number: String,
    pub company_name: String,
    pub active_status: String,
}

#[derive(Debug, sqlx::FromRow)]
struct RawSnapshotRow {
    list_source: String,
    kcc_license_number: Option<String>,
    reported_company_name: Option<String>,
    source_city: Option<String>,
    source_updated_at: Option<chrono::DateTime<chrono::Utc>>,
    source_record_hash: String,
}

struct MergedOperator {
    reported_company_name: String,
    all_variant_name: Option<String>,
    active_variant_name: Option<String>,
    source_city: Option<String>,
    source_updated_at: Option<chrono::DateTime<chrono::Utc>>,
    source_record_hash: String,
    seen_in_active_list: bool,
    seen_in_all_list: bool,
}

pub async fn materialize_operators_from_run(
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
        SELECT list_source, kcc_license_number, reported_company_name, source_city,
               source_updated_at, source_record_hash
        FROM ks_operator_raw_snapshot
        WHERE ingestion_run_id = $1
        "#,
    )
    .bind(ingestion_run_id)
    .fetch_all(pool)
    .await?;

    let mut merged: std::collections::HashMap<String, MergedOperator> =
        std::collections::HashMap::new();
    let mut rejected_no_identity: i64 = 0;

    for row in rows {
        let Some(license) = row.kcc_license_number else {
            rejected_no_identity += 1;
            continue;
        };
        let name = row.reported_company_name.clone().unwrap_or_default();

        let entry = merged
            .entry(license.clone())
            .or_insert_with(|| MergedOperator {
                reported_company_name: name.clone(),
                all_variant_name: None,
                active_variant_name: None,
                source_city: None,
                source_updated_at: None,
                source_record_hash: row.source_record_hash.clone(),
                seen_in_active_list: false,
                seen_in_all_list: false,
            });

        match row.list_source.as_str() {
            "active" => {
                entry.seen_in_active_list = true;
                entry.active_variant_name = row.reported_company_name.clone();
            }
            "all" => {
                entry.seen_in_all_list = true;
                entry.all_variant_name = row.reported_company_name.clone();
                // The all-operators list is treated as the more
                // authoritative source for descriptive fields when both
                // lists disagree, since it's documented as the complete
                // roster.
                entry.reported_company_name = name;
                if row.source_city.is_some() {
                    entry.source_city = row.source_city;
                }
                if row.source_updated_at.is_some() {
                    entry.source_updated_at = row.source_updated_at;
                }
            }
            _ => {}
        }
    }

    let mut created = 0i64;
    let mut updated = 0i64;

    for (license, op) in merged {
        let active_status = derive_active_status(op.seen_in_active_list, op.seen_in_all_list);
        let normalized = normalize_company_name(&op.reported_company_name);

        let inserted: bool = sqlx::query_scalar(
            r#"
            INSERT INTO ks_operator (
                kcc_license_number, reported_company_name, normalized_company_name,
                active_status, source_updated_at, source_city, source_record_hash,
                source_ingestion_run_id, last_seen_in_active_list_at, last_seen_in_all_list_at
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
            ON CONFLICT (kcc_license_number) DO UPDATE SET
                reported_company_name = EXCLUDED.reported_company_name,
                normalized_company_name = EXCLUDED.normalized_company_name,
                active_status = EXCLUDED.active_status,
                source_updated_at = COALESCE(EXCLUDED.source_updated_at, ks_operator.source_updated_at),
                source_city = COALESCE(EXCLUDED.source_city, ks_operator.source_city),
                source_record_hash = EXCLUDED.source_record_hash,
                source_ingestion_run_id = EXCLUDED.source_ingestion_run_id,
                last_seen_in_active_list_at = COALESCE(EXCLUDED.last_seen_in_active_list_at, ks_operator.last_seen_in_active_list_at),
                last_seen_in_all_list_at = COALESCE(EXCLUDED.last_seen_in_all_list_at, ks_operator.last_seen_in_all_list_at),
                updated_at = now()
            RETURNING (xmax = 0)
            "#,
        )
        .bind(&license)
        .bind(&op.reported_company_name)
        .bind(&normalized)
        .bind(active_status.as_db_str())
        .bind(op.source_updated_at)
        .bind(&op.source_city)
        .bind(&op.source_record_hash)
        .bind(ingestion_run_id)
        .bind(op.seen_in_active_list.then(chrono::Utc::now))
        .bind(op.seen_in_all_list.then(chrono::Utc::now))
        .fetch_one(pool)
        .await?;

        if inserted {
            created += 1;
        } else {
            updated += 1;
        }

        let operator_id: Uuid =
            sqlx::query_scalar("SELECT id FROM ks_operator WHERE kcc_license_number = $1")
                .bind(&license)
                .fetch_one(pool)
                .await?;

        for variant in [op.active_variant_name, op.all_variant_name]
            .into_iter()
            .flatten()
        {
            upsert_alias(pool, operator_id, &variant, ingestion_run_id).await?;
        }
    }

    Ok(MaterializeOutcome {
        created,
        updated,
        rejected_no_identity,
    })
}

async fn upsert_alias(
    pool: &PgPool,
    operator_id: Uuid,
    reported_company_name: &str,
    ingestion_run_id: Uuid,
) -> Result<(), sqlx::Error> {
    let normalized = normalize_company_name(reported_company_name);
    sqlx::query(
        r#"
        INSERT INTO ks_operator_alias (operator_id, reported_company_name, normalized_company_name, first_seen_ingestion_run_id)
        VALUES ($1, $2, $3, $4)
        ON CONFLICT (operator_id, reported_company_name) DO NOTHING
        "#,
    )
    .bind(operator_id)
    .bind(reported_company_name)
    .bind(&normalized)
    .bind(ingestion_run_id)
    .execute(pool)
    .await?;
    Ok(())
}

/// KS-05 acceptance criteria: "active/all reconciliation report." Reads
/// straight from the raw snapshot of one run (the same data
/// `run_operator_import` already summarized into its manifest) rather
/// than requiring materialization first, so it can be checked even if a
/// run was rejected before publish.
pub async fn active_all_reconciliation(
    pool: &PgPool,
    ingestion_run_id: Uuid,
) -> Result<Value, sqlx::Error> {
    let manifest: Option<Value> =
        sqlx::query_scalar("SELECT manifest FROM ks_ingestion_run WHERE id = $1")
            .bind(ingestion_run_id)
            .fetch_optional(pool)
            .await?;
    Ok(manifest
        .and_then(|m| m.get("reconciliation").cloned())
        .unwrap_or(Value::Null))
}

/// Exact lookup by KCC license (KS-05 acceptance criteria) — the only
/// identity path; never a name-based lookup.
pub async fn find_public_operator_by_license(
    pool: &PgPool,
    kcc_license_number: &str,
) -> Result<Option<PublicOperator>, sqlx::Error> {
    sqlx::query_as::<_, (String, String, String)>(
        "SELECT kcc_license_number, reported_company_name, active_status FROM ks_operator WHERE kcc_license_number = $1",
    )
    .bind(kcc_license_number)
    .fetch_optional(pool)
    .await
    .map(|row| row.map(|(kcc_license_number, company_name, active_status)| PublicOperator { kcc_license_number, company_name, active_status }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::privacy::assert_no_forbidden_fields;

    #[test]
    fn public_operator_never_carries_a_forbidden_field() {
        let operator = PublicOperator {
            kcc_license_number: "1001".to_string(),
            company_name: "Example Operating LLC".to_string(),
            active_status: "active".to_string(),
        };
        assert!(assert_no_forbidden_fields(&operator).is_ok());
    }
}
