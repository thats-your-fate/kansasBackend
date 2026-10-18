//! Explicit, repeatable repairs. Mapping repairs do not emit source-change events.
use crate::normalize::kgs_wells::derive_lifecycle_status;
use sqlx::{PgPool, Row};
use uuid::Uuid;

pub async fn repair_well_mappings(pool: &PgPool) -> anyhow::Result<u64> {
    let combinations =
        sqlx::query("SELECT DISTINCT well_class_raw,status_raw,status2_raw FROM ks_well")
            .fetch_all(pool)
            .await?;
    let mappings: Vec<serde_json::Value> = combinations.into_iter().map(|row| {
        let class: Option<String> = row.get("well_class_raw");
        let status: Option<String> = row.get("status_raw");
        let status2: Option<String> = row.get("status2_raw");
        serde_json::json!({"key":[class,status,status2],"status":derive_lifecycle_status(class.as_deref(),status.as_deref(),status2.as_deref())})
    }).collect();
    Ok(sqlx::query("UPDATE ks_well w SET lifecycle_status=m.status,updated_at=now() FROM jsonb_to_recordset($1::jsonb) AS m(key jsonb,status text) WHERE jsonb_build_array(w.well_class_raw,w.status_raw,w.status2_raw)=m.key AND w.lifecycle_status IS DISTINCT FROM m.status")
        .bind(serde_json::json!(mappings)).execute(pool).await?.rows_affected())
}

/// Historical dated facts only: never invent a status transition or observation date.
/// These rows are visibly marked as history, and retain the actual source event date.
pub async fn backfill_well_history(pool: &PgPool) -> anyhow::Result<u64> {
    Ok(sqlx::query(include_str!("well_history.sql"))
        .execute(pool)
        .await?
        .rows_affected())
}

/// Set-based statewide materialization; only source identifiers and exact API evidence.
pub async fn materialize_verified_leases(pool: &PgPool, run: Uuid) -> anyhow::Result<()> {
    let mut tx = pool.begin().await?;
    let valid: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM ks_ingestion_run r JOIN ks_source s ON s.id=r.source_id WHERE r.id=$1 AND r.status='completed' AND s.code='KGS_LEASES')")
        .bind(run).fetch_one(&mut *tx).await?;
    anyhow::ensure!(valid, "completed KGS_LEASES run required");
    // A transaction prevents readers seeing leases without their verified links.
    for statement in include_str!("verified_leases.sql").split("-- NEXT STATEMENT") {
        sqlx::query(statement).bind(run).execute(&mut *tx).await?;
    }
    tx.commit().await?;
    Ok(())
}
