use chrono::Utc;
use future_wells_kansas::{
    materialize::data_repair,
    source::{ingestion_run, registry},
};
use serde_json::json;
use sqlx::PgPool;
use uuid::Uuid;

#[tokio::test]
async fn repairs_are_repeatable_and_lease_links_require_product_scoped_evidence() {
    let pool = PgPool::connect(
        &std::env::var("DATABASE_URL").expect("isolated test DATABASE_URL required"),
    )
    .await
    .unwrap();
    sqlx::migrate!("./migrations").run(&pool).await.unwrap();
    let source = registry::find_source_by_code(&pool, "KGS_LEASES")
        .await
        .unwrap()
        .unwrap();
    let run = ingestion_run::start_run(
        &pool,
        ingestion_run::DownloadedPayload {
            source_id: source.id,
            source_url: "https://www.kgs.ku.edu/PRS/Ora_Archive/all_kansas_leases.zip",
            retrieved_at: Utc::now(),
            source_modified_at: None,
            payload_checksum: &Uuid::new_v4().to_string(),
            schema_fingerprint: None,
            parser_version: "kgs_leases_zip-v1",
        },
    )
    .await
    .unwrap();
    assert!(data_repair::materialize_verified_leases(&pool, run)
        .await
        .is_err());
    let key = Uuid::new_v4().to_string();
    let oil = format!("{key}-oil");
    let gas = format!("{key}-gas");
    let dor = format!("{key}-dor");
    for (kid, product) in [(&oil, "OIL"), (&gas, "GAS")] {
        sqlx::query("INSERT INTO ks_lease_raw_snapshot(ingestion_run_id,lease_kid,produces_raw,dor_code,reported_lease_name,raw_data,source_record_hash) VALUES($1,$2,$3,$4,'Same Name','{}','hash')")
            .bind(run).bind(kid).bind(product).bind(&dor).execute(&pool).await.unwrap();
    }
    ingestion_run::complete_run(&pool, run, 2, 0, None, None, json!({}))
        .await
        .unwrap();
    let well:Uuid=sqlx::query_scalar("INSERT INTO ks_well(api_validation_status,well_identity_match_method,lifecycle_status,status_raw,status2_raw,source_format,source_ingestion_run_id,oil_kid,gas_kid,spud_date,completion_date,plug_date) VALUES('missing','kid_fallback','producing','OIL','Inactive Well','zip',$1,$2,$3,'1995-01-02','1995-02-03',CURRENT_DATE+1) RETURNING internal_well_id")
        .bind(run).bind(&oil).bind(&gas).fetch_one(&pool).await.unwrap();
    data_repair::repair_well_mappings(&pool).await.unwrap();
    let status: String =
        sqlx::query_scalar("SELECT lifecycle_status FROM ks_well WHERE internal_well_id=$1")
            .bind(well)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(status, "inactive");
    assert_eq!(data_repair::repair_well_mappings(&pool).await.unwrap(), 0);
    data_repair::materialize_verified_leases(&pool, run)
        .await
        .unwrap();
    data_repair::materialize_verified_leases(&pool, run)
        .await
        .unwrap();
    let links:Vec<(String,String)>=sqlx::query_as("SELECT l.lease_kid,wl.product FROM ks_well_lease_link wl JOIN ks_lease l ON l.id=wl.lease_id WHERE wl.well_id=$1 ORDER BY wl.product")
        .bind(well).fetch_all(&pool).await.unwrap();
    assert_eq!(links, vec![(gas, "gas".into()), (oil, "oil".into())]);
    data_repair::backfill_well_history(&pool).await.unwrap();
    assert_eq!(data_repair::backfill_well_history(&pool).await.unwrap(), 0);
    let historical:Vec<(String,bool)>=sqlx::query_as("SELECT event_type,(payload->>'historical_backfill')::boolean FROM ks_event WHERE related_well_id=$1 ORDER BY event_type")
        .bind(well).fetch_all(&pool).await.unwrap();
    assert_eq!(
        historical,
        vec![
            ("COMPLETION_RECORDED".into(), true),
            ("SPUD_RECORDED".into(), true)
        ]
    );
    pool.close().await;
}
