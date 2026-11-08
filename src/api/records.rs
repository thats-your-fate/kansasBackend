//! Public verified documents and lease-level monthly production.
use super::{error::ApiError, state::AppState};
use axum::{
    extract::{Path, Query, State},
    response::Redirect,
    Json,
};
use chrono::NaiveDate;
use serde::Deserialize;
use serde_json::{json, Value};
use uuid::Uuid;

pub async fn documents_for_well(
    pool: &sqlx::PgPool,
    well_id: Uuid,
) -> Result<Vec<Value>, sqlx::Error> {
    sqlx::query_scalar(r#"SELECT jsonb_build_object('id',d.id,'title',d.document_type_raw,
      'document_type',d.document_type,'filing_date',d.filing_date,'effective_date',d.effective_date,
      'registered_at',d.first_observed_at,'stored_at',d.stored_at,'page_count',d.page_count,
      'source_url',d.source_url,'download_url','/api/ks/documents/'||d.id||'/download')
      FROM ks_well_document d JOIN ks_document_version v ON v.id=d.current_version_id AND v.document_id=d.id
      JOIN ks_ingestion_run r ON r.id=d.source_ingestion_run_id
      WHERE d.well_id=$1 AND d.public_access_state='public' AND d.content_object_storage_key IS NOT NULL
        AND r.status='completed' AND r.parser_version='kgs-public-pdf-v1'
      ORDER BY d.filing_date DESC NULLS LAST,d.first_observed_at DESC,d.id"#)
      .bind(well_id).fetch_all(pool).await
}

pub async fn download(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
) -> Result<(axum::http::HeaderMap, Redirect), ApiError> {
    let key: Option<String> = sqlx::query_scalar(r#"SELECT d.content_object_storage_key FROM ks_well_document d
      JOIN ks_well w ON w.internal_well_id=d.well_id JOIN ks_document_version v ON v.id=d.current_version_id
      JOIN ks_ingestion_run r ON r.id=d.source_ingestion_run_id
      WHERE d.id=$1 AND d.public_access_state='public' AND NOT w.is_tombstoned
        AND r.parser_version='kgs-public-pdf-v1' AND d.content_object_storage_key=v.object_storage_key"#)
      .bind(id).fetch_optional(&state.pool).await?.flatten();
    let key = key.ok_or_else(|| ApiError::NotFound("Public PDF not found".into()))?;
    if !key.starts_with("kansas/documents/sha256/") || key.contains("..") {
        return Err(ApiError::internal("invalid document key"));
    }
    let signed = tokio::task::spawn_blocking(move || -> Result<String, String> {
        let get =
            |name: &str| std::env::var(name).map_err(|_| "R2 configuration unavailable".to_owned());
        let bucket = get("R2_BUCKET_NAME")?;
        let endpoint = url::Url::parse(&get("R2_S3_ENPOINT")?)
            .map_err(|_| "invalid R2 configuration".to_owned())?;
        if endpoint.scheme() != "https"
            || !endpoint
                .host_str()
                .unwrap_or("")
                .ends_with(".r2.cloudflarestorage.com")
            || !endpoint.username().is_empty()
            || endpoint.password().is_some()
        {
            return Err("invalid R2 endpoint".into());
        }
        let result =
            std::process::Command::new(std::env::var("KS_AWS_BIN").unwrap_or("aws".into()))
                .env_clear()
                .env("PATH", "/usr/local/bin:/usr/bin:/bin")
                .env("AWS_ACCESS_KEY_ID", get("R2_ACCESS_KEY_ID")?)
                .env("AWS_SECRET_ACCESS_KEY", get("R2_SECRET_ACCESS_KEY")?)
                .env("AWS_CONFIG_FILE", "/dev/null")
                .env("AWS_SHARED_CREDENTIALS_FILE", "/dev/null")
                .env("AWS_EC2_METADATA_DISABLED", "true")
                .env("AWS_PAGER", "")
                .args([
                    "--endpoint-url",
                    &endpoint.origin().ascii_serialization(),
                    "--region",
                    "auto",
                    "s3",
                    "presign",
                    &format!("s3://{bucket}/{key}"),
                    "--expires-in",
                    "300",
                ])
                .output()
                .map_err(|_| "R2 signing unavailable".to_owned())?;
        if !result.status.success() || result.stdout.len() > 16384 {
            return Err("R2 signing failed".into());
        }
        let signed = String::from_utf8(result.stdout)
            .map_err(|_| "invalid R2 signing response".to_owned())?;
        let u = url::Url::parse(signed.trim()).map_err(|_| "invalid signed URL".to_owned())?;
        if u.origin() != endpoint.origin() || u.path() != format!("/{bucket}/{key}") {
            return Err("unexpected R2 destination".into());
        }
        Ok(signed.trim().to_owned())
    })
    .await
    .map_err(|_| ApiError::internal("document signer stopped"))?
    .map_err(ApiError::internal)?;
    let mut headers = axum::http::HeaderMap::new();
    headers.insert(
        axum::http::header::CACHE_CONTROL,
        axum::http::HeaderValue::from_static("private, no-store"),
    );
    Ok((headers, Redirect::temporary(&signed)))
}

#[derive(Deserialize)]
pub struct ProductionQuery {
    pub api: Option<String>,
    pub county: Option<String>,
    pub month: Option<NaiveDate>,
    pub page: Option<i64>,
}

// Resolve exact public well/lease relationships once, then use the existing
// lease index. A correlated EXISTS used to rescan links for every source row.
fn production_scope(has_api: bool) -> (String, String) {
    let predicate = r#"p.record_kind='monthly' AND make_date(p.year,p.month,1)>='2025-01-01'::date
      AND r.status='completed' AND r.parser_version='kgs_production_zip-v1'
      AND r.source_url LIKE 'https://www.kgs.ku.edu/PRS/Ora_Archive/%'
      AND ($2::text IS NULL OR lower(p.county)=lower(regexp_replace($2,' County$','','i')))
      AND ($3::date IS NULL OR make_date(p.year,p.month,1)=$3)"#;
    if has_api {
        let cte = "WITH linked AS MATERIALIZED (SELECT DISTINCT l.lease_id,l.product FROM ks_well_lease_link l JOIN ks_well w ON w.internal_well_id=l.well_id WHERE l.confidence=1 AND NOT w.is_tombstoned AND (w.api14=$1 OR w.api_raw=$1 OR w.kgs_kid=$1))";
        (cte.into(),format!("FROM linked JOIN ks_lease_production_month p ON p.lease_id=linked.lease_id AND p.product=linked.product JOIN ks_ingestion_run r ON r.id=p.source_ingestion_run_id WHERE {predicate}"))
    } else {
        (String::new(),format!("FROM ks_lease_production_month p JOIN ks_ingestion_run r ON r.id=p.source_ingestion_run_id WHERE $1::text IS NULL AND {predicate}"))
    }
}

pub async fn production(
    State(state): State<AppState>,
    Query(q): Query<ProductionQuery>,
) -> Result<Json<Value>, ApiError> {
    let page = q.page.unwrap_or(1);
    if page < 1 || page > 100000 || q.month.is_some_and(|d| chrono::Datelike::day(&d) != 1) {
        return Err(ApiError::bad_request("invalid production page/month"));
    }
    // Verified lease relationships are context, never divide a lease volume by
    // its well count or duplicate it into individual-well production records.
    let (cte, base) = production_scope(q.api.is_some());
    let total: i64 = sqlx::query_scalar(&format!("{cte} SELECT count(*) {base}"))
        .bind(&q.api)
        .bind(&q.county)
        .bind(q.month)
        .fetch_one(&state.pool)
        .await?;
    let items:Vec<Value>=sqlx::query_scalar(&format!(r#"{cte} SELECT jsonb_build_object('id',p.id,'lease_kid',p.lease_kid,
      'lease_name',p.reported_lease_name,'product',p.product,'month',make_date(p.year,p.month,1),
      'volume',p.production_volume,'unit',CASE p.product WHEN 'oil' THEN 'bbl' ELSE 'Mcf' END,
      'county',p.county,'operator',p.operator,'imported_at',r.completed_at,'source_url',r.source_url)
      {base} ORDER BY p.year DESC,p.month DESC,p.lease_kid,p.product LIMIT 50 OFFSET $4"#))
      .bind(&q.api).bind(&q.county).bind(q.month).bind((page-1)*50).fetch_all(&state.pool).await?;
    Ok(Json(
        json!({"data":{"items":items,"page":page,"total_items":total,"total_pages":(total+49)/50},
      "meta":{"reporting_level":"lease","quality_warnings":["Lease volumes are not individual-well production. The latest source month can be partial."]}}),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    #[ignore = "explicit Kansas pg_temp production query fixture"]
    async fn production_linked_scope_preserves_product_and_visibility() -> anyhow::Result<()> {
        anyhow::ensure!(
            std::env::var("KS_TEST_TEMP_DB").as_deref() == Ok("1"),
            "explicit fixture flag required"
        );
        let dsn = std::env::var("PG_CONNECTION")?;
        let url = url::Url::parse(&dsn)?;
        anyhow::ensure!(
            url.path() == "/future_wells_kansas",
            "Kansas target required"
        );
        let pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(1)
            .connect(&dsn)
            .await
            .map_err(|_| anyhow::anyhow!("Kansas fixture connection unavailable"))?;
        let mut db = pool.acquire().await?;
        sqlx::raw_sql("CREATE TEMP TABLE ks_well(internal_well_id int,api14 text,api_raw text,kgs_kid text,is_tombstoned bool); CREATE TEMP TABLE ks_well_lease_link(well_id int,lease_id int,product text,confidence numeric); CREATE TEMP TABLE ks_lease_production_month(lease_id int,product text,record_kind text,year int,month int,county text,source_ingestion_run_id int); CREATE TEMP TABLE ks_ingestion_run(id int,status text,parser_version text,source_url text); SET search_path TO pg_temp,public; INSERT INTO ks_well VALUES(1,'15000000010000','15-000-00001','100',false),(2,'15000000020000',NULL,'200',true); INSERT INTO ks_well_lease_link VALUES(1,10,'oil',1),(1,10,'oil',1),(1,20,'gas',0.5),(2,30,'gas',1); INSERT INTO ks_ingestion_run VALUES(1,'completed','kgs_production_zip-v1','https://www.kgs.ku.edu/PRS/Ora_Archive/test.zip'); INSERT INTO ks_lease_production_month VALUES(10,'oil','monthly',2026,9,'Test',1),(10,'gas','monthly',2026,9,'Test',1),(20,'gas','monthly',2026,9,'Test',1),(30,'gas','monthly',2026,9,'Test',1)").execute(&mut *db).await?;
        async fn count(db: &mut sqlx::PgConnection, api: Option<&str>) -> anyhow::Result<i64> {
            let (cte, base) = production_scope(api.is_some());
            Ok(sqlx::query_scalar(&format!("{cte} SELECT count(*) {base}"))
                .bind(api)
                .bind(None::<String>)
                .bind(None::<NaiveDate>)
                .fetch_one(db)
                .await?)
        }
        assert_eq!(count(&mut db, Some("15000000010000")).await?, 1);
        assert_eq!(count(&mut db, Some("100")).await?, 1);
        assert_eq!(count(&mut db, Some("15000000020000")).await?, 0);
        assert_eq!(count(&mut db, None).await?, 4);
        drop(db);
        pool.close().await;
        Ok(())
    }
}
