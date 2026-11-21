//! AI well summaries (superadmin only), following New Mexico and Oklahoma.
//!
//! A superadmin points this at one or more OpenAI-compatible inference servers on a disposable
//! GPU box (vLLM on Vast.ai), picks how many recently active wells to summarise, and watches
//! progress in `/api/admin/ai-summaries/app`. Every summary is stored as a `draft`; only
//! approved summaries are attached to public well JSON (`attach_public_summaries`).

use super::{error::ApiError, state::AppState};
use anyhow::Context;
use axum::{
    extract::{Path, Query, State},
    http::HeaderMap,
    response::Html,
    routing::{get, patch, post},
    Json, Router,
};
use futures_util::StreamExt;
use serde::Deserialize;
use serde_json::{json, Map, Value};
use sha2::{Digest, Sha256};
use sqlx::{PgPool, Row};
use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc, Mutex,
    },
    time::{Duration, Instant},
};
use uuid::Uuid;

pub const PROMPT_VERSION: &str = "ks-well-summary-v2";
const DEFAULT_MODEL: &str = "Qwen/Qwen3-30B-A3B-Instruct-2507";
const MAX_WELLS_PER_RUN: i64 = 20_000;
const MAX_CONCURRENCY: i64 = 256;
const MAX_ENDPOINTS: usize = 8;
const EVENTS_PER_WELL: i64 = 40;
const MAX_ATTEMPTS: u32 = 3;
const MAX_CONSECUTIVE_FAILURES: u32 = 25;
const WRITE_BATCH_MAX: usize = 64;
const REQUEST_TIMEOUT: Duration = Duration::from_secs(180);

const KS_LEASE_SOURCE_NOTE: &str =
    "Lease-level KGS production (shared across wells on the lease, not individual-well output).";

const SYSTEM_PROMPT: &str = "You write short, factual summaries of Kansas oil and gas wells for a public well-information website.\n\
Rules:\n\
- Use only the facts in the JSON you are given. Never guess or add outside knowledge.\n\
- Copy operator, well, county, field and formation names exactly as written in the JSON. You may change capitalization, nothing else: never shorten, correct, translate or replace a name.\n\
- Do not start with \"The well\". Lead with what matters most for this well: its latest filing, its drilling or completion, or its lease production.\n\
- Vary sentence structure from well to well like a human editor would; do not follow a fixed template. Avoid stock phrases such as \"is in the stage where\", \"has a completion filed\" and \"showed initial production of\".\n\
- Mention the well name once and the county. Do not give the API number, KGS KID, permit number, section, township or range.\n\
- Describe the dated activity in plain words, with dates written like \"September 14, 2026\". Never mention form numbers, status codes or internal record-keeping details.\n\
- The records come from the Kansas Corporation Commission (KCC) and the Kansas Geological Survey (KGS).\n\
- An \"Operator change\" event means the operator of record changed on that date; \"operator\" in \"well\" is the current operator.\n\
- Only call it an oil, gas or oil and gas well when \"well_type\" says so.\n\
- A drilling intent or permit does not mean the well was drilled.\n\
- Kansas production is reported per lease (KGS), which can include several wells. If \"lease_production\" is absent, do not mention lease production at all. When \"lease_production\" is present, say the reported volume is at the lease level (shared across wells on the lease, not this well's own output).\n\
- Units: oil and water are barrels; gas \"mcf\" means thousand cubic feet (write \"Mcf\" or \"thousand cubic feet\"). Never write million or billion.\n\
- Depths are in feet: \"total_depth_ft\" is the total depth on record.\n\
- Describe the well's condition only from \"progress\", the well's dates and \"lease_production\"; never write the word \"status\". Say that something has not happened only when \"progress\" says so.\n\
- \"progress\" says how far the well has got according to its filings. Use it so you never claim a step that has not happened, but do not restate it as its own sentence, and never use the words \"step\", \"stage\" or \"stands\".\n\
- Say \"an oil well\" or \"a gas well\" directly; never write \"classified as\".\n\
- Copy numbers exactly as given (you may add thousands separators). Never convert units, round, or add a number that is not in the JSON.\n\
- Leave out anything missing. Never say that details are missing or not available. Do not write \"unknown\", \"currently\", or filler about what has not happened or about a date range.\n\
- No investment advice, no opinions, no speculation about future drilling.\n\
- Write 2 to 4 sentences of plain prose. No headings, lists, or markdown.";

#[derive(Clone)]
pub struct AiState {
    pool: PgPool,
    ai_worker_pool: Option<PgPool>,
}

static ACTIVE_RUN: Mutex<Option<ActiveRun>> = Mutex::new(None);

struct ActiveRun {
    run_id: i64,
    cancel: Arc<AtomicBool>,
}

pub async fn ensure_schema(pool: &PgPool) -> anyhow::Result<()> {
    sqlx::raw_sql(include_str!("../../migrations/ai_summaries/ai_summaries.sql"))
        .execute(pool)
        .await
        .context("applying Kansas AI summary schema")?;
    Ok(())
}

/// A run cannot survive an API restart (its box token was only in memory).
pub async fn mark_interrupted_runs(pool: &PgPool) -> anyhow::Result<()> {
    sqlx::query(
        "UPDATE ks_ai_summary_runs SET status = 'interrupted', finished_at = now(), \
         last_error = COALESCE(last_error, 'API restarted during the run; resume it with the box token') \
         WHERE status IN ('running', 'cancelling')",
    )
    .execute(pool)
    .await
    .context("marking interrupted Kansas AI summary runs")?;
    Ok(())
}

pub fn routes(pool: PgPool, ai_worker_pool: Option<PgPool>) -> Router {
    let state = AiState {
        pool,
        ai_worker_pool,
    };
    Router::new()
        .route("/app/ai-summaries", get(page))
        .route("/api/admin/ai-summaries/app", get(page))
        .route("/api/admin/ai-summaries/candidates", get(candidates))
        .route("/api/admin/ai-summaries/test-endpoint", post(test_endpoint))
        .route(
            "/api/admin/ai-summaries/runs",
            get(list_runs).post(start_run),
        )
        .route("/api/admin/ai-summaries/runs/:id", get(get_run))
        .route("/api/admin/ai-summaries/runs/:id/cancel", post(cancel_run))
        .route("/api/admin/ai-summaries/runs/:id/resume", post(resume_run))
        .route("/api/admin/ai-summaries/runs/:id/log", get(run_log))
        .route(
            "/api/admin/ai-summaries/runs/:id/approve-drafts",
            post(approve_run_drafts),
        )
        .route("/api/admin/ai-summaries/summaries", get(list_summaries))
        .route(
            "/api/admin/ai-summaries/summaries/:id",
            patch(review_summary),
        )
        .with_state(state)
}

async fn page() -> Html<&'static str> {
    Html(include_str!("ai_summaries_page.html"))
}

fn worker_pool(state: &AiState) -> PgPool {
    state
        .ai_worker_pool
        .clone()
        .unwrap_or_else(|| state.pool.clone())
}

fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0_u8;
    for (x, y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    diff == 0
}

fn require_superadmin(headers: &HeaderMap) -> Result<String, ApiError> {
    if let Ok(expected) = std::env::var("KS_INTERNAL_ADMIN_SECRET") {
        let expected = expected.trim();
        if !expected.is_empty() {
            let provided = headers
                .get("x-ks-admin-secret")
                .and_then(|v| v.to_str().ok())
                .unwrap_or("");
            if !constant_time_eq(provided.as_bytes(), expected.as_bytes()) {
                return Err(ApiError::unauthorized("superadmin access required"));
            }
        }
    }
    let user = headers
        .get("x-ks-admin-user")
        .and_then(|v| v.to_str().ok())
        .map(str::trim)
        .filter(|s| !s.is_empty() && s.len() <= 254)
        .ok_or_else(|| ApiError::unauthorized("superadmin access required"))?;
    Ok(user.to_owned())
}

// ---------------------------------------------------------------------------------------
// Endpoints & HTTP client
// ---------------------------------------------------------------------------------------

#[derive(Clone, Debug)]
struct Endpoint {
    base: String,
}

fn parse_endpoints(raw: &str) -> Result<Vec<Endpoint>, ApiError> {
    let mut out = Vec::new();
    for line in raw
        .lines()
        .flat_map(|l| l.split(','))
        .map(str::trim)
        .filter(|s| !s.is_empty())
    {
        let parsed = reqwest::Url::parse(line)
            .map_err(|_| ApiError::bad_request(format!("invalid endpoint URL: {line}")))?;
        if !matches!(parsed.scheme(), "http" | "https") || parsed.host_str().is_none() {
            return Err(ApiError::bad_request(format!(
                "endpoint URL must be http or https: {line}"
            )));
        }
        if !parsed.username().is_empty() || parsed.password().is_some() {
            return Err(ApiError::bad_request(
                "endpoint URL must not contain credentials",
            ));
        }
        let mut base = parsed.as_str().trim_end_matches('/').to_owned();
        if let Some(stripped) = base.strip_suffix("/v1") {
            base = stripped.to_owned();
        }
        if !out.iter().any(|e: &Endpoint| e.base == base) {
            out.push(Endpoint { base });
        }
        if out.len() > MAX_ENDPOINTS {
            return Err(ApiError::bad_request(format!(
                "at most {MAX_ENDPOINTS} endpoints per run"
            )));
        }
    }
    if out.is_empty() {
        return Err(ApiError::bad_request("enter at least one endpoint URL"));
    }
    Ok(out)
}

fn endpoints_to_text(endpoints: &[Endpoint]) -> String {
    endpoints
        .iter()
        .map(|e| e.base.as_str())
        .collect::<Vec<_>>()
        .join("\n")
}

fn http_client() -> Result<reqwest::Client, ApiError> {
    reqwest::Client::builder()
        .connect_timeout(Duration::from_secs(10))
        .timeout(REQUEST_TIMEOUT)
        .pool_max_idle_per_host(MAX_CONCURRENCY as usize)
        .build()
        .map_err(|e| ApiError::internal(format!("building inference HTTP client: {e}")))
}

fn clean_token(token: Option<&str>) -> Option<String> {
    token
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_owned)
}

#[derive(Deserialize)]
struct TestEndpointRequest {
    endpoint_urls: String,
    token: Option<String>,
}

async fn test_endpoint(
    headers: HeaderMap,
    Json(body): Json<TestEndpointRequest>,
) -> Result<Json<Value>, ApiError> {
    require_superadmin(&headers)?;
    let endpoints = parse_endpoints(&body.endpoint_urls)?;
    let token = clean_token(body.token.as_deref());
    let client = http_client()?;

    let mut results = Vec::with_capacity(endpoints.len());
    for ep in &endpoints {
        let started = Instant::now();
        let mut req = client.get(format!("{}/v1/models", ep.base));
        if let Some(tok) = &token {
            req = req.bearer_auth(tok);
        }
        let resp = req
            .send()
            .await
            .map_err(|e| ApiError::bad_request(format!("{}: {}", ep.base, short_error(&e))))?;
        let status = resp.status();
        if !status.is_success() {
            let text = resp.text().await.unwrap_or_default();
            let detail: String = text.chars().take(200).collect();
            return Err(ApiError::bad_request(format!(
                "{} returned HTTP {}: {}",
                ep.base,
                status.as_u16(),
                detail
            )));
        }
        let parsed: Value = resp
            .json()
            .await
            .map_err(|e| ApiError::bad_request(format!("{}: invalid JSON: {e}", ep.base)))?;
        let models: Vec<String> = parsed["data"]
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(|m| m["id"].as_str().map(str::to_owned))
            .collect();
        results.push(json!({
            "url": ep.base,
            "latency_ms": started.elapsed().as_millis() as i64,
            "models": models,
        }));
    }

    Ok(Json(json!({ "endpoints": results })))
}

fn short_error(error: &reqwest::Error) -> String {
    if error.is_timeout() {
        "request timed out".to_owned()
    } else if error.is_connect() {
        "could not connect".to_owned()
    } else {
        let msg = error.to_string();
        msg.chars().take(180).collect()
    }
}

// ---------------------------------------------------------------------------------------
// Candidate selection & counts
// ---------------------------------------------------------------------------------------

#[derive(Deserialize)]
struct CandidatesQuery {
    since: Option<chrono::NaiveDate>,
    model: Option<String>,
    force: Option<bool>,
}

fn default_since() -> chrono::NaiveDate {
    chrono::NaiveDate::from_ymd_opt(2025, 1, 1).expect("valid date")
}

/// Dated Kansas activity per well since `$1`, combining `ks_event` (excluding lease-production
/// events), public `ks_well_document` filings, and well inventory milestone dates.
const CANDIDATE_ACTIVITY_SQL: &str = r#"
    WITH activity AS (
        SELECT e.related_well_id AS well_id, max(COALESCE(e.occurred_at, e.published_at)::date) AS latest_date
        FROM ks_event e
        WHERE e.related_well_id IS NOT NULL
          AND e.event_type NOT IN ('FIRST_LEASE_PRODUCTION_REPORTED', 'LEASE_PRODUCTION_UPDATED')
          AND COALESCE(e.occurred_at, e.published_at)::date >= $1
          AND COALESCE(e.occurred_at, e.published_at)::date <= CURRENT_DATE
        GROUP BY e.related_well_id
        UNION ALL
        SELECT d.well_id, max(d.filing_date) AS latest_date
        FROM ks_well_document d
        WHERE d.public_access_state = 'public'
          AND d.current_version_id IS NOT NULL
          AND d.filing_date BETWEEN $1 AND CURRENT_DATE
        GROUP BY d.well_id
        UNION ALL
        SELECT w.internal_well_id AS well_id,
               GREATEST(
                   CASE WHEN w.permit_date BETWEEN $1 AND CURRENT_DATE THEN w.permit_date END,
                   CASE WHEN w.spud_date BETWEEN $1 AND CURRENT_DATE THEN w.spud_date END,
                   CASE WHEN w.completion_date BETWEEN $1 AND CURRENT_DATE THEN w.completion_date END,
                   CASE WHEN w.plug_date BETWEEN $1 AND CURRENT_DATE THEN w.plug_date END
               ) AS latest_date
        FROM ks_well w
        WHERE (w.permit_date BETWEEN $1 AND CURRENT_DATE
            OR w.spud_date BETWEEN $1 AND CURRENT_DATE
            OR w.completion_date BETWEEN $1 AND CURRENT_DATE
            OR w.plug_date BETWEEN $1 AND CURRENT_DATE)
    ),
    recent_wells AS (
        SELECT well_id, max(latest_date) AS latest_date
        FROM activity
        WHERE latest_date IS NOT NULL
        GROUP BY well_id
    )
"#;

const IDENTITY_SQL: &str = "NOT w.is_tombstoned \
    AND NULLIF(btrim(w.well_name), '') IS NOT NULL \
    AND NULLIF(btrim(w.county), '') IS NOT NULL \
    AND COALESCE(NULLIF(btrim(w.api14), ''), NULLIF(btrim(w.api_raw), ''), NULLIF(btrim(w.kgs_kid), '')) IS NOT NULL";

const RECENT_SPUD_SQL: &str = "(w.spud_date IS NULL OR w.spud_date >= $1)";

async fn candidates(
    State(state): State<AiState>,
    headers: HeaderMap,
    Query(query): Query<CandidatesQuery>,
) -> Result<Json<Value>, ApiError> {
    require_superadmin(&headers)?;
    let since = query.since.unwrap_or_else(default_since);
    let model = query
        .model
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .unwrap_or(DEFAULT_MODEL);
    let force = query.force.unwrap_or(false);

    let row = sqlx::query(&format!(
        r#"
        {CANDIDATE_ACTIVITY_SQL}
        SELECT
            count(*)::bigint AS eligible_wells,
            count(*) FILTER (WHERE $4 OR NOT EXISTS (
                SELECT 1 FROM ks_well_ai_summaries s
                WHERE s.well_id = w.internal_well_id
                  AND s.model = $2
                  AND s.prompt_version = $3
            ))::bigint AS pending_wells
        FROM recent_wells rw
        JOIN ks_well w ON w.internal_well_id = rw.well_id
        WHERE {IDENTITY_SQL} AND {RECENT_SPUD_SQL}
        "#,
    ))
    .bind(since)
    .bind(model)
    .bind(PROMPT_VERSION)
    .bind(force)
    .fetch_one(&state.pool)
    .await?;

    Ok(Json(json!({
        "activity_since": since,
        "model": model,
        "prompt_version": PROMPT_VERSION,
        "eligible_wells": row.get::<i64, _>("eligible_wells"),
        "pending_wells": row.get::<i64, _>("pending_wells"),
    })))
}

// ---------------------------------------------------------------------------------------
// Runs: list, detail, start, cancel, resume, log
// ---------------------------------------------------------------------------------------

const RUN_JSON_SQL: &str = r#"
    SELECT jsonb_build_object(
        'id', id, 'status', status, 'endpoint_url', endpoint_url, 'model', model,
        'prompt_version', prompt_version, 'activity_since', activity_since,
        'well_limit', well_limit, 'concurrency', concurrency, 'force_regenerate', force_regenerate,
        'selected_count', selected_count, 'succeeded_count', succeeded_count,
        'failed_count', failed_count, 'skipped_count', skipped_count,
        'prompt_tokens', prompt_tokens, 'completion_tokens', completion_tokens,
        'created_by', created_by, 'created_at', created_at, 'started_at', started_at,
        'finished_at', finished_at, 'last_error', last_error,
        'elapsed_seconds', CASE WHEN started_at IS NULL THEN 0
            ELSE EXTRACT(EPOCH FROM COALESCE(finished_at, now()) - started_at)::bigint END
    )
    FROM ks_ai_summary_runs
"#;

async fn list_runs(
    State(state): State<AiState>,
    headers: HeaderMap,
) -> Result<Json<Value>, ApiError> {
    require_superadmin(&headers)?;
    let runs: Vec<Value> = sqlx::query_scalar(&format!("{RUN_JSON_SQL} ORDER BY id DESC LIMIT 20"))
        .fetch_all(&state.pool)
        .await?;
    Ok(Json(json!({ "runs": runs })))
}

async fn get_run(
    State(state): State<AiState>,
    headers: HeaderMap,
    Path(id): Path<i64>,
) -> Result<Json<Value>, ApiError> {
    require_superadmin(&headers)?;
    load_run_json(&state.pool, id).await.map(Json)
}

async fn load_run_json(pool: &PgPool, id: i64) -> Result<Value, ApiError> {
    let run: Option<Value> = sqlx::query_scalar(&format!("{RUN_JSON_SQL} WHERE id = $1"))
        .bind(id)
        .fetch_optional(pool)
        .await?;
    run.ok_or_else(|| ApiError::NotFound("AI summary run not found".to_owned()))
}

fn active_run_id() -> Option<i64> {
    ACTIVE_RUN
        .lock()
        .expect("active run mutex")
        .as_ref()
        .map(|r| r.run_id)
}

#[derive(Deserialize)]
struct StartRunRequest {
    endpoint_urls: String,
    token: Option<String>,
    model: Option<String>,
    limit: Option<i64>,
    concurrency: Option<i64>,
    activity_since: Option<chrono::NaiveDate>,
    force: Option<bool>,
}

struct ActiveSlot {
    run_id: i64,
    cancel: Arc<AtomicBool>,
}

impl ActiveSlot {
    fn acquire(run_id: i64) -> Result<Self, ApiError> {
        let mut guard = ACTIVE_RUN.lock().expect("active run mutex");
        if let Some(active) = guard.as_ref() {
            return Err(ApiError::bad_request(format!(
                "run #{} is already active; wait for it or cancel it first",
                active.run_id
            )));
        }
        let cancel = Arc::new(AtomicBool::new(false));
        *guard = Some(ActiveRun {
            run_id,
            cancel: cancel.clone(),
        });
        Ok(Self { run_id, cancel })
    }

    fn try_reserve() -> Result<(), ApiError> {
        let guard = ACTIVE_RUN.lock().expect("active run mutex");
        if let Some(active) = guard.as_ref() {
            return Err(ApiError::bad_request(format!(
                "run #{} is already active; wait for it or cancel it first",
                active.run_id
            )));
        }
        Ok(())
    }

    fn bind_run_id(&mut self, run_id: i64) {
        self.run_id = run_id;
        let mut guard = ACTIVE_RUN.lock().expect("active run mutex");
        if let Some(active) = guard.as_mut() {
            active.run_id = run_id;
        }
    }
}

impl Drop for ActiveSlot {
    fn drop(&mut self) {
        let mut guard = ACTIVE_RUN.lock().expect("active run mutex");
        if guard.as_ref().is_some_and(|r| r.run_id == self.run_id) {
            *guard = None;
        }
    }
}

fn release_active_run() {
    let mut guard = ACTIVE_RUN.lock().expect("active run mutex");
    *guard = None;
}

async fn start_run(
    State(state): State<AiState>,
    headers: HeaderMap,
    Json(body): Json<StartRunRequest>,
) -> Result<Json<Value>, ApiError> {
    let admin_email = require_superadmin(&headers)?;
    ActiveSlot::try_reserve()?;

    let endpoints = parse_endpoints(&body.endpoint_urls)?;
    let token = clean_token(body.token.as_deref());
    let model = body
        .model
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .unwrap_or(DEFAULT_MODEL)
        .to_owned();
    let well_limit = body.limit.unwrap_or(2000).clamp(1, MAX_WELLS_PER_RUN);
    let concurrency = body.concurrency.unwrap_or(64).clamp(1, MAX_CONCURRENCY);
    let since = body.activity_since.unwrap_or_else(default_since);
    let force = body.force.unwrap_or(false);
    let client = http_client()?;

    let mut slot = ActiveSlot::acquire(-1)?;
    let endpoint_text = endpoints_to_text(&endpoints);

    let run_id: i64 = sqlx::query_scalar(
        "INSERT INTO ks_ai_summary_runs \
         (status, endpoint_url, model, prompt_version, activity_since, well_limit, concurrency, \
          force_regenerate, created_by, started_at) \
         VALUES ('running', $1, $2, $3, $4, $5, $6, $7, $8, now()) \
         RETURNING id",
    )
    .bind(&endpoint_text)
    .bind(&model)
    .bind(PROMPT_VERSION)
    .bind(since)
    .bind(well_limit)
    .bind(concurrency)
    .bind(force)
    .bind(&admin_email)
    .fetch_one(&state.pool)
    .await?;

    slot.bind_run_id(run_id);

    let selected: i64 = sqlx::query_scalar(&format!(
        r#"
        {CANDIDATE_ACTIVITY_SQL},
        picked AS (
            SELECT w.internal_well_id AS well_id,
                   COALESCE(NULLIF(btrim(w.api14), ''), NULLIF(btrim(w.api_raw), ''), NULLIF(btrim(w.kgs_kid), '')) AS lookup_api,
                   rw.latest_date
            FROM recent_wells rw
            JOIN ks_well w ON w.internal_well_id = rw.well_id
            WHERE {IDENTITY_SQL} AND {RECENT_SPUD_SQL}
              AND ($4 OR NOT EXISTS (
                  SELECT 1 FROM ks_well_ai_summaries s
                  WHERE s.well_id = w.internal_well_id
                    AND s.model = $2
                    AND s.prompt_version = $3
              ))
            ORDER BY rw.latest_date DESC, w.internal_well_id
            LIMIT $5
        ),
        inserted AS (
            INSERT INTO ks_ai_summary_run_items (run_id, well_id, lookup_api, latest_activity_date)
            SELECT $6, well_id, lookup_api, latest_date FROM picked
            RETURNING well_id
        )
        SELECT count(*)::bigint FROM inserted
        "#,
    ))
    .bind(since)
    .bind(&model)
    .bind(PROMPT_VERSION)
    .bind(force)
    .bind(well_limit)
    .bind(run_id)
    .fetch_one(&state.pool)
    .await?;

    sqlx::query("UPDATE ks_ai_summary_runs SET selected_count = $2 WHERE id = $1")
        .bind(run_id)
        .bind(selected as i32)
        .execute(&state.pool)
        .await?;

    if selected == 0 {
        sqlx::query("UPDATE ks_ai_summary_runs SET status = 'completed', finished_at = now() WHERE id = $1")
            .bind(run_id)
            .execute(&state.pool)
            .await?;
        log(
            &state.pool,
            run_id,
            "info",
            None,
            "No eligible wells matched (all recent active wells already have a summary for this model and prompt version).",
        )
        .await;
        drop(slot);
        return load_run_json(&state.pool, run_id).await.map(Json);
    }

    log(
        &state.pool,
        run_id,
        "info",
        None,
        &format!(
            "Queued {selected} wells since {since} across {} endpoint(s) (model {model}, concurrency {concurrency}).",
            endpoints.len()
        ),
    )
    .await;

    let pool_for_task = worker_pool(&state);
    let cancel = slot.cancel.clone();
    std::mem::forget(slot);
    tokio::spawn(async move {
        let result = run_worker(
            pool_for_task.clone(),
            client,
            run_id,
            endpoints,
            token,
            model,
            since,
            concurrency as usize,
            force,
            cancel,
        )
        .await;
        if let Err(err) = result {
            finish_run_with_error(&pool_for_task, run_id, &err).await;
        }
        release_active_run();
    });

    load_run_json(&state.pool, run_id).await.map(Json)
}

async fn finish_run_with_error(pool: &PgPool, run_id: i64, message: &str) {
    let msg: String = message.chars().take(500).collect();
    let _ = sqlx::query(
        "UPDATE ks_ai_summary_runs SET status = 'failed', finished_at = now(), last_error = $2 WHERE id = $1",
    )
    .bind(run_id)
    .bind(&msg)
    .execute(pool)
    .await;
    log(pool, run_id, "error", None, &msg).await;
}

async fn cancel_run(
    State(state): State<AiState>,
    headers: HeaderMap,
    Path(id): Path<i64>,
) -> Result<Json<Value>, ApiError> {
    require_superadmin(&headers)?;
    let active = {
        let guard = ACTIVE_RUN.lock().expect("active run mutex");
        guard
            .as_ref()
            .filter(|r| r.run_id == id)
            .map(|r| r.cancel.clone())
    };
    if let Some(cancel) = active {
        cancel.store(true, Ordering::SeqCst);
        sqlx::query(
            "UPDATE ks_ai_summary_runs SET status = 'cancelling' WHERE id = $1 AND status = 'running'",
        )
        .bind(id)
        .execute(&state.pool)
        .await?;
        log(&state.pool, id, "warn", None, "Cancellation requested by admin.").await;
    } else {
        sqlx::query(
            "UPDATE ks_ai_summary_runs SET status = 'cancelled', finished_at = COALESCE(finished_at, now()) \
             WHERE id = $1 AND status IN ('running', 'cancelling')",
        )
        .bind(id)
        .execute(&state.pool)
        .await?;
    }
    load_run_json(&state.pool, id).await.map(Json)
}

#[derive(Deserialize)]
struct ResumeRunRequest {
    endpoint_urls: Option<String>,
    token: Option<String>,
    concurrency: Option<i64>,
}

async fn resume_run(
    State(state): State<AiState>,
    headers: HeaderMap,
    Path(id): Path<i64>,
    Json(body): Json<ResumeRunRequest>,
) -> Result<Json<Value>, ApiError> {
    require_superadmin(&headers)?;
    if active_run_id().is_some() {
        return Err(ApiError::bad_request("another run is already active"));
    }

    let row = sqlx::query(
        "SELECT status, endpoint_url, model, activity_since, concurrency, force_regenerate \
         FROM ks_ai_summary_runs WHERE id = $1",
    )
    .bind(id)
    .fetch_optional(&state.pool)
    .await?
    .ok_or_else(|| ApiError::NotFound("AI summary run not found".to_owned()))?;

    let status: String = row.get("status");
    if !matches!(status.as_str(), "interrupted" | "failed" | "cancelled") {
        return Err(ApiError::bad_request(format!(
            "cannot resume a run in status {status}"
        )));
    }

    let endpoint_raw = body
        .endpoint_urls
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_owned)
        .unwrap_or_else(|| row.get::<String, _>("endpoint_url"));
    let endpoints = parse_endpoints(&endpoint_raw)?;
    let token = clean_token(body.token.as_deref());
    let model: String = row.get("model");
    let since: chrono::NaiveDate = row.get("activity_since");
    let concurrency = body
        .concurrency
        .unwrap_or_else(|| i64::from(row.get::<i32, _>("concurrency")))
        .clamp(1, MAX_CONCURRENCY);
    let force: bool = row.get("force_regenerate");
    let client = http_client()?;

    let slot = ActiveSlot::acquire(id)?;

    sqlx::query(
        "UPDATE ks_ai_summary_run_items SET status = 'pending', error = NULL \
         WHERE run_id = $1 AND status IN ('failed', 'running')",
    )
    .bind(id)
    .execute(&state.pool)
    .await?;

    sqlx::query(
        "UPDATE ks_ai_summary_runs SET status = 'running', endpoint_url = $2, concurrency = $3, \
         failed_count = 0, finished_at = NULL, last_error = NULL \
         WHERE id = $1",
    )
    .bind(id)
    .bind(endpoints_to_text(&endpoints))
    .bind(concurrency as i32)
    .execute(&state.pool)
    .await?;

    log(
        &state.pool,
        id,
        "info",
        None,
        &format!(
            "Resumed across {} endpoint(s) (concurrency {concurrency}).",
            endpoints.len()
        ),
    )
    .await;

    let pool_for_task = worker_pool(&state);
    let cancel = slot.cancel.clone();
    std::mem::forget(slot);
    tokio::spawn(async move {
        let result = run_worker(
            pool_for_task.clone(),
            client,
            id,
            endpoints,
            token,
            model,
            since,
            concurrency as usize,
            force,
            cancel,
        )
        .await;
        if let Err(err) = result {
            finish_run_with_error(&pool_for_task, id, &err).await;
        }
        release_active_run();
    });

    load_run_json(&state.pool, id).await.map(Json)
}

#[derive(Deserialize)]
struct LogQuery {
    after_id: Option<i64>,
    limit: Option<i64>,
}

async fn run_log(
    State(state): State<AiState>,
    headers: HeaderMap,
    Path(id): Path<i64>,
    Query(query): Query<LogQuery>,
) -> Result<Json<Value>, ApiError> {
    require_superadmin(&headers)?;
    let after_id = query.after_id.unwrap_or(0);
    let limit = query.limit.unwrap_or(200).clamp(1, 500);
    let entries: Vec<Value> = sqlx::query_scalar(
        "SELECT jsonb_build_object( \
             'id', id, 'logged_at', logged_at, 'level', level, 'lookup_api', lookup_api, 'message', message \
         ) \
         FROM ks_ai_summary_log \
         WHERE run_id = $1 AND id > $2 \
         ORDER BY id ASC LIMIT $3",
    )
    .bind(id)
    .bind(after_id)
    .bind(limit)
    .fetch_all(&state.pool)
    .await?;
    Ok(Json(json!({ "entries": entries })))
}

// ---------------------------------------------------------------------------------------
// Summaries: list, review, bulk-approve
// ---------------------------------------------------------------------------------------

#[derive(Deserialize)]
struct SummariesQuery {
    status: Option<String>,
    run_id: Option<i64>,
    limit: Option<i64>,
    offset: Option<i64>,
}

async fn list_summaries(
    State(state): State<AiState>,
    headers: HeaderMap,
    Query(query): Query<SummariesQuery>,
) -> Result<Json<Value>, ApiError> {
    require_superadmin(&headers)?;
    let limit = query.limit.unwrap_or(25).clamp(1, 100);
    let offset = query.offset.unwrap_or(0).max(0);
    let status = query
        .status
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(normalize_review_status)
        .transpose()?;

    let total: i64 = sqlx::query_scalar(
        "SELECT count(*)::bigint FROM ks_well_ai_summaries \
         WHERE ($1::text IS NULL OR review_status = $1) \
           AND ($2::bigint IS NULL OR run_id = $2)",
    )
    .bind(status)
    .bind(query.run_id)
    .fetch_one(&state.pool)
    .await?;

    let summaries: Vec<Value> = sqlx::query_scalar(
        "SELECT jsonb_build_object( \
             'id', id, 'well_id', well_id, 'lookup_api', lookup_api, 'run_id', run_id, \
             'model', model, 'prompt_version', prompt_version, 'summary', summary, \
             'input_json', input_json, 'finish_reason', finish_reason, \
             'prompt_tokens', prompt_tokens, 'completion_tokens', completion_tokens, \
             'latency_ms', latency_ms, 'review_status', review_status, \
             'reviewed_by', reviewed_by, 'reviewed_at', reviewed_at, 'created_at', created_at \
         ) \
         FROM ks_well_ai_summaries \
         WHERE ($1::text IS NULL OR review_status = $1) \
           AND ($2::bigint IS NULL OR run_id = $2) \
         ORDER BY id DESC LIMIT $3 OFFSET $4",
    )
    .bind(status)
    .bind(query.run_id)
    .bind(limit)
    .bind(offset)
    .fetch_all(&state.pool)
    .await?;

    Ok(Json(json!({ "total": total, "summaries": summaries })))
}

fn normalize_review_status(value: &str) -> Result<&'static str, ApiError> {
    match value {
        "draft" => Ok("draft"),
        "approved" => Ok("approved"),
        "rejected" => Ok("rejected"),
        _ => Err(ApiError::bad_request(
            "review_status must be draft, approved, or rejected",
        )),
    }
}

#[derive(Deserialize)]
struct ReviewRequest {
    review_status: String,
}

async fn review_summary(
    State(state): State<AiState>,
    headers: HeaderMap,
    Path(id): Path<i64>,
    Json(body): Json<ReviewRequest>,
) -> Result<Json<Value>, ApiError> {
    let admin_email = require_superadmin(&headers)?;
    let status = normalize_review_status(body.review_status.trim())?;
    let row: Option<Value> = sqlx::query_scalar(
        "UPDATE ks_well_ai_summaries \
         SET review_status = $2, reviewed_by = $3, reviewed_at = now() \
         WHERE id = $1 \
         RETURNING jsonb_build_object('id', id, 'review_status', review_status, 'reviewed_by', reviewed_by, 'reviewed_at', reviewed_at)",
    )
    .bind(id)
    .bind(status)
    .bind(&admin_email)
    .fetch_optional(&state.pool)
    .await?;
    row.map(Json)
        .ok_or_else(|| ApiError::NotFound("AI summary not found".to_owned()))
}

async fn approve_run_drafts(
    State(state): State<AiState>,
    headers: HeaderMap,
    Path(id): Path<i64>,
) -> Result<Json<Value>, ApiError> {
    let admin_email = require_superadmin(&headers)?;
    let run_since: Option<chrono::NaiveDate> = sqlx::query_scalar(
        "SELECT activity_since FROM ks_ai_summary_runs WHERE id = $1",
    )
    .bind(id)
    .fetch_optional(&state.pool)
    .await?
    .ok_or_else(|| ApiError::NotFound("AI summary run not found".to_owned()))?;
    let since = run_since.unwrap_or_else(default_since);

    let approved: i64 = sqlx::query_scalar(
        "WITH updated AS ( \
             UPDATE ks_well_ai_summaries s \
             SET review_status = 'approved', reviewed_by = $2, reviewed_at = now() \
             FROM ks_well w \
             WHERE s.run_id = $1 \
               AND s.review_status = 'draft' \
               AND w.internal_well_id = s.well_id \
               AND NOT w.is_tombstoned \
               AND (w.spud_date IS NULL OR w.spud_date >= $3) \
             RETURNING s.id \
         ) \
         SELECT count(*)::bigint FROM updated",
    )
    .bind(id)
    .bind(&admin_email)
    .bind(since)
    .fetch_one(&state.pool)
    .await?;

    log(
        &state.pool,
        id,
        "info",
        None,
        &format!("Approved {approved} draft summary(ies) for public display."),
    )
    .await;

    Ok(Json(json!({ "run_id": id, "approved": approved })))
}

// ---------------------------------------------------------------------------------------
// Public summary attach middleware
// ---------------------------------------------------------------------------------------

fn public_summary_path(path: &str) -> bool {
    if let Some(rest) = path.strip_prefix("/api/ks/wells/") {
        return !rest.is_empty() && !rest.contains('/') && rest != "map";
    }
    path == "/api/ks/browse"
        || path.starts_with("/api/ks/counties/")
        || path.starts_with("/api/ks/operators/")
        || path.starts_with("/api/ks/fields/")
}

pub async fn attach_public_summaries(
    State(state): State<AppState>,
    request: axum::extract::Request,
    next: axum::middleware::Next,
) -> axum::response::Response {
    let applies =
        request.method() == axum::http::Method::GET && public_summary_path(request.uri().path());
    let response = next.run(request).await;
    if !applies || !response.status().is_success() {
        return response;
    }
    let is_json = response
        .headers()
        .get(axum::http::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|v| v.starts_with("application/json"));
    if !is_json {
        return response;
    }
    let (mut parts, body) = response.into_parts();
    let bytes = match axum::body::to_bytes(body, 32 * 1024 * 1024).await {
        Ok(bytes) => bytes,
        Err(error) => {
            tracing::warn!(%error, "Kansas AI summary middleware could not read response body");
            return axum::response::IntoResponse::into_response(
                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            );
        }
    };
    let unchanged = |parts: axum::http::response::Parts, bytes: axum::body::Bytes| {
        axum::response::Response::from_parts(parts, axum::body::Body::from(bytes))
    };
    let Ok(mut payload) = serde_json::from_slice::<Value>(&bytes) else {
        return unchanged(parts, bytes);
    };
    let mut keys = Vec::new();
    collect_well_keys(&payload, &mut keys);
    if keys.is_empty() {
        return unchanged(parts, bytes);
    }
    let rows = sqlx::query(
        r#"
        SELECT DISTINCT ON (s.well_id)
            s.well_id::text AS well_id_text,
            w.api14,
            w.api_raw,
            w.kgs_kid,
            s.lookup_api,
            s.summary
        FROM ks_well_ai_summaries s
        JOIN ks_well w ON w.internal_well_id = s.well_id
        WHERE s.review_status = 'approved'
          AND NOT w.is_tombstoned
          AND (
              w.internal_well_id::text = ANY($1)
              OR w.api14 = ANY($1)
              OR w.api_raw = ANY($1)
              OR w.kgs_kid = ANY($1)
              OR s.lookup_api = ANY($1)
          )
        ORDER BY s.well_id, s.reviewed_at DESC NULLS LAST, s.id DESC
        "#,
    )
    .bind(&keys)
    .fetch_all(&state.pool)
    .await;

    let mut summaries = HashMap::new();
    match rows {
        Ok(rows) => {
            for r in rows {
                let summary: String = match r.try_get("summary") {
                    Ok(s) => s,
                    Err(_) => continue,
                };
                for col in ["well_id_text", "api14", "api_raw", "kgs_kid", "lookup_api"] {
                    if let Ok(Some(k)) = r.try_get::<Option<String>, _>(col) {
                        if !k.is_empty() {
                            summaries.entry(k).or_insert_with(|| summary.clone());
                        }
                    }
                }
            }
        }
        Err(error) => {
            tracing::warn!(%error, "Kansas AI summary lookup failed; serving page without summaries");
            return unchanged(parts, bytes);
        }
    }
    if summaries.is_empty() {
        return unchanged(parts, bytes);
    }
    insert_summaries(&mut payload, &summaries);
    let Ok(new_bytes) = serde_json::to_vec(&payload) else {
        return unchanged(parts, bytes);
    };
    parts.headers.remove(axum::http::header::CONTENT_LENGTH);
    axum::response::Response::from_parts(parts, axum::body::Body::from(new_bytes))
}

fn is_well_or_activity_record(obj: &Map<String, Value>) -> bool {
    let has_id_or_api = obj.get("api14").and_then(Value::as_str).is_some()
        || obj.get("api_raw").and_then(Value::as_str).is_some()
        || obj.get("kgs_kid").and_then(Value::as_str).is_some()
        || obj.get("id").and_then(Value::as_str).is_some();
    has_id_or_api
        && (obj.contains_key("well_name")
            || obj.contains_key("lifecycle_status")
            || obj.contains_key("event_type"))
}

fn record_keys(obj: &Map<String, Value>) -> Vec<&str> {
    let mut out = Vec::new();
    for field in ["id", "api14", "api_raw", "kgs_kid"] {
        if let Some(val) = obj.get(field).and_then(Value::as_str) {
            if !val.is_empty() {
                out.push(val);
            }
        }
    }
    out
}

fn collect_well_keys(value: &Value, out: &mut Vec<String>) {
    match value {
        Value::Object(obj) => {
            if is_well_or_activity_record(obj) && out.len() < 2000 {
                for key in record_keys(obj) {
                    if !out.iter().any(|existing| existing == key) {
                        out.push(key.to_owned());
                    }
                }
            }
            for v in obj.values() {
                collect_well_keys(v, out);
            }
        }
        Value::Array(items) => items.iter().for_each(|v| collect_well_keys(v, out)),
        _ => {}
    }
}

fn insert_summaries(value: &mut Value, summaries: &HashMap<String, String>) {
    match value {
        Value::Object(obj) => {
            if is_well_or_activity_record(obj) {
                let found = record_keys(obj)
                    .into_iter()
                    .find_map(|k| summaries.get(k))
                    .cloned();
                if let Some(summary) = found {
                    obj.insert("ai_summary".to_owned(), Value::String(summary));
                }
            }
            for v in obj.values_mut() {
                insert_summaries(v, summaries);
            }
        }
        Value::Array(items) => items
            .iter_mut()
            .for_each(|v| insert_summaries(v, summaries)),
        _ => {}
    }
}

// ---------------------------------------------------------------------------------------
// Logging
// ---------------------------------------------------------------------------------------

async fn log(pool: &PgPool, run_id: i64, level: &str, api: Option<&str>, message: &str) {
    let message: String = message.chars().take(2000).collect();
    if let Err(error) = sqlx::query(
        "INSERT INTO ks_ai_summary_log(run_id, level, lookup_api, message) VALUES ($1, $2, $3, $4)",
    )
    .bind(run_id)
    .bind(level)
    .bind(api)
    .bind(&message)
    .execute(pool)
    .await
    {
        tracing::warn!(%error, run_id, "failed to write Kansas AI summary log");
    }
}

// ---------------------------------------------------------------------------------------
// Input builder from public Kansas well facts
// ---------------------------------------------------------------------------------------

async fn build_input(
    pool: &PgPool,
    well_id: Uuid,
    since: chrono::NaiveDate,
) -> Result<Option<Value>, ApiError> {
    let well_row = sqlx::query(
        r#"
        SELECT
            internal_well_id,
            well_name,
            COALESCE(NULLIF(btrim(operator_name_kgs_reported_current), ''), NULLIF(btrim(operator_name_kgs_original), '')) AS operator,
            county,
            field_name,
            lease_name,
            lifecycle_status,
            well_class_raw,
            status_raw,
            permit_date,
            spud_date,
            completion_date,
            plug_date,
            total_depth,
            producing_formation,
            formation_at_total_depth,
            horizontal_or_directional,
            ip_oil,
            ip_gas,
            ip_water
        FROM ks_well
        WHERE internal_well_id = $1
          AND NOT is_tombstoned
        "#,
    )
    .bind(well_id)
    .fetch_optional(pool)
    .await?;

    let Some(w) = well_row else {
        return Ok(None);
    };

    let well_name: Option<String> = w.get("well_name");
    let county: Option<String> = w.get("county");
    if well_name.as_deref().map(str::trim).unwrap_or("").is_empty()
        || county.as_deref().map(str::trim).unwrap_or("").is_empty()
    {
        return Ok(None);
    }

    let mut events = Vec::<(chrono::NaiveDate, String)>::new();

    let ks_ev_rows = sqlx::query(
        r#"
        SELECT event_type, COALESCE(occurred_at, published_at)::date AS d
        FROM ks_event
        WHERE related_well_id = $1
          AND event_type NOT IN ('FIRST_LEASE_PRODUCTION_REPORTED', 'LEASE_PRODUCTION_UPDATED')
          AND COALESCE(occurred_at, published_at)::date >= $2
          AND COALESCE(occurred_at, published_at)::date <= CURRENT_DATE
        ORDER BY d DESC
        LIMIT 40
        "#,
    )
    .bind(well_id)
    .bind(since)
    .fetch_all(pool)
    .await?;
    for r in ks_ev_rows {
        if let Some(d) = r.get::<Option<chrono::NaiveDate>, _>("d") {
            events.push((d, r.get("event_type")));
        }
    }

    let doc_rows = sqlx::query(
        r#"
        SELECT document_type, filing_date
        FROM ks_well_document
        WHERE well_id = $1
          AND public_access_state = 'public'
          AND current_version_id IS NOT NULL
          AND filing_date BETWEEN $2 AND CURRENT_DATE
        ORDER BY filing_date DESC
        LIMIT 40
        "#,
    )
    .bind(well_id)
    .bind(since)
    .fetch_all(pool)
    .await?;
    for r in doc_rows {
        let d: chrono::NaiveDate = r.get("filing_date");
        let dtype: String = r.get::<Option<String>, _>("document_type").unwrap_or_default();
        let code = match dtype.as_str() {
            "completion" | "aco1" => "COMPLETION_RECORDED",
            "plugging" | "cp1" | "cp4" => "PLUGGING_RECORDED",
            _ => "DOCUMENT_ADDED",
        };
        events.push((d, code.to_owned()));
    }

    let permit_date: Option<chrono::NaiveDate> = w.get("permit_date");
    let spud_date: Option<chrono::NaiveDate> = w.get("spud_date");
    let completion_date: Option<chrono::NaiveDate> = w.get("completion_date");
    let plug_date: Option<chrono::NaiveDate> = w.get("plug_date");
    let lifecycle_status: Option<String> = w.get("lifecycle_status");

    for (d_opt, code) in [
        (permit_date, "INTENT_POSTED"),
        (spud_date, "SPUD_RECORDED"),
        (completion_date, "COMPLETION_RECORDED"),
        (plug_date, "PLUGGING_RECORDED"),
    ] {
        if let Some(d) = d_opt {
            if d >= since {
                events.push((d, code.to_owned()));
            }
        }
    }

    events.sort_by(|a, b| b.0.cmp(&a.0).then_with(|| a.1.cmp(&b.1)));
    let mut seen = Vec::new();
    let mut activity = Vec::new();
    for (d, code) in events {
        let Some(label) = plain_event_label(&code) else {
            continue;
        };
        let key = (d, label);
        if seen.contains(&key) {
            continue;
        }
        seen.push(key);
        activity.push(json!({
            "date": d.format("%Y-%m-%d").to_string(),
            "event": label,
        }));
        if activity.len() >= EVENTS_PER_WELL as usize {
            break;
        }
    }

    // Lease-level KGS production (shared across wells on the lease, never well-level).
    let lease_prod_rows = sqlx::query(
        r#"
        WITH linked AS MATERIALIZED (
            SELECT DISTINCT l.lease_id, l.product
            FROM ks_well_lease_link l
            WHERE l.well_id = $1 AND l.confidence = 1
        )
        SELECT p.product,
               max(make_date(p.year, p.month, 1)) AS latest_month,
               sum(p.production_volume)::double precision AS total_vol
        FROM linked
        JOIN ks_lease_production_month p
          ON p.lease_id = linked.lease_id AND p.product = linked.product
        JOIN ks_ingestion_run r ON r.id = p.source_ingestion_run_id
        WHERE p.record_kind = 'monthly'
          AND make_date(p.year, p.month, 1) >= '2025-01-01'::date
          AND r.status = 'completed'
          AND r.parser_version = 'kgs_production_zip-v1'
          AND p.production_volume > 0
        GROUP BY p.product
        "#,
    )
    .bind(well_id)
    .fetch_all(pool)
    .await?;

    let mut lease_production = Map::new();
    let mut latest_lease_month: Option<chrono::NaiveDate> = None;
    for r in lease_prod_rows {
        let product: String = r.get("product");
        let vol: Option<f64> = r.get("total_vol");
        let lm: Option<chrono::NaiveDate> = r.get("latest_month");
        if let Some(m) = lm {
            latest_lease_month = Some(latest_lease_month.map_or(m, |cur| cur.max(m)));
        }
        if let Some(v) = vol.filter(|&x| x > 0.0) {
            match product.as_str() {
                "oil" => {
                    lease_production.insert("oil_bbl".to_owned(), number(v));
                }
                "gas" => {
                    lease_production.insert("gas_mcf".to_owned(), number(v));
                }
                _ => {}
            }
        }
    }
    if !lease_production.is_empty() {
        if let Some(m) = latest_lease_month {
            lease_production.insert(
                "latest_month".to_owned(),
                Value::String(m.format("%Y-%m").to_string()),
            );
        }
        lease_production.insert(
            "reporting_note".to_owned(),
            Value::String(KS_LEASE_SOURCE_NOTE.to_owned()),
        );
    }

    let mut well_obj = Map::new();
    if let Some(name) = well_name.as_deref().map(str::trim).filter(|s| !s.is_empty()) {
        well_obj.insert("well_name".to_owned(), Value::String(name.to_owned()));
    }
    if let Some(op) = w
        .get::<Option<String>, _>("operator")
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty() && !is_placeholder_operator(s))
    {
        well_obj.insert("operator".to_owned(), Value::String(op.to_owned()));
    }
    if let Some(c) = county.as_deref().map(str::trim).filter(|s| !s.is_empty()) {
        well_obj.insert("county".to_owned(), Value::String(c.to_owned()));
    }
    for (col, key) in [
        ("field_name", "field"),
        ("lease_name", "lease_name"),
        ("producing_formation", "producing_formation"),
        ("formation_at_total_depth", "formation_at_total_depth"),
    ] {
        if let Some(val) = w
            .get::<Option<String>, _>(col)
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty() && !is_placeholder_value(s))
        {
            well_obj.insert(key.to_owned(), Value::String(val.to_owned()));
        }
    }

    if let Some(wc) = w.get::<Option<String>, _>("well_class_raw") {
        if let Some(wt) = well_type_words(&wc) {
            well_obj.insert("well_type".to_owned(), Value::String(wt.to_owned()));
        }
    }
    if let Some(ls) = lifecycle_status.as_deref().and_then(status_words) {
        well_obj.insert("status".to_owned(), Value::String(ls.to_owned()));
    }
    if w.get::<bool, _>("horizontal_or_directional") {
        well_obj.insert("directional_or_horizontal".to_owned(), Value::Bool(true));
    }
    if let Some(td) = w.get::<Option<i32>, _>("total_depth").filter(|&d| d > 0) {
        well_obj.insert("total_depth_ft".to_owned(), json!(td));
    }
    for (d_opt, key) in [
        (permit_date, "permit_date"),
        (spud_date, "spud_date"),
        (completion_date, "completion_date"),
        (plug_date, "plug_date"),
    ] {
        if let Some(d) = d_opt {
            well_obj.insert(key.to_owned(), Value::String(d.format("%Y-%m-%d").to_string()));
        }
    }

    let mut initial_test = Map::new();
    for (col, key) in [
        ("ip_oil", "oil_bbl"),
        ("ip_gas", "gas_mcf"),
        ("ip_water", "water_bbl"),
    ] {
        if let Some(v) = w.get::<Option<f64>, _>(col).filter(|&x| x > 0.0) {
            initial_test.insert(key.to_owned(), number(v));
        }
    }
    if !initial_test.is_empty() {
        well_obj.insert("initial_test".to_owned(), Value::Object(initial_test));
    }

    let progress = where_it_stands(
        lifecycle_status.as_deref(),
        permit_date,
        spud_date,
        completion_date,
        plug_date,
        !lease_production.is_empty(),
    );

    let mut root = Map::new();
    root.insert("well".to_owned(), Value::Object(well_obj));
    if let Some(p) = progress {
        root.insert("progress".to_owned(), Value::String(p.to_owned()));
    }
    if !activity.is_empty() {
        root.insert("activity".to_owned(), Value::Array(activity));
    }
    if !lease_production.is_empty() {
        root.insert("lease_production".to_owned(), Value::Object(lease_production));
    }

    Ok(Some(Value::Object(root)))
}

fn number(value: f64) -> Value {
    if (value.fract()).abs() < 1e-6 {
        json!(value.round() as i64)
    } else {
        json!((value * 100.0).round() / 100.0)
    }
}

fn is_placeholder_value(value: &str) -> bool {
    let n = value.trim().to_ascii_uppercase();
    matches!(n.as_str(), "UNKNOWN" | "UNNAMED" | "UNAVAILABLE" | "NONE" | "N/A")
}

fn is_placeholder_operator(name: &str) -> bool {
    let n = name.trim().to_ascii_uppercase();
    n.contains("NOT ASSIGNED")
        || is_placeholder_value(&n)
        || n.starts_with("OPERATOR ")
}

fn well_type_words(code: &str) -> Option<&'static str> {
    let u = code.trim().to_ascii_uppercase();
    Some(match u.as_str() {
        "O" | "OIL" => "oil",
        "G" | "GAS" => "gas",
        "OG" | "O&G" | "OIL AND GAS" => "oil and gas",
        "I" | "INJ" | "INJECTION" | "EOR" => "injection",
        "S" | "SWD" => "saltwater disposal",
        "W" | "WSW" => "water",
        _ => return None,
    })
}

fn status_words(status: &str) -> Option<&'static str> {
    match status.trim().to_ascii_lowercase().as_str() {
        "producing" => Some("producing"),
        "shut_in" | "shut in" => Some("shut in"),
        "plugged" | "plugged_and_abandoned" => Some("plugged"),
        "drilling" => Some("drilling"),
        "permitted" => Some("permitted"),
        "inactive" | "temporarily_abandoned" => Some("inactive"),
        _ => None,
    }
}

fn plain_event_label(code: &str) -> Option<&'static str> {
    Some(match code {
        "INTENT_POSTED" => "Drilling intent posted",
        "INTENT_EXPIRED" => "Drilling intent expired",
        "SPUD_RECORDED" => "Well spudded",
        "COMPLETION_RECORDED" => "Completion recorded",
        "WELL_STATUS_CHANGED" => "Well status changed",
        "PLUGGING_RECORDED" => "Plugging recorded",
        "OPERATOR_TRANSFER_APPROVED" => "Operator change",
        "DOCUMENT_ADDED" => "Document added to the KGS well file",
        _ => return None,
    })
}

fn where_it_stands(
    lifecycle_status: Option<&str>,
    permit_date: Option<chrono::NaiveDate>,
    spud_date: Option<chrono::NaiveDate>,
    completion_date: Option<chrono::NaiveDate>,
    plug_date: Option<chrono::NaiveDate>,
    has_lease_production: bool,
) -> Option<&'static str> {
    if lifecycle_status == Some("plugged") || plug_date.is_some() {
        return Some("plugged on record");
    }
    if completion_date.is_some() && has_lease_production {
        return Some("completed with lease-level production on record");
    }
    if completion_date.is_some() {
        return Some("completed on record");
    }
    if has_lease_production {
        return Some("linked to a lease with production on record");
    }
    if spud_date.is_some() {
        return Some("spudded on record");
    }
    if permit_date.is_some() {
        return Some("drilling intent on record; not yet reported as drilled");
    }
    None
}

// ---------------------------------------------------------------------------------------
// Validation & cleanup (NM v3 / OK v8 safeguards)
// ---------------------------------------------------------------------------------------

fn has_public_identity(input: &Value) -> bool {
    let present = |key: &str| {
        input["well"][key]
            .as_str()
            .map(str::trim)
            .is_some_and(|v| !v.is_empty() && !v.eq_ignore_ascii_case("unknown"))
    };
    present("well_name") && present("county")
}

fn unsupported_words(summary: &str, input: &Value) -> Vec<String> {
    const COMMON: &[&str] = &[
        "the", "a", "an", "on", "in", "its", "it", "this", "that", "after", "before", "since",
        "following", "with", "as", "at", "by", "for", "from", "of", "to", "and", "both", "two",
        "three", "another", "later", "earlier", "then", "previously", "also", "over", "during",
        "through", "between", "according", "documents", "document", "cumulative", "total",
        "recent", "latest", "last", "most", "there", "these", "all", "each", "reported",
        "records", "filed", "permitted", "approved", "spud", "spudded", "plugging", "drilling",
        "operator", "well", "wells", "county", "kansas", "kcc", "kgs", "geological", "survey",
        "corporation", "commission", "pool", "field", "lease", "completed", "drilled", "tested",
        "plugged", "located", "operated", "initially", "originally", "production", "unit",
        "number", "mcf", "bbl", "llc", "inc", "lp", "co", "corp", "company", "ltd", "notice",
        "record", "completion", "permit", "intent", "linked", "previous", "additional", "further",
        "documentation", "january", "february", "march", "april", "may",
        "june", "july", "august", "september", "october", "november", "december",
    ];
    let facts_exact = input.to_string();
    let facts = facts_exact.to_lowercase();
    let prompt = SYSTEM_PROMPT.to_lowercase();
    let mut out = Vec::new();
    for raw in summary.split(|c: char| !(c.is_alphanumeric() || c == '&' || c == '-' || c == '\''))
    {
        let word = raw.trim_matches(|c: char| c == '-' || c == '\'');
        if word.chars().count() < 3 || !word.chars().next().is_some_and(char::is_uppercase) {
            continue;
        }
        if has_odd_capital(word) && !facts_exact.contains(word) {
            if !out.iter().any(|w: &String| w == word) {
                out.push(word.to_owned());
            }
            continue;
        }
        let lower = word.to_lowercase();
        let lower = lower.strip_suffix("'s").unwrap_or(&lower);
        if COMMON.contains(&lower)
            || facts.contains(lower)
            || prompt.contains(&format!(" {lower} "))
            || out.iter().any(|w: &String| w == word)
        {
            continue;
        }
        out.push(word.to_owned());
    }
    out
}

fn rejection_reason(summary: &str, input: &Value) -> Option<String> {
    let words = unsupported_words(summary, input);
    if !words.is_empty() {
        return Some(format!("words not in the facts: {}", words.join(", ")));
    }
    let lower = summary.to_ascii_lowercase();
    for leak in [
        "current step",
        "progress note",
        " stands ",
        "where it stands",
    ] {
        if lower.contains(leak) {
            return Some(format!("restated the input field ({})", leak.trim()));
        }
    }
    let well_type = input["well"]["well_type"].as_str().unwrap_or("");
    let claims_oil = lower.contains("oil well") || lower.contains("oil and gas well");
    let claims_gas = lower.contains("gas well");
    if (claims_oil && !well_type.contains("oil")) || (claims_gas && !well_type.contains("gas")) {
        return Some(format!(
            "well type not in the facts (facts: {})",
            if well_type.is_empty() { "none" } else { well_type }
        ));
    }
    if lower.contains("million") || lower.contains("billion") {
        return Some("converted units (million/billion)".to_owned());
    }
    if let Some(reason) = condition_contradiction(&lower, input) {
        return Some(reason);
    }
    let numbers = unsupported_numbers(summary, input);
    if !numbers.is_empty() {
        return Some(format!("numbers not in the facts: {}", numbers.join(", ")));
    }
    None
}

fn condition_contradiction(lower: &str, input: &Value) -> Option<String> {
    let progress = input["progress"].as_str().unwrap_or("");
    let well = &input["well"];
    let past_permit = (!progress.is_empty() && !progress.starts_with("drilling intent"))
        || input.get("lease_production").is_some_and(|p| !p.is_null())
        || well.get("spud_date").is_some_and(|v| !v.is_null())
        || well.get("total_depth_ft").is_some();
    if past_permit {
        for claim in [
            "not yet reported as drilled",
            "not yet been drilled",
            "not been drilled",
            "not yet drilled",
            "not been reported as drilled",
            "permitted but not",
            "no spud",
        ] {
            if lower.contains(claim) {
                return Some(format!("claims \"{claim}\" on a well past its permit"));
            }
        }
    }
    let facts = input.to_string().to_ascii_lowercase();
    if (lower.contains("not active") || lower.contains("no longer active"))
        && !facts.contains("inactive")
        && !facts.contains("not active")
        && !facts.contains("plug")
    {
        return Some("claims the well is not active without such a fact".to_owned());
    }
    None
}

fn unsupported_numbers(summary: &str, input: &Value) -> Vec<String> {
    let facts = input.to_string();
    let mut out = Vec::new();
    for token in summary.split_whitespace() {
        let raw = token.trim_matches(|c: char| !c.is_ascii_alphanumeric() && c != '#');
        if raw.is_empty()
            || !raw
                .chars()
                .all(|c| c.is_ascii_digit() || c == ',' || c == '.')
            || !raw.starts_with(|c: char| c.is_ascii_digit())
        {
            continue;
        }
        let mut norm = raw.replace(',', "");
        if norm.contains('.') {
            norm = norm.trim_end_matches('0').trim_end_matches('.').to_owned();
        }
        let value: f64 = norm.parse().unwrap_or(0.0);
        if value < 100.0 && !norm.contains('.') {
            continue;
        }
        if !facts.contains(&norm) && !out.iter().any(|o: &String| o == raw) {
            out.push(raw.to_owned());
        }
    }
    out
}

fn fix_capitals(summary: &str, input: &Value) -> String {
    let facts = input.to_string().to_lowercase();
    summary
        .split(' ')
        .map(|token| {
            let word = token.trim_matches(|c: char| !c.is_alphanumeric());
            if word.is_empty() || !has_odd_capital(word) || !facts.contains(&word.to_lowercase()) {
                return token.to_owned();
            }
            let mut title = String::with_capacity(word.len());
            for (i, ch) in word.chars().enumerate() {
                if i == 0 {
                    title.extend(ch.to_uppercase());
                } else {
                    title.extend(ch.to_lowercase());
                }
            }
            token.replacen(word, &title, 1)
        })
        .collect::<Vec<_>>()
        .join(" ")
}

fn has_odd_capital(word: &str) -> bool {
    let body = word
        .strip_prefix("Mc")
        .or_else(|| word.strip_prefix("Mac"))
        .unwrap_or(word);
    let chars: Vec<char> = body.chars().collect();
    chars
        .windows(2)
        .any(|w| w[0].is_lowercase() && w[1].is_uppercase())
}

fn input_sha256(input: &Value) -> String {
    let bytes = serde_json::to_vec(input).unwrap_or_default();
    hex::encode(Sha256::digest(bytes))
}

// ---------------------------------------------------------------------------------------
// Model call
// ---------------------------------------------------------------------------------------

#[derive(Debug)]
struct ModelOutput {
    summary: String,
    finish_reason: Option<String>,
    prompt_tokens: Option<i64>,
    completion_tokens: Option<i64>,
}

fn chat_request_body(model: &str, input: &Value, attempt: u32) -> Value {
    let mut body = json!({
        "model": model,
        "messages": [
            { "role": "system", "content": SYSTEM_PROMPT },
            { "role": "user", "content": format!("Well facts (JSON):\n{input}") },
        ],
        "temperature": if attempt <= 1 { 0.3 } else { 0.6 },
        "top_p": 0.9,
        "max_tokens": 350,
    });
    if model.to_ascii_lowercase().contains("qwen") {
        body["chat_template_kwargs"] = json!({ "enable_thinking": false });
    }
    body
}

fn clean_summary(raw: &str) -> String {
    let mut text = raw.to_owned();
    while let Some(start) = text.find("<think>") {
        match text[start..].find("</think>") {
            Some(end) => text.replace_range(start..start + end + "</think>".len(), ""),
            None => text.truncate(start),
        }
    }
    polish_summary(&text.split_whitespace().collect::<Vec<_>>().join(" "))
}

fn polish_summary(text: &str) -> String {
    let text = text
        .replace("**", "")
        .replace("__", "")
        .replace(" classified as ", " ")
        .replace(" officially ", " ");
    let mut out = String::with_capacity(text.len());
    let mut capitalize_next = false;
    for (i, word) in text.split(' ').enumerate() {
        let bare = word.trim_end_matches([',', '.']);
        if bare.eq_ignore_ascii_case("currently") {
            if word.ends_with('.') && out.ends_with(' ') {
                out.pop();
                out.push('.');
                out.push(' ');
            }
            if i == 0 || out.ends_with(". ") {
                capitalize_next = true;
            }
            continue;
        }
        if capitalize_next {
            let mut chars = word.chars();
            if let Some(first) = chars.next() {
                out.extend(first.to_uppercase());
                out.push_str(chars.as_str());
            }
            capitalize_next = false;
        } else {
            out.push_str(word);
        }
        out.push(' ');
    }
    drop_filler_sentences(out.trim_end())
}

fn drop_filler_sentences(text: &str) -> String {
    let mut kept = Vec::new();
    for sentence in text.split_inclusive(". ") {
        let lower = sentence.to_ascii_lowercase();
        let says_missing = lower.contains("available") || lower.contains("not provided");
        let missing = (says_missing
            && (lower.starts_with("no ") || lower.contains(" no ") || lower.contains(" not ")))
            || lower.contains("no further")
            || (lower.starts_with("no ")
                && (lower.contains("reported") || lower.contains("recorded")))
            || lower.contains("are expected")
            || lower.contains("is expected");
        if !missing {
            kept.push(sentence);
        }
    }
    let joined = kept.concat();
    if joined.trim().is_empty() {
        text.to_owned()
    } else {
        joined.trim_end().to_owned()
    }
}

fn parse_chat_response(body: &Value) -> Result<ModelOutput, String> {
    let choice = body["choices"].get(0).ok_or("response has no choices")?;
    let content = choice["message"]["content"]
        .as_str()
        .ok_or("response has no message content")?;
    let summary = clean_summary(content);
    if summary.is_empty() {
        return Err("model returned an empty summary".to_owned());
    }
    Ok(ModelOutput {
        summary,
        finish_reason: choice["finish_reason"].as_str().map(str::to_owned),
        prompt_tokens: body["usage"]["prompt_tokens"].as_i64(),
        completion_tokens: body["usage"]["completion_tokens"].as_i64(),
    })
}

enum CallError {
    Retryable(String),
    Fatal(String),
}

async fn call_model_once(
    client: &reqwest::Client,
    endpoint: &Endpoint,
    token: Option<&str>,
    body: &Value,
) -> Result<ModelOutput, CallError> {
    let mut req = client
        .post(format!("{}/v1/chat/completions", endpoint.base))
        .json(body);
    if let Some(tok) = token {
        req = req.bearer_auth(tok);
    }
    let response = req
        .send()
        .await
        .map_err(|e| CallError::Retryable(short_error(&e)))?;
    let status = response.status();
    if status.as_u16() == 401 || status.as_u16() == 403 {
        return Err(CallError::Fatal(format!(
            "box rejected the token (HTTP {})",
            status.as_u16()
        )));
    }
    if status.as_u16() == 429 || status.is_server_error() {
        let text = response.text().await.unwrap_or_default();
        return Err(CallError::Retryable(format!(
            "HTTP {}: {}",
            status.as_u16(),
            text.chars().take(160).collect::<String>()
        )));
    }
    if !status.is_success() {
        let text = response.text().await.unwrap_or_default();
        return Err(CallError::Fatal(format!(
            "HTTP {}: {}",
            status.as_u16(),
            text.chars().take(160).collect::<String>()
        )));
    }
    let parsed: Value = response
        .json()
        .await
        .map_err(|e| CallError::Retryable(format!("invalid JSON from box: {e}")))?;
    parse_chat_response(&parsed).map_err(CallError::Retryable)
}

// ---------------------------------------------------------------------------------------
// Worker pipeline
// ---------------------------------------------------------------------------------------

enum ItemOutcome {
    Succeeded {
        well_id: Uuid,
        lookup_api: String,
        input_sha256: String,
        input_json: Value,
        output: ModelOutput,
        latency_ms: i32,
        attempts: i32,
    },
    Skipped {
        well_id: Uuid,
        reason: String,
    },
    Failed {
        well_id: Uuid,
        lookup_api: String,
        error: String,
        attempts: i32,
        transport_failure: bool,
    },
    Fatal(String),
}

#[allow(clippy::too_many_arguments)]
async fn run_worker(
    pool: PgPool,
    client: reqwest::Client,
    run_id: i64,
    endpoints: Vec<Endpoint>,
    token: Option<String>,
    model: String,
    since: chrono::NaiveDate,
    concurrency: usize,
    force: bool,
    cancel: Arc<AtomicBool>,
) -> Result<(), String> {
    let rows = sqlx::query(
        "SELECT well_id, lookup_api FROM ks_ai_summary_run_items \
         WHERE run_id = $1 AND status = 'pending' \
         ORDER BY latest_activity_date DESC NULLS LAST, well_id",
    )
    .bind(run_id)
    .fetch_all(&pool)
    .await
    .map_err(|e| format!("loading queued items: {e}"))?;

    let (tx, rx) = tokio::sync::mpsc::channel::<(Uuid, String, Value, String)>(concurrency * 2);

    let producer_pool = pool.clone();
    let producer_cancel = cancel.clone();
    let producer_model = model.clone();
    let producer = tokio::spawn(async move {
        let mut skipped = Vec::new();
        for row in rows {
            if producer_cancel.load(Ordering::Relaxed) {
                break;
            }
            let well_id: Uuid = row.get("well_id");
            let lookup_api: String = row.get("lookup_api");
            let input = match build_input(&producer_pool, well_id, since).await {
                Ok(Some(v)) if has_public_identity(&v) => v,
                Ok(_) => {
                    skipped.push((well_id, "missing well name or county".to_owned()));
                    continue;
                }
                Err(e) => {
                    skipped.push((well_id, format!("input error: {e:?}")));
                    continue;
                }
            };
            let sha = input_sha256(&input);
            if !force {
                let exists: bool = sqlx::query_scalar(
                    "SELECT EXISTS(SELECT 1 FROM ks_well_ai_summaries \
                     WHERE well_id = $1 AND model = $2 AND prompt_version = $3 AND input_sha256 = $4)",
                )
                .bind(well_id)
                .bind(&producer_model)
                .bind(PROMPT_VERSION)
                .bind(&sha)
                .fetch_one(&producer_pool)
                .await
                .unwrap_or(false);
                if exists {
                    skipped.push((well_id, "unchanged input already summarised".to_owned()));
                    continue;
                }
            }
            if tx.send((well_id, lookup_api, input, sha)).await.is_err() {
                break;
            }
        }
        skipped
    });

    let ep_counter = Arc::new(AtomicUsize::new(0));
    let endpoints = Arc::new(endpoints);
    let token = Arc::new(token);
    let model_arc = Arc::new(model);

    let stream = futures_util::stream::unfold(rx, |mut rx| async move {
        rx.recv().await.map(|item| (item, rx))
    });

    let mut work_stream = Box::pin(
        stream
            .map(|(well_id, lookup_api, input, sha)| {
                let client = client.clone();
                let endpoints = endpoints.clone();
                let token = token.clone();
                let model = model_arc.clone();
                let ep_counter = ep_counter.clone();
                let cancel = cancel.clone();
                async move {
                    if cancel.load(Ordering::Relaxed) {
                        return ItemOutcome::Skipped {
                            well_id,
                            reason: "cancelled".to_owned(),
                        };
                    }
                    let started = Instant::now();
                    let mut last_err = String::new();
                    let mut transport_failure = false;
                    for attempt in 1..=MAX_ATTEMPTS {
                        let idx = ep_counter.fetch_add(1, Ordering::Relaxed) % endpoints.len();
                        let body = chat_request_body(&model, &input, attempt);
                        match call_model_once(&client, &endpoints[idx], token.as_deref(), &body)
                            .await
                        {
                            Ok(mut output) => {
                                output.summary = fix_capitals(&output.summary, &input);
                                if let Some(reason) = rejection_reason(&output.summary, &input) {
                                    last_err = format!("rejected: {reason}");
                                    transport_failure = false;
                                } else {
                                    return ItemOutcome::Succeeded {
                                        well_id,
                                        lookup_api,
                                        input_sha256: sha,
                                        input_json: input,
                                        output,
                                        latency_ms: started.elapsed().as_millis() as i32,
                                        attempts: attempt as i32,
                                    };
                                }
                            }
                            Err(CallError::Fatal(msg)) => return ItemOutcome::Fatal(msg),
                            Err(CallError::Retryable(msg)) => {
                                last_err = msg;
                                transport_failure = true;
                            }
                        }
                        if attempt < MAX_ATTEMPTS {
                            tokio::time::sleep(Duration::from_millis(250 * u64::from(attempt)))
                                .await;
                        }
                    }
                    ItemOutcome::Failed {
                        well_id,
                        lookup_api,
                        error: last_err,
                        attempts: MAX_ATTEMPTS as i32,
                        transport_failure,
                    }
                }
            })
            .buffer_unordered(concurrency),
    );

    let mut batch = Vec::with_capacity(WRITE_BATCH_MAX);
    let mut consecutive_transport_failures: u32 = 0;
    let mut fatal_error: Option<String> = None;

    while let Some(outcome) = work_stream.next().await {
        match &outcome {
            ItemOutcome::Fatal(msg) => {
                fatal_error = Some(msg.clone());
                cancel.store(true, Ordering::SeqCst);
                break;
            }
            ItemOutcome::Failed {
                transport_failure: true,
                ..
            } => {
                consecutive_transport_failures += 1;
                if consecutive_transport_failures >= MAX_CONSECUTIVE_FAILURES {
                    fatal_error = Some(format!(
                        "{MAX_CONSECUTIVE_FAILURES} consecutive box errors; stopping run"
                    ));
                    cancel.store(true, Ordering::SeqCst);
                    batch.push(outcome);
                    break;
                }
            }
            ItemOutcome::Succeeded { .. } => {
                consecutive_transport_failures = 0;
            }
            _ => {}
        }
        batch.push(outcome);
        if batch.len() >= WRITE_BATCH_MAX {
            flush_batch(&pool, run_id, &model_arc, &mut batch).await?;
        }
    }

    if !batch.is_empty() {
        flush_batch(&pool, run_id, &model_arc, &mut batch).await?;
    }

    if let Ok(skipped) = producer.await {
        if !skipped.is_empty() {
            let mut skip_batch: Vec<ItemOutcome> = skipped
                .into_iter()
                .map(|(well_id, reason)| ItemOutcome::Skipped { well_id, reason })
                .collect();
            while !skip_batch.is_empty() {
                let take = skip_batch.len().min(WRITE_BATCH_MAX);
                let mut chunk: Vec<ItemOutcome> = skip_batch.drain(..take).collect();
                flush_batch(&pool, run_id, &model_arc, &mut chunk).await?;
            }
        }
    }

    if let Some(err) = fatal_error {
        return Err(err);
    }

    let final_status = if cancel.load(Ordering::Relaxed) {
        "cancelled"
    } else {
        "completed"
    };
    sqlx::query("UPDATE ks_ai_summary_runs SET status = $2, finished_at = now() WHERE id = $1")
        .bind(run_id)
        .bind(final_status)
        .execute(&pool)
        .await
        .map_err(|e| format!("finishing run: {e}"))?;

    log(&pool, run_id, "info", None, &format!("Run {final_status}.")).await;

    Ok(())
}

async fn flush_batch(
    pool: &PgPool,
    run_id: i64,
    model: &str,
    batch: &mut Vec<ItemOutcome>,
) -> Result<(), String> {
    let mut tx = pool
        .begin()
        .await
        .map_err(|e| format!("starting batch tx: {e}"))?;
    let mut succ = 0_i32;
    let mut fail = 0_i32;
    let mut skip = 0_i32;
    let mut p_tok = 0_i64;
    let mut c_tok = 0_i64;

    for outcome in batch.drain(..) {
        match outcome {
            ItemOutcome::Succeeded {
                well_id,
                lookup_api,
                input_sha256,
                input_json,
                output,
                latency_ms,
                attempts,
            } => {
                let summary_id: i64 = sqlx::query_scalar(
                    "INSERT INTO ks_well_ai_summaries \
                     (well_id, lookup_api, run_id, model, prompt_version, input_sha256, input_json, \
                      summary, finish_reason, prompt_tokens, completion_tokens, latency_ms) \
                     VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12) \
                     ON CONFLICT (well_id, model, prompt_version, input_sha256) DO UPDATE \
                     SET summary = EXCLUDED.summary, run_id = EXCLUDED.run_id, \
                         finish_reason = EXCLUDED.finish_reason, prompt_tokens = EXCLUDED.prompt_tokens, \
                         completion_tokens = EXCLUDED.completion_tokens, latency_ms = EXCLUDED.latency_ms \
                     RETURNING id",
                )
                .bind(well_id)
                .bind(&lookup_api)
                .bind(run_id)
                .bind(model)
                .bind(PROMPT_VERSION)
                .bind(&input_sha256)
                .bind(&input_json)
                .bind(&output.summary)
                .bind(&output.finish_reason)
                .bind(output.prompt_tokens.map(|v| v as i32))
                .bind(output.completion_tokens.map(|v| v as i32))
                .bind(latency_ms)
                .fetch_one(&mut *tx)
                .await
                .map_err(|e| format!("saving summary: {e}"))?;

                sqlx::query(
                    "UPDATE ks_ai_summary_run_items \
                     SET status = 'succeeded', attempts = $3, latency_ms = $4, summary_id = $5, updated_at = now() \
                     WHERE run_id = $1 AND well_id = $2",
                )
                .bind(run_id)
                .bind(well_id)
                .bind(attempts)
                .bind(latency_ms)
                .bind(summary_id)
                .execute(&mut *tx)
                .await
                .map_err(|e| format!("updating run item: {e}"))?;

                succ += 1;
                p_tok += output.prompt_tokens.unwrap_or(0);
                c_tok += output.completion_tokens.unwrap_or(0);
            }
            ItemOutcome::Skipped { well_id, reason } => {
                sqlx::query(
                    "UPDATE ks_ai_summary_run_items \
                     SET status = 'skipped', error = $3, updated_at = now() \
                     WHERE run_id = $1 AND well_id = $2",
                )
                .bind(run_id)
                .bind(well_id)
                .bind(&reason)
                .execute(&mut *tx)
                .await
                .map_err(|e| format!("updating skipped item: {e}"))?;
                skip += 1;
            }
            ItemOutcome::Failed {
                well_id,
                lookup_api,
                error,
                attempts,
                ..
            } => {
                sqlx::query(
                    "UPDATE ks_ai_summary_run_items \
                     SET status = 'failed', attempts = $3, error = $4, updated_at = now() \
                     WHERE run_id = $1 AND well_id = $2",
                )
                .bind(run_id)
                .bind(well_id)
                .bind(attempts)
                .bind(&error)
                .execute(&mut *tx)
                .await
                .map_err(|e| format!("updating failed item: {e}"))?;
                let _ = sqlx::query(
                    "INSERT INTO ks_ai_summary_log(run_id, level, lookup_api, message) VALUES ($1, 'warn', $2, $3)",
                )
                .bind(run_id)
                .bind(&lookup_api)
                .bind(&error)
                .execute(&mut *tx)
                .await;
                fail += 1;
            }
            ItemOutcome::Fatal(_) => {}
        }
    }

    sqlx::query(
        "UPDATE ks_ai_summary_runs \
         SET succeeded_count = succeeded_count + $2, \
             failed_count = failed_count + $3, \
             skipped_count = skipped_count + $4, \
             prompt_tokens = prompt_tokens + $5, \
             completion_tokens = completion_tokens + $6 \
         WHERE id = $1",
    )
    .bind(run_id)
    .bind(succ)
    .bind(fail)
    .bind(skip)
    .bind(p_tok)
    .bind(c_tok)
    .execute(&mut *tx)
    .await
    .map_err(|e| format!("updating run counters: {e}"))?;

    tx.commit()
        .await
        .map_err(|e| format!("committing batch tx: {e}"))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn public_summary_path_filters_correctly() {
        assert!(public_summary_path("/api/ks/wells/15001200010000"));
        assert!(public_summary_path("/api/ks/counties/ellis"));
        assert!(public_summary_path("/api/ks/operators/acme-oil"));
        assert!(public_summary_path("/api/ks/fields/bemis-shutts"));
        assert!(public_summary_path("/api/ks/browse"));
        assert!(!public_summary_path("/api/ks/wells/map"));
        assert!(!public_summary_path("/api/ks/seo/sitemap-directories"));
    }

    #[test]
    fn require_superadmin_checks_headers() {
        let empty = HeaderMap::new();
        assert!(require_superadmin(&empty).is_err());

        let mut ok_headers = HeaderMap::new();
        ok_headers.insert("x-ks-admin-user", "yaro".parse().unwrap());
        assert_eq!(require_superadmin(&ok_headers).unwrap(), "yaro");
    }

    #[test]
    fn summary_validation_and_middleware_injection() {
        let input = json!({
            "well": {
                "well_name": "ELLIS UNIT 2",
                "operator": "SUNFLOWER OPERATING INC",
                "county": "Ellis",
                "well_type": "oil",
                "spud_date": "2026-03-15"
            },
            "progress": "spudded on record",
            "lease_production": {
                "oil_bbl": 840,
                "latest_month": "2026-09",
                "reporting_note": KS_LEASE_SOURCE_NOTE
            }
        });

        let good = "SUNFLOWER OPERATING INC spudded ELLIS UNIT 2 in Ellis County on March 15, 2026. Linked lease-level KGS production totals 840 barrels of oil through September 2026.";
        assert_eq!(rejection_reason(good, &input), None);

        let bad_name = "SUNFLOWER OPERATING INC spudded ELLIS UNIT 2 in Ellis County with Chevron.";
        assert!(rejection_reason(bad_name, &input).is_some());

        let bad_number = "ELLIS UNIT 2 in Ellis County produced 7,777 barrels.";
        assert!(rejection_reason(bad_number, &input).is_some());

        let mut payload = json!({
            "data": {
                "well": { "id": "w-ks-1", "api14": "15051200010000", "well_name": "ELLIS UNIT 2", "lifecycle_status": "producing" }
            }
        });
        let mut keys = Vec::new();
        collect_well_keys(&payload, &mut keys);
        assert!(keys.contains(&"15051200010000".to_owned()));

        let mut map = HashMap::new();
        map.insert("15051200010000".to_owned(), "Approved Kansas summary.".to_owned());
        insert_summaries(&mut payload, &map);
        assert_eq!(payload["data"]["well"]["ai_summary"], "Approved Kansas summary.");
    }
}
