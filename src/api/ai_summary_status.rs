//! Small status record of the AI summary pipeline for the SEO host telemetry collector.
//!
//! The collector (`futureWellsOps/scripts/seo_telemetry_readonly.py`) reads no database, so the
//! API periodically writes the latest run outcome and the newest generated/approved summary times
//! to `/var/lib/future-wells-<st>/backend-data/ai-summary-runs/latest.json`, a path inside the
//! isolated API unit's `ReadWritePaths`. The superadmin SEO monitoring page on futurewells.co
//! shows it as the `ai-summaries` job. The record holds no well data, prompts, endpoints, tokens
//! or error text: only a mapped status, timestamps, the prompt version and two counts.
//!
//! `FW_AI_SUMMARY_STATUS_FILE` overrides the path. Without the override the record is written only
//! when the isolated `backend-data` directory exists, so development machines write nothing.

use serde_json::{json, Value};
use sqlx::{PgPool, Row};
use std::{
    io::Write,
    path::{Path, PathBuf},
    time::Duration,
};

const STATE: &str = "ks";
const INTERVAL: Duration = Duration::from_secs(120);

/// One cheap query: the newest run by primary key plus two aggregates over the summary table.
const STATUS_SQL: &str = r#"
SELECT
    r.id AS run_id,
    CASE r.status
        WHEN 'completed' THEN 'succeeded'
        WHEN 'cancelling' THEN 'running'
        WHEN 'interrupted' THEN 'stopped'
        ELSE r.status
    END AS status,
    to_char(COALESCE(r.started_at, r.created_at) AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS started_at,
    to_char(r.finished_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS ended_at,
    r.prompt_version,
    (SELECT to_char(max(s.created_at) AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"')
       FROM ks_well_ai_summaries s) AS last_generated_at,
    (SELECT to_char(max(COALESCE(s.reviewed_at, s.created_at)) AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"')
       FROM ks_well_ai_summaries s WHERE s.review_status = 'approved') AS last_approved_at,
    (SELECT count(DISTINCT s.well_id) FROM ks_well_ai_summaries s
      WHERE s.review_status = 'approved') AS approved_wells,
    to_char(now() AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS recorded_at
FROM (SELECT 1) AS one
LEFT JOIN LATERAL (
    SELECT id, status, started_at, created_at, finished_at, prompt_version
      FROM ks_ai_summary_runs ORDER BY id DESC LIMIT 1
) AS r ON true
"#;

pub fn record_path() -> Option<PathBuf> {
    if let Some(path) = std::env::var_os("FW_AI_SUMMARY_STATUS_FILE").filter(|v| !v.is_empty()) {
        return Some(PathBuf::from(path));
    }
    let data = PathBuf::from(format!("/var/lib/future-wells-{STATE}/backend-data"));
    data.is_dir()
        .then(|| data.join("ai-summary-runs").join("latest.json"))
}

/// Starts the background writer when a record path is configured. Failures are logged and retried
/// on the next tick; they never affect the API.
pub fn spawn(pool: PgPool) {
    let Some(path) = record_path() else {
        tracing::debug!("AI summary status record disabled: no isolated backend-data directory");
        return;
    };
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(INTERVAL);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            ticker.tick().await;
            if let Err(error) = refresh(&pool, &path).await {
                tracing::warn!(error = %error, "writing the AI summary status record failed");
            }
        }
    });
}

pub async fn refresh(pool: &PgPool, path: &Path) -> anyhow::Result<()> {
    let record = load(pool).await?;
    let path = path.to_path_buf();
    tokio::task::spawn_blocking(move || write_atomically(&path, &record)).await??;
    Ok(())
}

pub async fn load(pool: &PgPool) -> anyhow::Result<Value> {
    let row = sqlx::query(STATUS_SQL).fetch_one(pool).await?;
    let run_id: Option<i64> = row.try_get("run_id")?;
    let status: Option<String> = row.try_get("status")?;
    Ok(json!({
        "schema": "future-wells-ai-summary-status/1",
        "job": "ai-summaries",
        "state": STATE.to_ascii_uppercase(),
        "run_id": run_id,
        // No run yet is reported as no status, never as a failure or success.
        "status": status,
        "started_at": row.try_get::<Option<String>, _>("started_at")?,
        "ended_at": row.try_get::<Option<String>, _>("ended_at")?,
        "prompt_version": row.try_get::<Option<String>, _>("prompt_version")?,
        "last_generated_at": row.try_get::<Option<String>, _>("last_generated_at")?,
        "last_approved_at": row.try_get::<Option<String>, _>("last_approved_at")?,
        "approved_wells": row.try_get::<i64, _>("approved_wells")?,
        "recorded_at": row.try_get::<String, _>("recorded_at")?,
    }))
}

pub fn write_atomically(path: &Path, record: &Value) -> anyhow::Result<()> {
    let directory = path
        .parent()
        .ok_or_else(|| anyhow::anyhow!("status record path has no parent"))?;
    std::fs::create_dir_all(directory)?;
    let temporary = directory.join(format!(".latest.{}.tmp", std::process::id()));
    let result = (|| {
        let mut file = std::fs::File::create(&temporary)?;
        serde_json::to_writer(&mut file, record)?;
        file.write_all(b"\n")?;
        file.sync_all()?;
        std::fs::rename(&temporary, path)?;
        anyhow::Ok(())
    })();
    if result.is_err() {
        let _ = std::fs::remove_file(&temporary);
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn writes_a_complete_record_and_replaces_it() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("ai-summary-runs").join("latest.json");
        write_atomically(&path, &json!({"status": "running"})).unwrap();
        write_atomically(&path, &json!({"status": "succeeded"})).unwrap();
        let value: Value = serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
        assert_eq!(value["status"], "succeeded");
        let leftovers: Vec<_> = std::fs::read_dir(path.parent().unwrap())
            .unwrap()
            .map(|e| e.unwrap().file_name())
            .collect();
        assert_eq!(leftovers, vec![std::ffi::OsString::from("latest.json")]);
    }

    /// Opt-in: point `AI_STATUS_TEST_DATABASE_URL` at an isolated scratch database that already
    /// has the AI summary schema. Never run it against a shared or production database.
    #[tokio::test]
    #[ignore]
    async fn refresh_writes_the_record_from_a_scratch_database() {
        let url = std::env::var("AI_STATUS_TEST_DATABASE_URL").expect("scratch database url");
        let pool = PgPool::connect(&url).await.unwrap();
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("latest.json");
        refresh(&pool, &path).await.unwrap();
        let value: Value = serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
        assert_eq!(value["job"], "ai-summaries");
        assert_eq!(value["state"], STATE.to_ascii_uppercase());
        assert!(value["approved_wells"].is_i64());
        println!("{value}");
    }

    #[test]
    fn status_query_reads_only_this_states_tables() {
        assert!(STATUS_SQL.contains(&format!("FROM {STATE}_ai_summary_runs")));
        assert!(STATUS_SQL.contains(&format!("FROM {STATE}_well_ai_summaries")));
        for other in ["co", "nd", "nm", "ok", "ut", "wy", "ks"].into_iter().filter(|s| *s != STATE) {
            assert!(!STATUS_SQL.contains(&format!("{other}_ai_summary")), "{other}");
            assert!(!STATUS_SQL.contains(&format!("{other}_well_ai")), "{other}");
        }
    }
}
