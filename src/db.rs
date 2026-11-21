use sqlx::postgres::{PgPool, PgPoolOptions};

/// Creates a bounded connection pool. No query runs at construction time
/// beyond the pool's own connection handshake.
pub async fn connect(database_url: &str) -> Result<PgPool, sqlx::Error> {
    PgPoolOptions::new()
        .max_connections(
            std::env::var("KS_DB_MAX_CONNECTIONS")
                .ok()
                .and_then(|v| v.parse::<u32>().ok())
                .filter(|v| (1..=32).contains(v))
                .unwrap_or(4),
        )
        .idle_timeout(std::time::Duration::from_secs(60))
        .connect(database_url)
        .await
}

/// One lazy connection for the AI summary worker, separate from the public API pool so a long
/// summary run never queues public requests, and it closes itself when idle.
pub fn connect_ai_summary_worker_lazy(database_url: &str) -> Result<PgPool, sqlx::Error> {
    PgPoolOptions::new()
        .min_connections(0)
        .max_connections(1)
        .acquire_timeout(std::time::Duration::from_secs(30))
        .idle_timeout(Some(std::time::Duration::from_secs(60)))
        .after_connect(|connection, _| {
            Box::pin(async move {
                sqlx::query("SELECT set_config('max_parallel_workers_per_gather', '0', false), set_config('lock_timeout', '3s', false), set_config('statement_timeout', '30s', false), set_config('application_name', 'fw-ks-ai-summaries', false)")
                    .execute(&mut *connection)
                    .await?;
                Ok(())
            })
        })
        .connect_lazy(database_url)
}
