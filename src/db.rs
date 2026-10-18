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
