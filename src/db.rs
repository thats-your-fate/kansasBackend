use sqlx::postgres::{PgPool, PgPoolOptions};

/// Creates a bounded connection pool. No query runs at construction time
/// beyond the pool's own connection handshake.
pub async fn connect(database_url: &str) -> Result<PgPool, sqlx::Error> {
    PgPoolOptions::new()
        .max_connections(10)
        .connect(database_url)
        .await
}
