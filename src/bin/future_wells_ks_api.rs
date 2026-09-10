use future_wells_kansas::{api, api::state::AppState, config::Config, db};
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenvy::dotenv().ok();

    let cfg = Config::from_env()?;

    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::new(&cfg.log_level))
        .init();

    let pool = db::connect(&cfg.database_url).await?;
    sqlx::migrate!("./migrations").run(&pool).await?;

    let addr = format!("{}:{}", cfg.api_host, cfg.api_port);
    let listener = tokio::net::TcpListener::bind(&addr).await?;
    tracing::info!(%addr, "future_wells_ks_api listening");

    let app = api::router(AppState { pool });
    axum::serve(listener, app).await?;

    Ok(())
}
