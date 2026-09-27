pub mod browse;
pub mod envelope;
pub mod error;
pub mod health;
pub mod seo;
pub mod state;

use axum::{routing::get, Router};

use state::AppState;

/// Route table. `/api/ks/{resource}` once real resources land, matching
/// the `/api/<abbr>/...` convention documented in
/// `docs/architecture/kansas-source-boundary.md`. KS-001 scaffold ships
/// only the liveness route — every data-backed route (wells, operators,
/// production, permits, dockets) is added task-by-task per
/// `future_wells_kansas_backend_marketing_codex_tasks.md` §7-8, once its
/// underlying table is materialized from a validated source.
pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/api/ks/browse", get(browse::start))
        .route("/api/ks/wells/:api", get(browse::well_detail))
        .route("/api/ks/counties", get(browse::county_directory))
        .route("/api/ks/counties/:slug", get(browse::county_detail))
        .route("/api/ks/operators", get(browse::operator_directory))
        .route("/api/ks/operators/:slug", get(browse::operator_detail))
        .route("/api/ks/fields", get(browse::field_directory))
        .route("/api/ks/fields/:slug", get(browse::field_detail))
        .route(
            "/api/ks/seo/sitemap-directories",
            get(seo::sitemap_directories),
        )
        .route("/health", get(health::health))
        .with_state(state)
}
