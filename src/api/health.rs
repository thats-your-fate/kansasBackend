use axum::Json;
use serde::Serialize;

use super::envelope::{Envelope, Meta};

#[derive(Debug, Serialize)]
pub struct HealthData {
    pub status: &'static str,
    pub service: &'static str,
    pub version: &'static str,
}

/// Liveness endpoint only. Makes no database call and no source request —
/// KS-001 scope is "backend starts locally with no source access."
pub async fn health() -> Json<Envelope<HealthData>> {
    let data = HealthData {
        status: "ok",
        service: "future_wells_ks_api",
        version: env!("CARGO_PKG_VERSION"),
    };
    Json(Envelope::new(data, Meta::bare()))
}
