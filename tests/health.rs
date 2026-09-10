use axum::body::Body;
use axum::http::{Request, StatusCode};
use future_wells_kansas::api;
use future_wells_kansas::api::state::AppState;
use tower::ServiceExt;

async fn test_pool() -> sqlx::PgPool {
    let database_url =
        std::env::var("DATABASE_URL").expect("DATABASE_URL must be set to run health tests");
    let pool = future_wells_kansas::db::connect(&database_url)
        .await
        .expect("failed to connect to test database");
    sqlx::migrate!("./migrations").run(&pool).await.unwrap();
    pool
}

#[tokio::test]
async fn health_endpoint_returns_ok_envelope() {
    let app = api::router(AppState {
        pool: test_pool().await,
    });

    let response = app
        .oneshot(
            Request::builder()
                .uri("/health")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();

    assert_eq!(json["data"]["status"], "ok");
    assert_eq!(json["meta"]["state"], "KS");
}

#[tokio::test]
async fn browse_endpoint_returns_public_summary_envelope() {
    let app = api::router(AppState {
        pool: test_pool().await,
    });

    let response = app
        .oneshot(
            Request::builder()
                .uri("/api/ks/browse")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();

    assert!(json["data"]["recent_activity"].is_array());
    assert!(json["data"]["top_counties"].is_array());
    assert!(json["data"]["top_operators"].is_array());
    assert!(json["data"]["top_fields"].is_array());
    assert_eq!(json["meta"]["state"], "KS");
}
