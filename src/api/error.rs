use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde::Serialize;

/// Every error this API returns. Kept outside the `{data, meta}` envelope
/// (which is for successful responses only) since an error body has no
/// meaningful `data`.
#[derive(Debug)]
pub enum ApiError {
    BadRequest(String),
    Unauthorized(String),
    NotFound(String),
    Internal(String),
}

#[derive(Debug, Serialize)]
struct ErrorBody {
    error: ErrorDetail,
}

#[derive(Debug, Serialize)]
struct ErrorDetail {
    code: &'static str,
    message: String,
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let (status, code, message) = match self {
            ApiError::BadRequest(message) => (StatusCode::BAD_REQUEST, "bad_request", message),
            ApiError::Unauthorized(message) => (StatusCode::UNAUTHORIZED, "unauthorized", message),
            ApiError::NotFound(message) => (StatusCode::NOT_FOUND, "not_found", message),
            ApiError::Internal(message) => {
                tracing::error!(%message, "internal API error");
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "internal_error",
                    "an internal error occurred".to_string(),
                )
            }
        };
        (
            status,
            Json(ErrorBody {
                error: ErrorDetail { code, message },
            }),
        )
            .into_response()
    }
}

impl From<sqlx::Error> for ApiError {
    fn from(e: sqlx::Error) -> Self {
        ApiError::Internal(e.to_string())
    }
}

impl ApiError {
    pub fn bad_request(message: impl std::fmt::Display) -> Self {
        Self::BadRequest(message.to_string())
    }

    pub fn unauthorized(message: impl std::fmt::Display) -> Self {
        Self::Unauthorized(message.to_string())
    }

    pub fn internal(message: impl std::fmt::Display) -> Self {
        Self::Internal(message.to_string())
    }
}
