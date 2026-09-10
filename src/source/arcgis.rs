//! Resolves an ArcGIS Online item ID to its current hosted-service URL.
//!
//! KGS publishes the well master as a hosted ArcGIS FeatureServer layer.
//! The item ID (`config::SourceUrls::kgs_wells_arcgis_item_id`) is stable
//! for the life of the item, but the FeatureServer URL it points to can
//! change if KGS republishes or moves the underlying service — pinning
//! that URL in config and never re-checking it is exactly how sibling
//! state backends have accumulated silently-stale source URLs. Resolve it
//! at runtime instead; fall back to the pinned URL only when this lookup
//! itself is impractical (offline dev, arcgis.com unreachable).

use serde::Deserialize;

const ARCGIS_ITEM_API_BASE: &str = "https://www.arcgis.com/sharing/rest/content/items";

#[derive(Debug, Deserialize)]
struct ArcGisItemResponse {
    url: Option<String>,
    /// Present when the item ID is unknown/inaccessible; surfaced in the
    /// error rather than silently treated as a missing `url`.
    error: Option<ArcGisItemError>,
}

#[derive(Debug, Deserialize)]
struct ArcGisItemError {
    message: Option<String>,
}

#[derive(Debug, thiserror::Error)]
pub enum ArcGisResolveError {
    #[error("request to ArcGIS item API failed: {0}")]
    Request(#[from] reqwest::Error),
    #[error("ArcGIS item {item_id} returned an error: {message}")]
    ItemError { item_id: String, message: String },
    #[error("ArcGIS item {item_id} has no service url in its metadata")]
    MissingUrl { item_id: String },
}

/// Fetches `{item_id}`'s public metadata from the ArcGIS Online sharing
/// API and returns its current hosted *service* URL. Network- and
/// item-lookup errors are returned rather than panicking, so callers can
/// fall back to a pinned config URL when resolution isn't practical right
/// now.
///
/// Verified against the live KGS wells item at KS-002: the returned URL is
/// the FeatureServer root (e.g. `.../FeatureServer`), not a specific
/// layer — callers must append `/{layer_index}` (`/0` for the wells
/// layer) themselves before querying features, the same way the pinned
/// `kgs_wells_arcgis_layer` fallback already includes it.
pub async fn resolve_service_url(item_id: &str) -> Result<String, ArcGisResolveError> {
    let url = format!("{ARCGIS_ITEM_API_BASE}/{item_id}?f=json");
    let response = reqwest::get(&url)
        .await?
        .json::<ArcGisItemResponse>()
        .await?;

    if let Some(error) = response.error {
        return Err(ArcGisResolveError::ItemError {
            item_id: item_id.to_string(),
            message: error.message.unwrap_or_else(|| "unknown error".to_string()),
        });
    }

    response.url.ok_or_else(|| ArcGisResolveError::MissingUrl {
        item_id: item_id.to_string(),
    })
}

// --- Generic ArcGIS FeatureServer query primitives (KS-03) ---
//
// `layer_url` throughout is the full layer endpoint including the layer
// index, e.g. `{service_root}/0` — callers resolve the service root via
// `resolve_service_url` (or a pinned fallback) and append `/{index}`
// themselves, per the doc comment above. Every function takes `layer_url`
// as a parameter rather than hardcoding a KGS-specific constant so tests
// can point them at a `wiremock` server instead of the real service.

#[derive(Debug, thiserror::Error)]
pub enum ArcGisQueryError {
    #[error("request to ArcGIS layer failed: {0}")]
    Request(#[from] reqwest::Error),
    #[error("unexpected ArcGIS response shape: {0}")]
    UnexpectedShape(String),
}

#[derive(Debug, Clone, Deserialize)]
pub struct ArcGisEditingInfo {
    #[serde(rename = "dataLastEditDate")]
    pub data_last_edit_date: Option<i64>,
    #[serde(rename = "schemaLastEditDate")]
    pub schema_last_edit_date: Option<i64>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ArcGisFieldMeta {
    pub name: String,
    #[serde(rename = "type")]
    pub field_type: String,
}

/// The layer metadata KS-03 requires recording "before extraction" and
/// re-reading "after extraction" to detect a source change mid-import.
#[derive(Debug, Clone, Deserialize)]
pub struct ArcGisLayerMetadata {
    #[serde(rename = "maxRecordCount")]
    pub max_record_count: i64,
    #[serde(rename = "editingInfo")]
    pub editing_info: Option<ArcGisEditingInfo>,
    #[serde(default)]
    pub fields: Vec<ArcGisFieldMeta>,
}

impl ArcGisLayerMetadata {
    /// A short, stable fingerprint of the field schema (names + types,
    /// sorted) — used both as `ks_ingestion_run.schema_fingerprint` and to
    /// detect a schema change between the before/after metadata reads.
    pub fn schema_fingerprint(&self) -> String {
        let mut parts: Vec<String> = self
            .fields
            .iter()
            .map(|f| format!("{}:{}", f.name, f.field_type))
            .collect();
        parts.sort();
        use sha2::{Digest, Sha256};
        let mut hasher = Sha256::new();
        hasher.update(parts.join(","));
        format!("{:x}", hasher.finalize())
    }
}

pub async fn fetch_layer_metadata(
    client: &reqwest::Client,
    layer_url: &str,
) -> Result<ArcGisLayerMetadata, ArcGisQueryError> {
    let url = format!("{layer_url}?f=json");
    Ok(client.get(&url).send().await?.json().await?)
}

#[derive(Debug, Deserialize)]
struct CountResponse {
    count: i64,
}

pub async fn fetch_total_count(
    client: &reqwest::Client,
    layer_url: &str,
) -> Result<i64, ArcGisQueryError> {
    let url = format!("{layer_url}/query");
    let response: CountResponse = client
        .get(&url)
        .query(&[("where", "1=1"), ("returnCountOnly", "true"), ("f", "json")])
        .send()
        .await?
        .json()
        .await?;
    Ok(response.count)
}

#[derive(Debug, Deserialize)]
struct ObjectIdsResponse {
    #[serde(rename = "objectIds")]
    object_ids: Option<Vec<i64>>,
    #[serde(rename = "exceededTransferLimit", default)]
    exceeded_transfer_limit: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ObjectIdsPage {
    pub object_ids: Vec<i64>,
    pub exceeded_transfer_limit: bool,
}

/// Fetches one page of object IDs, cursor-paginated by `OBJECTID` (the
/// ArcGIS `returnIdsOnly` endpoint truncates at an undocumented limit
/// well below the layer's real row count — verified live against the KGS
/// wells layer at KS-03, which truncates at 100,000 of 519,092 total, and
/// reports that in `exceededTransferLimit` rather than in `maxRecordCount`
/// — so `resultOffset` is not the right pagination mechanism here;
/// `WHERE OBJECTID > :after_id ORDER BY OBJECTID` is). Pass the last ID
/// from the previous page as `after_id`; `None` starts from the beginning.
pub async fn fetch_object_ids_page(
    client: &reqwest::Client,
    layer_url: &str,
    after_id: Option<i64>,
) -> Result<ObjectIdsPage, ArcGisQueryError> {
    let where_clause = match after_id {
        Some(id) => format!("OBJECTID>{id}"),
        None => "1=1".to_string(),
    };
    let url = format!("{layer_url}/query");
    let response: ObjectIdsResponse = client
        .get(&url)
        .query(&[
            ("where", where_clause.as_str()),
            ("returnIdsOnly", "true"),
            ("orderByFields", "OBJECTID"),
            ("f", "json"),
        ])
        .send()
        .await?
        .json()
        .await?;
    Ok(ObjectIdsPage {
        object_ids: response.object_ids.unwrap_or_default(),
        exceeded_transfer_limit: response.exceeded_transfer_limit,
    })
}

#[derive(Debug, Clone, Deserialize)]
pub struct ArcGisGeometry {
    pub x: f64,
    pub y: f64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ArcGisFeature {
    pub attributes: serde_json::Map<String, serde_json::Value>,
    pub geometry: Option<ArcGisGeometry>,
}

#[derive(Debug, Deserialize)]
struct FeaturesResponse {
    #[serde(default)]
    features: Vec<ArcGisFeature>,
}

/// Fetches full attributes + WGS84 geometry (`outSR=4326`, per KS-03) for
/// exactly the given object IDs. Callers must never pass more IDs than
/// the layer's `maxRecordCount` (KS-03: "fetch records in batches no
/// larger than the source maximum") — this function does not chunk on
/// the caller's behalf, so chunking happens once, visibly, at the call
/// site (see `parse::kgs_wells::fetch_all_features`).
pub async fn fetch_features_batch(
    client: &reqwest::Client,
    layer_url: &str,
    object_ids: &[i64],
) -> Result<Vec<ArcGisFeature>, ArcGisQueryError> {
    if object_ids.is_empty() {
        return Ok(Vec::new());
    }
    let ids_csv = object_ids
        .iter()
        .map(|id| id.to_string())
        .collect::<Vec<_>>()
        .join(",");
    let url = format!("{layer_url}/query");
    let response: FeaturesResponse = client
        .get(&url)
        .query(&[
            ("objectIds", ids_csv.as_str()),
            ("outFields", "*"),
            ("outSR", "4326"),
            ("returnGeometry", "true"),
            ("f", "json"),
        ])
        .send()
        .await?
        .json()
        .await?;
    Ok(response.features)
}

/// Pages through the full object-ID list via `OBJECTID > :after` cursor
/// pagination (see `fetch_object_ids_page`'s doc comment for why this,
/// not `resultOffset`, is required against layers like this). Shared by
/// every ArcGIS-backed importer (KS-03's wells, KS-08's fields) — the
/// pagination behavior is a property of the ArcGIS REST API itself, not
/// of any one dataset.
pub async fn fetch_all_object_ids(
    client: &reqwest::Client,
    layer_url: &str,
    retry_policy: crate::source::retry::RetryPolicy,
    limit: Option<usize>,
) -> Result<Vec<i64>, ArcGisQueryError> {
    use crate::source::retry::with_retry;
    let mut object_ids = Vec::new();
    let mut after_id: Option<i64> = None;
    loop {
        let page = with_retry(retry_policy, || {
            fetch_object_ids_page(client, layer_url, after_id)
        })
        .await?;
        if page.object_ids.is_empty() {
            break;
        }
        after_id = page.object_ids.last().copied();
        object_ids.extend(page.object_ids);
        if let Some(limit) = limit {
            if object_ids.len() >= limit {
                object_ids.truncate(limit);
                break;
            }
        }
        if !page.exceeded_transfer_limit {
            break;
        }
    }
    object_ids.sort_unstable();
    object_ids.dedup();
    Ok(object_ids)
}

/// A GeoJSON feature as returned by `f=geojson` — used for polygon/line
/// layers (KS-08's field boundaries) instead of `fetch_features_batch`'s
/// esri-native point geometry shape. Confirmed live against the real KGS
/// fields layer at KS-08: requesting `f=geojson` makes the ArcGIS server
/// itself do the esri-ring-to-GeoJSON conversion (including correctly
/// separating outer rings from holes), which is far more reliable than
/// reimplementing that algorithm client-side from the esri-native
/// `{"rings": [...]}` shape `fetch_features_batch` would otherwise return.
#[derive(Debug, Clone, Deserialize)]
pub struct GeoJsonFeature {
    pub properties: serde_json::Map<String, serde_json::Value>,
    /// A raw GeoJSON geometry object (`{"type": "Polygon", "coordinates": [...]}`)
    /// — stored as-is; callers pass it straight to Postgres/PostGIS's
    /// `ST_GeomFromGeoJSON`, never hand-parsed in Rust.
    pub geometry: Option<serde_json::Value>,
}

#[derive(Debug, Deserialize)]
struct GeoJsonFeatureCollection {
    #[serde(default)]
    features: Vec<GeoJsonFeature>,
}

/// Same contract as `fetch_features_batch`, but for geometry types
/// (polygons, lines) where GeoJSON's explicit ring/hole structure is
/// worth the extra request-shape difference. `outSR=4326` still applies —
/// confirmed live that the ArcGIS server honors it for `f=geojson` output
/// exactly as it does for `f=json`.
pub async fn fetch_features_batch_geojson(
    client: &reqwest::Client,
    layer_url: &str,
    object_ids: &[i64],
) -> Result<Vec<GeoJsonFeature>, ArcGisQueryError> {
    if object_ids.is_empty() {
        return Ok(Vec::new());
    }
    let ids_csv = object_ids
        .iter()
        .map(|id| id.to_string())
        .collect::<Vec<_>>()
        .join(",");
    let url = format!("{layer_url}/query");
    let response: GeoJsonFeatureCollection = client
        .get(&url)
        .query(&[
            ("objectIds", ids_csv.as_str()),
            ("outFields", "*"),
            ("outSR", "4326"),
            ("returnGeometry", "true"),
            ("f", "geojson"),
        ])
        .send()
        .await?
        .json()
        .await?;
    Ok(response.features)
}
