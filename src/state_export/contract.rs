use chrono::{DateTime, NaiveDate, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;

pub const SCHEMA_VERSION: &str = "state-export-v1";
pub const SOURCE_CODE: &str = "kansas_backend";

#[derive(Debug, thiserror::Error)]
pub enum ContractError {
    #[error("unsupported schema version: {0}")]
    UnsupportedSchemaVersion(String),
    #[error("{entity} source_key is required")]
    MissingSourceKey { entity: &'static str },
    #[error("invalid JSON: {0}")]
    InvalidJson(#[from] serde_json::Error),
}

pub type ContractResult<T> = Result<T, ContractError>;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum ExportModeV1 {
    Baseline,
    Full,
    Incremental,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct StateExportManifestV1 {
    pub export_id: String,
    pub schema_version: String,
    pub state_code: String,
    pub source_code: String,
    pub export_mode: ExportModeV1,
    pub created_at: DateTime<Utc>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lower_change_sequence: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub upper_change_sequence: Option<i64>,
    pub watermark_start: Option<DateTime<Utc>>,
    pub watermark_end: Option<DateTime<Utc>>,
    pub snapshot_at: Option<DateTime<Utc>>,
    pub files: Vec<EntityFileDescriptorV1>,
}

impl StateExportManifestV1 {
    pub fn parse_json(input: &str) -> ContractResult<Self> {
        let manifest: Self = serde_json::from_str(input)?;
        manifest.validate()?;
        Ok(manifest)
    }

    pub fn validate(&self) -> ContractResult<()> {
        reject_unsupported_version(&self.schema_version)?;
        require_value("Manifest", &self.export_id)?;
        require_value("Manifest", &self.state_code)?;
        require_value("Manifest", &self.source_code)?;
        for file in &self.files {
            require_value("ManifestFile", &file.filename)?;
            require_value("ManifestFile", &file.entity_type)?;
            require_value("ManifestFile", &file.sha256_checksum)?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct EntityFileDescriptorV1 {
    pub filename: String,
    pub entity_type: String,
    pub compression_type: Option<String>,
    pub record_count: u64,
    pub sha256_checksum: String,
    pub min_source_key: Option<String>,
    pub max_source_key: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SourceEnvelopeV1 {
    pub source_key: String,
    pub source_updated_at: Option<DateTime<Utc>>,
    pub source_payload: Value,
}

impl SourceEnvelopeV1 {
    fn validate(&self, entity: &'static str) -> ContractResult<()> {
        require_value(entity, &self.source_key)
    }
}

macro_rules! entity_record {
    ($name:ident) => {
        impl $name {
            pub fn validate(&self) -> ContractResult<()> {
                self.source.validate(stringify!($name))
            }
        }
    };
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CountyV1 {
    #[serde(flatten)]
    pub source: SourceEnvelopeV1,
    pub name: String,
    pub normalized_name: String,
    pub fips_code: Option<String>,
    pub regulator_code: Option<String>,
}
entity_record!(CountyV1);

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct OperatorV1 {
    #[serde(flatten)]
    pub source: SourceEnvelopeV1,
    pub regulator_operator_number: Option<String>,
    pub display_name: String,
    pub normalized_name: String,
    pub status: Option<String>,
}
entity_record!(OperatorV1);

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RegulatoryAreaV1 {
    #[serde(flatten)]
    pub source: SourceEnvelopeV1,
    pub area_type: String,
    pub code: Option<String>,
    pub name: String,
    pub parent_area_source_key: Option<String>,
    pub county_source_key: Option<String>,
}
entity_record!(RegulatoryAreaV1);

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct WellV1 {
    #[serde(flatten)]
    pub source: SourceEnvelopeV1,
    pub normalized_api_number: Option<String>,
    pub displayed_api_number: Option<String>,
    pub well_name: Option<String>,
    pub well_number: Option<String>,
    pub operator_source_key: Option<String>,
    pub county_source_key: Option<String>,
    pub current_status: Option<String>,
    pub well_type: Option<String>,
    pub spud_date: Option<NaiveDate>,
    pub completion_date: Option<NaiveDate>,
    pub first_production_date: Option<NaiveDate>,
    pub plug_date: Option<NaiveDate>,
    pub surface_latitude: Option<String>,
    pub surface_longitude: Option<String>,
    pub bottom_hole_latitude: Option<String>,
    pub bottom_hole_longitude: Option<String>,
}
entity_record!(WellV1);

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct WellIdentifierV1 {
    #[serde(flatten)]
    pub source: SourceEnvelopeV1,
    pub well_source_key: String,
    pub identifier_type: String,
    pub value: String,
    pub normalized_value: String,
    pub is_primary: bool,
}
entity_record!(WellIdentifierV1);

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct WellGeometryV1 {
    #[serde(flatten)]
    pub source: SourceEnvelopeV1,
    pub well_source_key: String,
    pub geometry_type: String,
    pub geojson: Value,
    pub min_latitude: Option<String>,
    pub min_longitude: Option<String>,
    pub max_latitude: Option<String>,
    pub max_longitude: Option<String>,
}
entity_record!(WellGeometryV1);

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct WellboreV1 {
    #[serde(flatten)]
    pub source: SourceEnvelopeV1,
    #[serde(default)]
    pub well_source_key: Option<String>,
    #[serde(default)]
    pub well_identifier_type: Option<String>,
    #[serde(default)]
    pub well_identifier_value: Option<String>,
    #[serde(default)]
    pub normalized_well_identifier: Option<String>,
    #[serde(default)]
    pub display_name: Option<String>,
    #[serde(default)]
    pub wellbore_number: Option<String>,
    #[serde(default)]
    pub parent_wellbore_source_key: Option<String>,
    pub bore_role: String,
    pub trajectory_kind: String,
    #[serde(default)]
    pub is_lateral: Option<bool>,
    #[serde(default)]
    pub classification_method: Option<String>,
    #[serde(default)]
    pub geometry_quality: Option<String>,
    pub status: Option<String>,
    #[serde(default)]
    pub reported_measured_depth_ft: Option<String>,
    #[serde(default)]
    pub reported_true_vertical_depth_ft: Option<String>,
    #[serde(default)]
    pub reported_lateral_length_ft: Option<String>,
}
entity_record!(WellboreV1);

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct WellboreGeometryV1 {
    #[serde(flatten)]
    pub source: SourceEnvelopeV1,
    pub wellbore_source_key: String,
    pub geometry_type: String,
    pub geojson: Value,
    #[serde(default)]
    pub geometry_quality: Option<String>,
    pub min_latitude: Option<String>,
    pub min_longitude: Option<String>,
    pub max_latitude: Option<String>,
    pub max_longitude: Option<String>,
}
entity_record!(WellboreGeometryV1);

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RegulatoryRecordV1 {
    #[serde(flatten)]
    pub source: SourceEnvelopeV1,
    pub well_source_key: Option<String>,
    pub original_well_source_key: Option<String>,
    pub record_type: String,
    pub record_subtype: Option<String>,
    pub effective_at: Option<DateTime<Utc>>,
    pub filed_at: Option<DateTime<Utc>>,
    pub status: Option<String>,
    pub title: Option<String>,
    pub summary: Option<String>,
    pub relationship_resolution_status: String,
}
entity_record!(RegulatoryRecordV1);

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct DocumentV1 {
    #[serde(flatten)]
    pub source: SourceEnvelopeV1,
    pub well_source_key: Option<String>,
    pub original_well_source_key: Option<String>,
    pub document_type: Option<String>,
    pub title: Option<String>,
    pub document_number: Option<String>,
    pub filing_date: Option<NaiveDate>,
    pub effective_date: Option<NaiveDate>,
    pub source_url: Option<String>,
    pub object_storage_key: Option<String>,
    pub mime_type: Option<String>,
    pub content_checksum: Option<String>,
    pub page_count: Option<i32>,
    pub relationship_resolution_status: String,
}
entity_record!(DocumentV1);

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct DocumentContentV1 {
    #[serde(flatten)]
    pub source: SourceEnvelopeV1,
    pub document_source_key: String,
    pub content_type: String,
    pub text_content: Option<String>,
    pub structured_content: Value,
    pub content_checksum: Option<String>,
    pub extraction_version: Option<String>,
}
entity_record!(DocumentContentV1);

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct EventV1 {
    #[serde(flatten)]
    pub source: SourceEnvelopeV1,
    pub well_source_key: Option<String>,
    pub original_well_source_key: Option<String>,
    pub event_type: String,
    pub event_subtype: Option<String>,
    pub event_at: Option<DateTime<Utc>>,
    pub published_at: Option<DateTime<Utc>>,
    pub title: Option<String>,
    pub summary: Option<String>,
    pub relationship_resolution_status: String,
}
entity_record!(EventV1);

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct EventDocumentLinkV1 {
    #[serde(flatten)]
    pub source: SourceEnvelopeV1,
    pub event_source_key: String,
    pub document_source_key: String,
    pub relation_type: String,
    pub relationship_resolution_status: String,
}
entity_record!(EventDocumentLinkV1);

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ProductionMonthlyV1 {
    #[serde(flatten)]
    pub source: SourceEnvelopeV1,
    pub well_source_key: Option<String>,
    pub original_well_source_key: Option<String>,
    pub operator_source_key: Option<String>,
    pub production_month: NaiveDate,
    pub oil_volume: Option<String>,
    pub gas_volume: Option<String>,
    pub water_volume: Option<String>,
    pub days_produced: Option<String>,
    pub source_allocation_type: Option<String>,
    pub revision_status: Option<String>,
    pub relationship_resolution_status: String,
}
entity_record!(ProductionMonthlyV1);

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TombstoneV1 {
    #[serde(flatten)]
    pub source: SourceEnvelopeV1,
    pub entity_type: String,
    pub entity_source_key: String,
    pub deleted_at: Option<DateTime<Utc>>,
    pub reason: Option<String>,
}
entity_record!(TombstoneV1);

pub fn reject_unsupported_version(version: &str) -> ContractResult<()> {
    if version == SCHEMA_VERSION {
        Ok(())
    } else {
        Err(ContractError::UnsupportedSchemaVersion(version.to_string()))
    }
}

fn require_value(entity: &'static str, value: &str) -> ContractResult<()> {
    if value.trim().is_empty() {
        Err(ContractError::MissingSourceKey { entity })
    } else {
        Ok(())
    }
}
