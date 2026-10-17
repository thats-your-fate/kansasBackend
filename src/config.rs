use std::env;

/// Kansas-specific configuration namespace. All secrets and environment
/// values come from the process environment (typically loaded from `.env`
/// in local development); nothing here hardcodes a credential. The
/// `future_wells_kansas` database name appears only as a documentation
/// example, never as a fallback value.
#[derive(Debug, Clone)]
pub struct Config {
    /// Postgres connection string. No default — must be set externally.
    /// Local dev convention: `postgresql://<user>:<pass>@<host>:5432/future_wells_kansas`.
    pub database_url: String,
    pub api_host: String,
    pub api_port: u16,
    /// Local root for archived source downloads (source_artifact.local_path
    /// base, once the source/ ingestion layer lands per KS-004).
    pub source_archive_root: String,
    pub log_level: String,
    /// KS-10's explicit legal-review escape hatch
    /// (`constants::PRE_1990_LEGAL_REVIEW_OVERRIDE_ENV`) — must be
    /// `"true"` exactly to enable; anything else (including unset) keeps
    /// the pre-1990 production block in force.
    pub pre_1990_legal_review_override_enabled: bool,
    /// Official KGS/KCC source URLs — see `SourceUrls` doc comment. Every
    /// importer must read a URL from here, never inline one of its own.
    pub source_urls: SourceUrls,
}

/// Official KGS/KCC source endpoints, seeded from the KS-002 discovery
/// pass. Every field is env-overridable (falling back to the confirmed
/// value below) so a source URL that moves can be fixed by redeploying
/// config, not shipping a code change — this is the one place these URLs
/// are allowed to live; importer code must call into this struct rather
/// than declaring a URL literal of its own.
#[derive(Debug, Clone)]
pub struct SourceUrls {
    // --- KGS: well master ---
    /// Catalog/entry-point page listing the current well-download options.
    pub kgs_wells_catalog: String,
    /// ArcGIS Online item ID for the "Oil and Gas Wells Download" hosted
    /// layer. Item IDs are stable; the FeatureServer URL they resolve to
    /// can change if KGS republishes the layer — resolve this at runtime
    /// via `source::arcgis::resolve_service_url` rather than trusting
    /// `kgs_wells_arcgis_layer` as anything but a fallback.
    pub kgs_wells_arcgis_item_id: String,
    /// Pinned FeatureServer layer URL, last known good. Fallback only —
    /// used when runtime item-ID resolution is impractical (offline dev,
    /// ArcGIS Online unreachable), never preferred over a fresh resolve.
    pub kgs_wells_arcgis_layer: String,
    /// Static full-extract ZIP of the well master, refreshed nightly.
    pub kgs_wells_static_zip: String,
    /// Documented file/column format for the well master export.
    pub kgs_wells_format: String,

    // --- KGS: leases ---
    pub kgs_leases_catalog: String,
    pub kgs_all_leases_zip: String,
    pub kgs_lease_format: String,

    // --- KGS: production ---
    /// Base URL to join with `constants::KGS_PRODUCTION_FILES` filenames.
    pub kgs_production_archive_base: String,

    // --- KGS: fields ---
    pub kgs_fields_catalog: String,
    /// ArcGIS Online item ID for the "Oil_and_Gas_Fields" hosted layer,
    /// confirmed live 2026-09-05 (resolves via the sharing API the same
    /// way `kgs_wells_arcgis_item_id` does).
    pub kgs_fields_arcgis_item_id: String,
    /// Pinned fallback FeatureServer layer URL, last known good.
    pub kgs_fields_arcgis_layer: String,
    /// Base directory for the field-production `.xlsx` files
    /// (`constants::KGS_FIELD_ACTIVE_FILES`) and the abandoned-fields file.
    pub kgs_field_files_base: String,
    /// The one abandoned-fields file (lifetime totals, no per-year split —
    /// a genuinely different shape from the active-field files, confirmed
    /// live 2026-09-05).
    pub kgs_field_abandoned_file: String,

    // --- KGS: horizontal wells (ORDS/Oracle APEX endpoint) ---
    pub kgs_horizontal_wells: String,

    // --- KGS: well documents (KS-15) ---
    /// Real, confirmed-live per-well "Qualified Well Database" detail
    /// page (`?f_kid=<KID>` appended by the caller) — the same ORDS
    /// family as `kgs_horizontal_wells` above. Confirmed 2026-09-05 via
    /// direct fetch of a real well (KID 1001190703); see
    /// `docs/ops/ks-15-kgs-documents-live-smoke.md`.
    pub kgs_well_detail_page: String,

    // --- KCC ---
    pub kcc_data_search: String,
    pub kcc_intents: String,
    pub kcc_operators: String,
    pub kcc_transfers: String,
    /// Form-driven results endpoint behind `kcc_transfers`.
    pub kcc_transfer_results: String,
    pub kcc_forms: String,
    pub kcc_conservation_dockets: String,
    /// KCC-Connect e-filing portal. Authoritative for any docket filed
    /// after November 2025 — never build new-filing ingestion against
    /// legacy eStar.
    pub kcc_connect: String,
}

impl SourceUrls {
    pub fn from_env() -> Self {
        fn var(key: &str, default: &str) -> String {
            env::var(key).unwrap_or_else(|_| default.to_string())
        }

        SourceUrls {
            kgs_wells_catalog: var("KGS_WELLS_CATALOG", "https://www.kgs.ku.edu/PRS/petroDB.html"),
            kgs_wells_arcgis_item_id: var("KGS_WELLS_ARCGIS_ITEM_ID", "514428df866b46c893b6834ff0bf303d"),
            kgs_wells_arcgis_layer: var(
                "KGS_WELLS_ARCGIS_LAYER",
                "https://services2.arcgis.com/ZOdjAzAQ2B0f85zi/ArcGIS/rest/services/Oil_and_Gas_Wells_Download/FeatureServer/0",
            ),
            kgs_wells_static_zip: var("KGS_WELLS_STATIC_ZIP", "https://www.kgs.ku.edu/PRS/Ora_Archive/ks_wells.zip"),
            kgs_wells_format: var("KGS_WELLS_FORMAT", "https://www.kgs.ku.edu/PRS/file_format.html"),

            kgs_leases_catalog: var("KGS_LEASES_CATALOG", "https://www.kgs.ku.edu/Magellan/Field/lease.html"),
            kgs_all_leases_zip: var(
                "KGS_ALL_LEASES_ZIP",
                "https://www.kgs.ku.edu/PRS/Ora_Archive/all_kansas_leases.zip",
            ),
            kgs_lease_format: var(
                "KGS_LEASE_FORMAT",
                "https://www.kgs.ku.edu/Magellan/Field/lease_file_format.html",
            ),

            kgs_production_archive_base: var(
                "KGS_PRODUCTION_ARCHIVE_BASE",
                "https://www.kgs.ku.edu/PRS/Ora_Archive/",
            ),

            kgs_fields_catalog: var("KGS_FIELDS_CATALOG", "https://www.kgs.ku.edu/Magellan/Field/index.html"),
            kgs_fields_arcgis_item_id: var("KGS_FIELDS_ARCGIS_ITEM_ID", "390ee1b9eade40bca42ee3534a813b20"),
            kgs_fields_arcgis_layer: var(
                "KGS_FIELDS_ARCGIS_LAYER",
                "https://services2.arcgis.com/ZOdjAzAQ2B0f85zi/arcgis/rest/services/Oil_and_Gas_Fields/FeatureServer/0",
            ),
            kgs_field_files_base: var("KGS_FIELD_FILES_BASE", "https://www.kgs.ku.edu/Magellan/Field/"),
            kgs_field_abandoned_file: var(
                "KGS_FIELD_ABANDONED_FILE",
                "https://www.kgs.ku.edu/Magellan/Field/fields_abandoned.xlsx",
            ),

            kgs_horizontal_wells: var(
                "KGS_HORIZONTAL_WELLS",
                "https://chasm.kgs.ku.edu/ords/qualified.ogw5.HorizWells",
            ),

            kgs_well_detail_page: var(
                "KGS_WELL_DETAIL_PAGE",
                "https://chasm.kgs.ku.edu/ords/qualified.well_page.DisplayWell",
            ),

            kcc_data_search: var("KCC_DATA_SEARCH", "https://www.kcc.ks.gov/oil-gas/data-search"),
            kcc_intents: var("KCC_INTENTS", "https://www.kcc.ks.gov/notices-of-intents-search"),
            kcc_operators: var("KCC_OPERATORS", "https://www.kcc.ks.gov/kansas-oil-operators"),
            kcc_transfers: var(
                "KCC_TRANSFERS",
                "https://www.kcc.ks.gov/notices-of-transfers-approved-search",
            ),
            kcc_transfer_results: var("KCC_TRANSFER_RESULTS", "https://www.kcc.ks.gov/transfersSearch.php"),
            kcc_forms: var("KCC_FORMS", "https://www.kcc.ks.gov/oil-gas/oil-gas-forms"),
            kcc_conservation_dockets: var(
                "KCC_CONSERVATION_DOCKETS",
                "https://www.kcc.ks.gov/oil-gas/conservation-dockets",
            ),
            kcc_connect: var("KCC_CONNECT", "https://kcc-connect.kcc.ks.gov/"),
        }
    }

    /// Looks up a URL by the `SourceDescriptor.id` values in
    /// `constants::SOURCES`, for display/CLI purposes. Returns the pinned
    /// fallback for `ks_kgs_wells_arcgis` — callers that need the current
    /// service URL should prefer `source::arcgis::resolve_service_url`.
    pub fn get(&self, id: &str) -> Option<&str> {
        Some(match id {
            "ks_kgs_wells_catalog" => &self.kgs_wells_catalog,
            "ks_kgs_wells_arcgis" => &self.kgs_wells_arcgis_layer,
            "ks_kgs_wells_static_zip" => &self.kgs_wells_static_zip,
            "ks_kgs_wells_format" => &self.kgs_wells_format,
            "ks_kgs_leases_catalog" => &self.kgs_leases_catalog,
            "ks_kgs_all_leases_zip" => &self.kgs_all_leases_zip,
            "ks_kgs_lease_format" => &self.kgs_lease_format,
            "ks_kgs_production_archive" => &self.kgs_production_archive_base,
            "ks_kgs_fields_catalog" => &self.kgs_fields_catalog,
            "ks_kgs_fields_arcgis" => &self.kgs_fields_arcgis_layer,
            "ks_kgs_field_files_base" => &self.kgs_field_files_base,
            "ks_kgs_field_abandoned_file" => &self.kgs_field_abandoned_file,
            "ks_kgs_horizontal_wells" => &self.kgs_horizontal_wells,
            "ks_kgs_well_detail_page" => &self.kgs_well_detail_page,
            "ks_kcc_data_search" => &self.kcc_data_search,
            "ks_kcc_intents" => &self.kcc_intents,
            "ks_kcc_operators" => &self.kcc_operators,
            "ks_kcc_transfers" => &self.kcc_transfers,
            "ks_kcc_transfer_results" => &self.kcc_transfer_results,
            "ks_kcc_forms" => &self.kcc_forms,
            "ks_kcc_conservation_dockets" => &self.kcc_conservation_dockets,
            "ks_kcc_connect" => &self.kcc_connect,
            _ => return None,
        })
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("missing required environment variable: {0}")]
    MissingVar(&'static str),
    #[error("invalid value for {name}: {value}")]
    InvalidValue { name: &'static str, value: String },
}

impl Config {
    /// Loads configuration from the process environment. Does not read or
    /// parse `.env` itself — call `dotenvy::dotenv().ok()` before invoking
    /// this in local development, as the sibling state backends do.
    pub fn from_env() -> Result<Self, ConfigError> {
        let database_url = env::var("DATABASE_URL")
            .or_else(|_| env::var("PG_CONNECTION"))
            .map_err(|_| ConfigError::MissingVar("DATABASE_URL"))?;

        let api_host = env::var("KS_API_HOST").unwrap_or_else(|_| "0.0.0.0".to_string());

        let api_port = match env::var("KS_API_PORT") {
            Ok(v) => v.parse::<u16>().map_err(|_| ConfigError::InvalidValue {
                name: "KS_API_PORT",
                value: v,
            })?,
            Err(_) => 4008,
        };

        let source_archive_root =
            env::var("KS_SOURCE_ARCHIVE_ROOT").unwrap_or_else(|_| "./data/kansas".to_string());

        let log_level = env::var("KS_LOG_LEVEL").unwrap_or_else(|_| "info".to_string());

        let pre_1990_legal_review_override_enabled =
            env::var(crate::constants::PRE_1990_LEGAL_REVIEW_OVERRIDE_ENV)
                .map(|v| v == "true")
                .unwrap_or(false);

        Ok(Config {
            database_url,
            api_host,
            api_port,
            source_archive_root,
            log_level,
            pre_1990_legal_review_override_enabled,
            source_urls: SourceUrls::from_env(),
        })
    }
}
