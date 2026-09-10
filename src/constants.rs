//! Single registry of Kansas state constants and official source IDs,
//! transcribed from `future_wells_kansas_backend_marketing_codex_tasks.md`
//! section 2 (official source baseline). Source *URLs* live in
//! `config::SourceUrls` (env-overridable) — this registry carries only
//! stable identity/description/category metadata; importer code must
//! resolve a source's actual URL from `Config::source_urls`, never
//! hardcode one of its own.

pub const STATE_KEY: &str = "KS";
pub const STATE_NAME: &str = "Kansas";

/// American Petroleum Institute state code embedded in the 10/14-digit API
/// well number (e.g. `15-171-20701...`). This is NOT the Census FIPS code
/// (which is `20` for Kansas) — the two numbering systems are unrelated
/// and must never be conflated in parsing code.
pub const API_STATE_PREFIX: &str = "15";

pub fn api_digits_belong_to_state(digits: &str) -> bool {
    (digits.len() == 10 || digits.len() == 14) && digits.starts_with(API_STATE_PREFIX)
}

/// The eight fixed KGS production archive filenames, joined onto
/// `Config::source_urls.kgs_production_archive_base` to build each
/// download URL. Filenames are KGS's own naming, not derived — do not
/// try to generate them programmatically from a year range.
pub const KGS_PRODUCTION_FILES: &[&str] = &[
    "oil_leases_2020_present.zip",
    "gas_leases_2020_present.zip",
    "oil_leases_2010_2019.zip",
    "gas_leases_2010_2019.zip",
    "oil_leases_2000_2009.zip",
    "gas_leases_2000_2009.zip",
    "oil_leases_1990_1999.zip",
    "gas_leases_1990_1999.zip",
];

/// Joins `base` with each entry in `KGS_PRODUCTION_FILES`. `base` should
/// come from `Config::source_urls.kgs_production_archive_base`.
pub fn production_file_urls(base: &str) -> Vec<String> {
    let base = base.trim_end_matches('/');
    KGS_PRODUCTION_FILES
        .iter()
        .map(|file| format!("{base}/{file}"))
        .collect()
}

/// KS-08: the six "currently published active-field production" files
/// listed on the KGS field index page (confirmed live 2026-09-05 by
/// fetching `https://www.kgs.ku.edu/Magellan/Field/index.html`), joined
/// onto `Config::source_urls.kgs_field_files_base`. Real, confirmed
/// filenames — not derived from a year-range pattern.
pub const KGS_FIELD_ACTIVE_FILES: &[&str] = &[
    "fields_2020_present.xlsx",
    "fields_2010_2019.xlsx",
    "fields_2000_2009.xlsx",
    "fields_1990_1999.xlsx",
    "fields_1980_1989.xlsx",
    "fields_1970_1979.xlsx",
];

/// Joins `base` with each entry in `KGS_FIELD_ACTIVE_FILES`.
pub fn field_active_file_urls(base: &str) -> Vec<String> {
    let base = base.trim_end_matches('/');
    KGS_FIELD_ACTIVE_FILES
        .iter()
        .map(|file| format!("{base}/{file}"))
        .collect()
}

/// KS-08's explicit, hard-required disclaimer: field polygons describe
/// KGS's own research boundaries, never a legal interest. Every
/// public-facing field response must carry this text — not a comment
/// someone has to remember to add.
pub const FIELD_GEOMETRY_WARNING: &str =
    "Field boundaries are contextual, research-oriented approximations maintained by the Kansas Geological Survey. They are not legal lease, mineral, survey, title, or ownership boundaries.";

/// KS-09's explicit, hard-required disclaimer: the lease dimension and
/// well-to-lease links are identity cross-references only. Every
/// public-facing lease response must carry this text.
pub const LEASE_OWNERSHIP_DISCLAIMER: &str =
    "Lease records identify wells and production reporting relationships only. They do not establish or imply surface, mineral, royalty, or working-interest ownership.";

/// KS-02: the 12 trackable KGS/KCC feeds, matching `ks_source.code` rows
/// exactly (migration `0002_ks_source_registry.sql`). Ingestion code must
/// reference these constants, never a raw string literal, so a typo in a
/// source code fails to compile instead of silently matching nothing at
/// query time. This is a narrower, ingestion-relevant subset of `SOURCES`
/// above (which also lists catalog/format/discovery-only pages that
/// aren't themselves ingestible datasets).
pub const KGS_WELLS_ARCGIS: &str = "KGS_WELLS_ARCGIS";
pub const KGS_WELLS_ZIP: &str = "KGS_WELLS_ZIP";
pub const KGS_FIELDS: &str = "KGS_FIELDS";
pub const KGS_LEASES: &str = "KGS_LEASES";
pub const KGS_PRODUCTION_OIL: &str = "KGS_PRODUCTION_OIL";
pub const KGS_PRODUCTION_GAS: &str = "KGS_PRODUCTION_GAS";
pub const KGS_DOCUMENTS: &str = "KGS_DOCUMENTS";
pub const KCC_OPERATORS: &str = "KCC_OPERATORS";
pub const KCC_INTENTS: &str = "KCC_INTENTS";
pub const KCC_TRANSFERS: &str = "KCC_TRANSFERS";
pub const KCC_CONSERVATION_DOCKETS: &str = "KCC_CONSERVATION_DOCKETS";
pub const KCC_KOLAR_DOCUMENTS: &str = "KCC_KOLAR_DOCUMENTS";

pub const INGESTION_SOURCE_CODES: &[&str] = &[
    KGS_WELLS_ARCGIS,
    KGS_WELLS_ZIP,
    KGS_FIELDS,
    KGS_LEASES,
    KGS_PRODUCTION_OIL,
    KGS_PRODUCTION_GAS,
    KGS_DOCUMENTS,
    KCC_OPERATORS,
    KCC_INTENTS,
    KCC_TRANSFERS,
    KCC_CONSERVATION_DOCKETS,
    KCC_KOLAR_DOCUMENTS,
];

/// KS-03: Kansas membership classification. KGS's own well-master
/// description notes "some data from other states is included for
/// research purposes" — every raw record must be classified before it's
/// treated as a Kansas well, per one of two rules:
///   1. a valid Kansas API state prefix (`API_STATE_PREFIX`, "15"), OR
///   2. WGS84 geometry inside the Kansas bounding box AND a recognized
///      Kansas county name.
///
/// Records failing both are retained in `ks_well_raw_snapshot` (never
/// deleted) with `kansas_membership_status = 'excluded_other_state'` and a
/// reason — see `src/parse/kgs_wells/membership.rs`.
///
/// This bounding box (not a precise state-boundary polygon) is a
/// deliberate scope decision: Kansas has no significant diagonal or
/// irregular border segments that a rectangular box would misclassify
/// against a neighboring state at meaningful volume, and pairing it with
/// the county-name check (Kansas counties don't share names with
/// out-of-state counties in this dataset's COUNTY field in practice)
/// keeps the false-positive rate low without bundling a real polygon
/// shapefile dependency. Revisit with a real polygon if false positives
/// near the border turn out to matter in practice.
pub const KS_BOUNDING_BOX_MIN_LATITUDE: f64 = 36.99;
pub const KS_BOUNDING_BOX_MAX_LATITUDE: f64 = 40.01;
pub const KS_BOUNDING_BOX_MIN_LONGITUDE: f64 = -102.06;
pub const KS_BOUNDING_BOX_MAX_LONGITUDE: f64 = -94.59;

pub fn point_in_kansas_bounding_box(latitude: f64, longitude: f64) -> bool {
    (KS_BOUNDING_BOX_MIN_LATITUDE..=KS_BOUNDING_BOX_MAX_LATITUDE).contains(&latitude)
        && (KS_BOUNDING_BOX_MIN_LONGITUDE..=KS_BOUNDING_BOX_MAX_LONGITUDE).contains(&longitude)
}

/// All 105 Kansas counties (a fixed, unchanging enumeration — Kansas has
/// had exactly 105 counties since 1893). Matched case-insensitively
/// against the source `COUNTY` field, which is a plain name with no FIPS
/// code in either KGS source (ArcGIS layer or the ZIP export).
pub const KS_COUNTIES: &[&str] = &[
    "Allen",
    "Anderson",
    "Atchison",
    "Barber",
    "Barton",
    "Bourbon",
    "Brown",
    "Butler",
    "Chase",
    "Chautauqua",
    "Cherokee",
    "Cheyenne",
    "Clark",
    "Clay",
    "Cloud",
    "Coffey",
    "Comanche",
    "Cowley",
    "Crawford",
    "Decatur",
    "Dickinson",
    "Doniphan",
    "Douglas",
    "Edwards",
    "Elk",
    "Ellis",
    "Ellsworth",
    "Finney",
    "Ford",
    "Franklin",
    "Geary",
    "Gove",
    "Graham",
    "Grant",
    "Gray",
    "Greeley",
    "Greenwood",
    "Hamilton",
    "Harper",
    "Harvey",
    "Haskell",
    "Hodgeman",
    "Jackson",
    "Jefferson",
    "Jewell",
    "Johnson",
    "Kearny",
    "Kingman",
    "Kiowa",
    "Labette",
    "Lane",
    "Leavenworth",
    "Lincoln",
    "Linn",
    "Logan",
    "Lyon",
    "Marion",
    "Marshall",
    "McPherson",
    "Meade",
    "Miami",
    "Mitchell",
    "Montgomery",
    "Morris",
    "Morton",
    "Nemaha",
    "Neosho",
    "Ness",
    "Norton",
    "Osage",
    "Osborne",
    "Ottawa",
    "Pawnee",
    "Phillips",
    "Pottawatomie",
    "Pratt",
    "Rawlins",
    "Reno",
    "Republic",
    "Rice",
    "Riley",
    "Rooks",
    "Rush",
    "Russell",
    "Saline",
    "Scott",
    "Sedgwick",
    "Seward",
    "Shawnee",
    "Sheridan",
    "Sherman",
    "Smith",
    "Stafford",
    "Stanton",
    "Stevens",
    "Sumner",
    "Thomas",
    "Trego",
    "Wabaunsee",
    "Wallace",
    "Washington",
    "Wichita",
    "Wilson",
    "Woodson",
    "Wyandotte",
];

pub fn is_valid_kansas_county(name: &str) -> bool {
    let trimmed = name.trim();
    KS_COUNTIES
        .iter()
        .any(|county| county.eq_ignore_ascii_case(trimmed))
}

/// KGS licenses pre-1987 production from IHS; it may not be repackaged,
/// resold, or disseminated. For a clean MVP this project only ever ingests
/// the 1990-present archives (see `KGS_PRODUCTION_FILES`, which already
/// excludes anything older) — this constant is the single place that
/// boundary is asserted, and every future downloader must check it before
/// requesting a production file, not just avoid listing older filenames.
pub const MIN_PRODUCTION_YEAR: i32 = 1990;

/// KS-10: the explicit escape hatch the task doc requires — "reject any
/// configured input containing a year earlier than 1990 unless a
/// separate future legal-review feature flag is explicitly enabled."
/// Reads as a boolean env var; unset/anything other than `"true"` means
/// disabled. Deliberately not read via `config::SourceUrls` — this isn't
/// a source URL, it's a legal/compliance gate, kept in its own
/// unmistakably-named variable.
pub const PRE_1990_LEGAL_REVIEW_OVERRIDE_ENV: &str = "KS_PRE_1990_LEGAL_REVIEW_OVERRIDE";

pub fn is_production_year_ingestible(year: i32, legal_review_override_enabled: bool) -> bool {
    year >= MIN_PRODUCTION_YEAR || legal_review_override_enabled
}

/// KGS's production month field overloads two sentinel values onto what
/// is otherwise a 1-12 calendar month: `0` means "this row is a yearly
/// total," `-1` means "this row is a starting cumulative value." Both are
/// real KGS record kinds, not malformed data, and must never be mixed
/// into a monthly production series.
pub const PRODUCTION_MONTH_YEARLY_TOTAL: i16 = 0;
pub const PRODUCTION_MONTH_STARTING_CUMULATIVE: i16 = -1;

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ProductionRecordKind {
    Monthly,
    YearlyTotal,
    StartingCumulative,
}

/// Classifies a raw KGS production-month value. Returns `None` for
/// anything outside `{-1, 0, 1..=12}` so unexpected values surface as a
/// parse/validation failure rather than being silently folded into
/// `Monthly`.
pub fn classify_production_month(month: i16) -> Option<ProductionRecordKind> {
    match month {
        PRODUCTION_MONTH_YEARLY_TOTAL => Some(ProductionRecordKind::YearlyTotal),
        PRODUCTION_MONTH_STARTING_CUMULATIVE => Some(ProductionRecordKind::StartingCumulative),
        1..=12 => Some(ProductionRecordKind::Monthly),
        _ => None,
    }
}

/// `source_identity_confidence` from the Kansas identity model (task doc
/// §3.1): how sure a well/lease-alias resolution is, never left implicit.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SourceIdentityConfidence {
    Exact,
    Probable,
    Unresolved,
}

/// Shared Future Wells lifecycle value, derived from KGS's raw
/// `WELL_CLASS`/`STATUS`/`STATUS2` triple (task doc §3.5). The mapping
/// from raw codes to this enum is implemented at KS-005 once the file-
/// format doc's coded values are confirmed — this enum fixes the target
/// shape now. `Unknown` means "this raw combination hasn't been mapped
/// yet," and must surface for human review (a QA report over materialized
/// wells), never be treated as a synonym for `Inactive`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum WellLifecycleStatus {
    Planned,
    IntentExpired,
    Drilled,
    Completed,
    Producing,
    Inactive,
    Injection,
    SaltwaterDisposal,
    Plugged,
    DryAndAbandoned,
    Unknown,
}

/// A single official source, as catalogued in the task planning document
/// section 2. `id` is a lookup key into `config::SourceUrls::get` — this
/// struct never carries a URL itself, so a source's endpoint can move via
/// configuration without a code change.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SourceDescriptor {
    pub id: &'static str,
    pub description: &'static str,
    pub category: SourceCategory,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SourceCategory {
    /// Kansas Geological Survey: well master, leases/production, fields —
    /// the statewide well/production foundation (plan §1 rows "Well
    /// master", "Leases and production", "Fields").
    KgsFoundation,
    /// Kansas Corporation Commission: operators, drilling intents,
    /// approved transfers, conservation dockets — the time-sensitive
    /// regulatory signal (plan §1 rows "Operators", "Drilling intents",
    /// "Approved transfers", "Conservation dockets").
    KccRegulatory,
}

pub const SOURCES: &[SourceDescriptor] = &[
    SourceDescriptor {
        id: "ks_kgs_wells_catalog",
        description: "KGS petroleum database catalog page — entry point listing the current well-download options (ArcGIS layer, static ZIP, file-format doc).",
        category: SourceCategory::KgsFoundation,
    },
    SourceDescriptor {
        id: "ks_kgs_wells_arcgis",
        description: "KGS's hosted ArcGIS FeatureServer layer for the well master (item id in kgs_wells_arcgis_item_id — resolve at runtime via source::arcgis, this URL is the pinned fallback only). ~519,092 records, point geometry, 2,000-record query cap; some out-of-state records may be included.",
        category: SourceCategory::KgsFoundation,
    },
    SourceDescriptor {
        id: "ks_kgs_wells_static_zip",
        description: "Static full-extract ZIP of the well master: API, KID, names, legal location, field, operator, dates, status, formation, IP, lease-link IDs. Refreshed nightly; prefer this over the ArcGIS layer for a full bulk sync.",
        category: SourceCategory::KgsFoundation,
    },
    SourceDescriptor {
        id: "ks_kgs_wells_format",
        description: "KGS's documented file/column format for the well master export — read before writing the parser.",
        category: SourceCategory::KgsFoundation,
    },
    SourceDescriptor {
        id: "ks_kgs_leases_catalog",
        description: "KGS Magellan lease catalog page — entry point for lease entities.",
        category: SourceCategory::KgsFoundation,
    },
    SourceDescriptor {
        id: "ks_kgs_all_leases_zip",
        description: "Static full-extract ZIP of all Kansas lease entities.",
        category: SourceCategory::KgsFoundation,
    },
    SourceDescriptor {
        id: "ks_kgs_lease_format",
        description: "KGS's documented file/column format for the lease ZIP.",
        category: SourceCategory::KgsFoundation,
    },
    SourceDescriptor {
        id: "ks_kgs_production_archive",
        description: "Base URL for KGS's monthly lease/production ZIP archive (received from the Kansas Department of Revenue); combine with KGS_PRODUCTION_FILES for the 8 oil/gas x era files. Production is lease-level, not well-level; the reported contributing-well count is limited quality.",
        category: SourceCategory::KgsFoundation,
    },
    SourceDescriptor {
        id: "ks_kgs_fields_catalog",
        description: "KGS Magellan field-database catalog page — entry point for field entities, GIS, active/abandoned field production files. Field geometry is context, not a legal mineral, lease, or title boundary.",
        category: SourceCategory::KgsFoundation,
    },
    SourceDescriptor {
        id: "ks_kgs_horizontal_wells",
        description: "KGS's Oracle APEX (ORDS) endpoint listing horizontal wells specifically — narrower than the general well master.",
        category: SourceCategory::KgsFoundation,
    },
    SourceDescriptor {
        id: "ks_kcc_data_search",
        description: "KCC's general oil & gas data-search entry point.",
        category: SourceCategory::KccRegulatory,
    },
    SourceDescriptor {
        id: "ks_kcc_intents",
        description: "KCC notice-of-intent-to-drill search. Rolling 365-day window; new intents generally posted around 6pm Central. Must snapshot daily — records disappear once the window rolls. Do not extract surface-owner/contact data for commercial use.",
        category: SourceCategory::KccRegulatory,
    },
    SourceDescriptor {
        id: "ks_kcc_operators",
        description: "KCC's Kansas oil operators list/search. Never expose or index addresses, contact people, or phone numbers for lead generation — KCC restricts commercial-solicitation use of names and addresses.",
        category: SourceCategory::KccRegulatory,
    },
    SourceDescriptor {
        id: "ks_kcc_transfers",
        description: "KCC notices-of-transfers-approved search. Rolling 90-day window; daily snapshots required to preserve history. Well matching is sometimes probabilistic since the listing centers on operator, lease, PLSS, and dates.",
        category: SourceCategory::KccRegulatory,
    },
    SourceDescriptor {
        id: "ks_kcc_transfer_results",
        description: "Form-driven results endpoint behind the transfers search page — the actual query target once the search form's parameters are known.",
        category: SourceCategory::KccRegulatory,
    },
    SourceDescriptor {
        id: "ks_kcc_forms",
        description: "KCC's oil & gas forms index (C-1, ACO-1, T-1, CP-1/CP-4, etc.) — reference for document-type metadata, not a data feed itself.",
        category: SourceCategory::KccRegulatory,
    },
    SourceDescriptor {
        id: "ks_kcc_conservation_dockets",
        description: "KCC's conservation-dockets page: daily active-docket/recent-action surface for horizontal-well, flaring, plugging, UIC, new-pool and exception signals.",
        category: SourceCategory::KccRegulatory,
    },
    SourceDescriptor {
        id: "ks_kcc_connect",
        description: "KCC-Connect, KCC's e-filing portal. KCC moved post-November-2025 conservation-docket filings here — do not build new-filing ingestion against legacy eStar.",
        category: SourceCategory::KccRegulatory,
    },
];

/// KS-14: the date Kansas activity-event monitoring went live. Fixed
/// (not env-configurable) deliberately: `should_emit_event` is called
/// from inside the wells/production/intents/transfers materializers,
/// which run as part of the same test binary as everything else in this
/// repo — an env-var-based cutoff would be process-global mutable state,
/// unsafe to change from a `#[tokio::test]` that runs concurrently with
/// every other test in the suite. A fixed constant with an explicit
/// `cutoff` parameter on `should_emit_event` keeps the suppression logic
/// itself fully unit-testable (see this module's own tests) without that
/// hazard. Only consulted for a run flagged `is_baseline_backfill`.
pub const LIVE_MONITORING_CUTOFF: &str = "2026-09-05T00:00:00Z";

/// Parses `LIVE_MONITORING_CUTOFF`. Infallible — the constant is a fixed,
/// known-valid literal, not user input.
pub fn live_monitoring_cutoff() -> chrono::DateTime<chrono::Utc> {
    LIVE_MONITORING_CUTOFF
        .parse()
        .expect("LIVE_MONITORING_CUTOFF is a fixed, known-valid ISO 8601 literal")
}

/// KS-14 acceptance criterion: "baseline-event suppression." A pure,
/// directly testable decision function — every event-emitting call site
/// (wells, production, intents, transfers) calls this before recording
/// an event, rather than each reimplementing the same three-way logic.
///
/// - `force_historical` (the "deliberate historical timeline job" escape
///   hatch) always wins: true.
/// - A non-backfill (ordinary live/incremental) run always emits: this
///   function only ever suppresses a *backfill* run's events.
/// - A backfill run emits only if `occurred_at` is known AND at or after
///   `cutoff`.
///
/// `observed_at` is deliberately **not** used as a fallback for a
/// backfill run, even though it is always known: during a backfill,
/// `observed_at` is "whenever this reprocessing run happened to execute"
/// — always "now," and therefore always at or after any sane cutoff. A
/// fallback to it would silently defeat suppression for exactly the
/// event types most likely to flood a backfill (e.g. a well-status
/// change with no source-reported date) — caught by a real integration
/// test (`baseline_backfill_run_suppresses_historical_transition_events`)
/// during KS-14, not merely reasoned about. When `occurred_at` is
/// genuinely unknown, the safe default during a backfill is to suppress,
/// not to guess recency from processing time.
pub fn should_emit_event(
    is_baseline_backfill: bool,
    occurred_at: Option<chrono::DateTime<chrono::Utc>>,
    cutoff: chrono::DateTime<chrono::Utc>,
    force_historical: bool,
) -> bool {
    if force_historical {
        return true;
    }
    if !is_baseline_backfill {
        return true;
    }
    occurred_at.is_some_and(|date| date >= cutoff)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::SourceUrls;

    #[test]
    fn source_ids_are_state_prefixed() {
        for source in SOURCES {
            assert!(
                source.id.starts_with("ks_"),
                "source id {} must start with ks_",
                source.id
            );
        }
    }

    #[test]
    fn ingestion_source_codes_are_exactly_twelve_and_uppercase() {
        assert_eq!(INGESTION_SOURCE_CODES.len(), 12);
        for code in INGESTION_SOURCE_CODES {
            assert_eq!(
                *code,
                code.to_ascii_uppercase(),
                "ingestion source code {code} must be uppercase, matching ks_source.code"
            );
        }
        let unique: std::collections::HashSet<_> = INGESTION_SOURCE_CODES.iter().collect();
        assert_eq!(
            unique.len(),
            INGESTION_SOURCE_CODES.len(),
            "ingestion source codes must be unique"
        );
    }

    #[test]
    fn every_source_id_resolves_to_a_configured_url() {
        let urls = SourceUrls::from_env();
        for source in SOURCES {
            assert!(
                urls.get(source.id).is_some(),
                "source id {} has no matching field in SourceUrls::get",
                source.id
            );
        }
    }

    #[test]
    fn api_prefix_is_kansas() {
        assert_eq!(API_STATE_PREFIX, "15");
        assert!(api_digits_belong_to_state("1517120701"));
        assert!(!api_digits_belong_to_state("3517120701"));
    }

    #[test]
    fn production_file_urls_joins_base_and_filenames() {
        let urls = production_file_urls("https://www.kgs.ku.edu/PRS/Ora_Archive/");
        assert_eq!(urls.len(), KGS_PRODUCTION_FILES.len());
        assert_eq!(
            urls[0],
            "https://www.kgs.ku.edu/PRS/Ora_Archive/oil_leases_2020_present.zip"
        );
    }

    /// Hard data-use control (task doc §3.7 item 1): pre-1987 IHS-licensed
    /// production must never be ingested. Every configured production
    /// filename must name an era starting at MIN_PRODUCTION_YEAR or later
    /// — this fails the build if a pre-1990 archive is ever added to
    /// KGS_PRODUCTION_FILES without also revisiting this guard.
    #[test]
    fn production_files_never_predate_min_year() {
        for file in KGS_PRODUCTION_FILES {
            let earliest_year: i32 = file
                .split(|c: char| !c.is_ascii_digit())
                .filter(|s| s.len() == 4)
                .filter_map(|s| s.parse().ok())
                .min()
                .unwrap_or_else(|| panic!("no 4-digit year found in production filename {file}"));
            assert!(
                is_production_year_ingestible(earliest_year, false),
                "production filename {file} implies data before MIN_PRODUCTION_YEAR ({MIN_PRODUCTION_YEAR})"
            );
        }
    }

    #[test]
    fn pre_1990_years_are_only_ingestible_with_the_override_flag() {
        assert!(!is_production_year_ingestible(1989, false));
        assert!(is_production_year_ingestible(1989, true));
        assert!(is_production_year_ingestible(1990, false));
    }

    #[test]
    fn kansas_has_exactly_105_counties_and_no_duplicates() {
        assert_eq!(KS_COUNTIES.len(), 105);
        let unique: std::collections::HashSet<_> =
            KS_COUNTIES.iter().map(|c| c.to_ascii_uppercase()).collect();
        assert_eq!(unique.len(), 105);
    }

    #[test]
    fn county_lookup_is_case_insensitive_and_trims_whitespace() {
        assert!(is_valid_kansas_county("Sedgwick"));
        assert!(is_valid_kansas_county("SEDGWICK"));
        assert!(is_valid_kansas_county("  sedgwick  "));
        assert!(!is_valid_kansas_county("Denver"));
        assert!(!is_valid_kansas_county(""));
    }

    #[test]
    fn bounding_box_accepts_wichita_rejects_denver_and_oklahoma_city() {
        // Wichita, KS
        assert!(point_in_kansas_bounding_box(37.6872, -97.3301));
        // Denver, CO
        assert!(!point_in_kansas_bounding_box(39.7392, -104.9903));
        // Oklahoma City, OK
        assert!(!point_in_kansas_bounding_box(35.4676, -97.5164));
    }

    #[test]
    fn classify_production_month_separates_sentinels_from_real_months() {
        assert_eq!(
            classify_production_month(0),
            Some(ProductionRecordKind::YearlyTotal)
        );
        assert_eq!(
            classify_production_month(-1),
            Some(ProductionRecordKind::StartingCumulative)
        );
        assert_eq!(
            classify_production_month(6),
            Some(ProductionRecordKind::Monthly)
        );
        assert_eq!(classify_production_month(13), None);
        assert_eq!(classify_production_month(-2), None);
    }

    fn dt(s: &str) -> chrono::DateTime<chrono::Utc> {
        s.parse().unwrap()
    }

    #[test]
    fn live_runs_always_emit_regardless_of_date() {
        let cutoff = dt("2026-01-01T00:00:00Z");
        assert!(should_emit_event(
            false,
            Some(dt("1990-01-01T00:00:00Z")),
            cutoff,
            false
        ));
        assert!(should_emit_event(false, None, cutoff, false));
    }

    #[test]
    fn backfill_runs_suppress_events_before_the_cutoff() {
        let cutoff = dt("2026-01-01T00:00:00Z");
        assert!(!should_emit_event(
            true,
            Some(dt("1990-01-01T00:00:00Z")),
            cutoff,
            false
        ));
    }

    #[test]
    fn backfill_runs_still_emit_events_on_or_after_the_cutoff() {
        let cutoff = dt("2026-01-01T00:00:00Z");
        assert!(should_emit_event(
            true,
            Some(dt("2026-06-01T00:00:00Z")),
            cutoff,
            false
        ));
        assert!(should_emit_event(true, Some(cutoff), cutoff, false));
    }

    #[test]
    fn backfill_suppresses_by_default_when_occurred_at_is_unknown_never_falling_back_to_processing_time(
    ) {
        // The real bug this guards against: an event with no
        // source-reported date (e.g. WELL_STATUS_CHANGED) must not use
        // "now" as a stand-in during a backfill — "now" is always at or
        // after any sane cutoff, which would silently defeat suppression
        // for exactly the event types most likely to flood a backfill.
        let cutoff = dt("2026-01-01T00:00:00Z");
        assert!(!should_emit_event(true, None, cutoff, false));
    }

    #[test]
    fn force_historical_always_wins_even_in_a_suppressed_backfill() {
        let cutoff = dt("2026-01-01T00:00:00Z");
        assert!(should_emit_event(
            true,
            Some(dt("1990-01-01T00:00:00Z")),
            cutoff,
            true
        ));
        assert!(should_emit_event(true, None, cutoff, true));
    }
}
