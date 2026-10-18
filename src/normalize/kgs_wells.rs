//! KS-04: normalizes raw KGS well rows (from `ks_well_raw_snapshot`, KS-03)
//! into the fields `materialize::kgs_wells` writes to `ks_well`. Pure
//! logic, no I/O — every function here is directly unit-testable against
//! fixtures.

use chrono::NaiveDate;

use crate::constants::{self, WellLifecycleStatus};

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ApiValidationStatus {
    /// 14-digit, numeric, Kansas-prefixed — the standard shape.
    Valid,
    /// 10-digit only (no sidetrack/completion suffix) — common on older
    /// ("historical") wells. `api14` is still populated by padding
    /// `0000`, since that's KGS's own convention for a base well with no
    /// sidetrack (confirmed by real data: a plain well's API_NUM_NODASH
    /// is 14 digits ending in `0000` even though its dashed API_NUMBER
    /// shows only 3 segments) — but this status says so wasn't confirmed
    /// this way in the source, it was inferred.
    ValidShortForm,
    /// Present but not a clean 10- or 14-digit numeric string.
    Malformed,
    /// No API value in the source row at all.
    Missing,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize)]
pub struct ApiIdentity {
    pub api_raw: Option<String>,
    pub api14: Option<String>,
    pub api_base: Option<String>,
    pub api_suffix: Option<String>,
    pub api_validation_status: ApiValidationStatus,
}

/// Parses an API number into its canonical parts. `api_num_nodash` (the
/// digit-only form) is the parsing input — `api_number_dashed` is kept
/// only as the preserved `api_raw` display value, never re-parsed itself,
/// since its dash placement varies (3 segments for a base well, 4 for a
/// sidetrack — see the real examples in the doc comment on
/// `ApiValidationStatus::ValidShortForm`).
pub fn parse_api_number(
    api_num_nodash: Option<&str>,
    api_number_dashed: Option<&str>,
) -> ApiIdentity {
    let api_raw = api_number_dashed
        .map(|s| s.to_string())
        .or_else(|| api_num_nodash.map(|s| s.to_string()));

    let Some(nodash) = api_num_nodash.map(str::trim).filter(|s| !s.is_empty()) else {
        return ApiIdentity {
            api_raw,
            api14: None,
            api_base: None,
            api_suffix: None,
            api_validation_status: ApiValidationStatus::Missing,
        };
    };

    if !nodash.chars().all(|c| c.is_ascii_digit()) {
        return ApiIdentity {
            api_raw,
            api14: None,
            api_base: None,
            api_suffix: None,
            api_validation_status: ApiValidationStatus::Malformed,
        };
    }

    match nodash.len() {
        14 => ApiIdentity {
            api_raw,
            api14: Some(nodash.to_string()),
            api_base: Some(nodash[0..10].to_string()),
            api_suffix: Some(nodash[10..14].to_string()),
            api_validation_status: ApiValidationStatus::Valid,
        },
        10 => ApiIdentity {
            api_raw,
            api14: Some(format!("{nodash}0000")),
            api_base: Some(nodash.to_string()),
            api_suffix: Some("0000".to_string()),
            api_validation_status: ApiValidationStatus::ValidShortForm,
        },
        _ => ApiIdentity {
            api_raw,
            api14: None,
            api_base: None,
            api_suffix: None,
            api_validation_status: ApiValidationStatus::Malformed,
        },
    }
}

/// True when `api14` both parses cleanly and carries the Kansas API
/// prefix — the "valid" half of KS-03's membership rule, reused here
/// since a row already staged as `included` could still have a malformed
/// API relative to the *identity* rules (membership only required the
/// prefix check OR the geometry+county fallback, not a full parse).
pub fn api_identity_is_kansas_valid(identity: &ApiIdentity) -> bool {
    matches!(
        identity.api_validation_status,
        ApiValidationStatus::Valid | ApiValidationStatus::ValidShortForm
    ) && identity
        .api14
        .as_deref()
        .is_some_and(constants::api_digits_belong_to_state)
}

/// Combine the KGS purpose code and KCC status description, ranking lifecycle
/// signals before well purpose. Match whole words so inactive cannot mean active.
/// Unknown combinations stay visible as Unknown, never default to Producing.
pub fn derive_lifecycle_status(
    well_class: Option<&str>,
    status: Option<&str>,
    status2: Option<&str>,
) -> WellLifecycleStatus {
    let combined: String = [well_class, status, status2]
        .into_iter()
        .flatten()
        .map(str::to_lowercase)
        .collect::<Vec<_>>()
        .join(" ");
    classify_status_text(&combined).unwrap_or(WellLifecycleStatus::Unknown)
}

/// `text` must already be lowercased (callers pass the combined,
/// lowercased field text) — patterns below are matched in priority order,
/// most specific first, and the function returns on the first match.
fn classify_status_text(text: &str) -> Option<WellLifecycleStatus> {
    let t = text.trim();
    if t.is_empty() {
        return None;
    }
    let words: Vec<&str> = t.split(|c: char| !c.is_ascii_alphanumeric()).collect();
    let has = |word: &str| words.contains(&word);
    if t.contains("expired intent") {
        Some(WellLifecycleStatus::IntentExpired)
    } else if t.contains("d&a") || t.contains("dry and abandoned") || t.contains("dry hole") {
        Some(WellLifecycleStatus::DryAndAbandoned)
    } else if t.contains("p&a")
        || has("plugged")
        || t.contains("fee fund plugging")
        || t.contains("federal plugging")
    {
        Some(WellLifecycleStatus::Plugged)
    } else if has("inactive")
        || t.contains("shut-in")
        || t.contains("shut in")
        || t.contains("authorization terminated")
        || t.contains("authority suspended")
    {
        Some(WellLifecycleStatus::Inactive)
    } else if has("loc") || t.contains("intent to drill") {
        Some(WellLifecycleStatus::Planned)
    } else if t.contains("swd")
        || t.contains("saltwater disposal")
        || t.contains("salt water disposal")
    {
        Some(WellLifecycleStatus::SaltwaterDisposal)
    } else if t.contains("injection")
        || has("iw")
        || has("inj")
        || has("eor")
        || t.contains("input well")
    {
        Some(WellLifecycleStatus::Injection)
    } else if has("producing") || has("active") {
        Some(WellLifecycleStatus::Producing)
    } else if t.contains("drilling") || has("drilled") || has("spudded") {
        Some(WellLifecycleStatus::Drilled)
    } else if t.contains("completion") || has("completed") || has("recompleted") {
        Some(WellLifecycleStatus::Completed)
    } else if t.contains("permit") || t.contains("planned") {
        Some(WellLifecycleStatus::Planned)
    } else {
        None
    }
}

/// KGS STATUS encodes well purpose independently of lifecycle (e.g. OIL-P&A).
/// https://www.kgs.ku.edu/Magellan/Elog/status.html
pub fn derive_well_type(well_class: Option<&str>, status: Option<&str>) -> Option<String> {
    let raw = status.or(well_class)?.trim().to_ascii_uppercase();
    let code = raw.split("-P&A").next().unwrap_or(&raw);
    let kind = match code {
        "OIL" | "OIL WELL" => "OIL",
        "GAS" | "GAS WELL" => "GAS",
        "O&G" | "OIL/GAS" | "OIL AND GAS" => "OIL AND GAS",
        "CBM" => "COALBED METHANE",
        "SWD" => "SALTWATER DISPOSAL",
        "EOR" => "ENHANCED OIL RECOVERY",
        "INJ" => "INJECTION",
        "D&A" => "DRY HOLE",
        _ if code.starts_with("OTHER") => {
            let detail = code.split_once('(').and_then(|(_, s)| s.strip_suffix(')'));
            match detail {
                Some("CATH") => "CATHODIC PROTECTION",
                Some("WATER") => "WATER",
                Some("STRAT") => "STRATIGRAPHIC TEST",
                Some("OBS") => "OBSERVATION",
                Some("GSW" | "GAS-STG") => "GAS STORAGE",
                Some("GAS-INJ" | "OIL&GAS-INJ" | "INJ OR EOR" | "CLASS1" | "CLASS ONE (OLD)") => {
                    "INJECTION"
                }
                Some("HELIUM") => "HELIUM",
                _ => return None,
            }
        }
        _ => {
            return well_class
                .filter(|v| !v.trim().is_empty())
                .map(str::to_owned)
        }
    };
    Some(kind.to_owned())
}

/// Parses a KGS date string. The two sources use different formats
/// (confirmed real samples): ArcGIS `MM/DD/YYYY` (e.g. `"07/12/1983"`),
/// ZIP `DD-MON-YYYY` with an uppercase month abbreviation (e.g.
/// `"01-MAR-1967"`). Returns `None` — never panics — on anything that
/// doesn't parse; the raw string is always preserved alongside this by
/// the caller regardless of parse success.
pub fn parse_kgs_date(raw: Option<&str>, source_format: &str) -> Option<NaiveDate> {
    let raw = raw?.trim();
    if raw.is_empty() {
        return None;
    }
    match source_format {
        "arcgis" => NaiveDate::parse_from_str(raw, "%m/%d/%Y").ok(),
        "zip" => NaiveDate::parse_from_str(&title_case_month(raw), "%d-%b-%Y").ok(),
        _ => None,
    }
}

fn title_case_month(date: &str) -> String {
    let parts: Vec<&str> = date.split('-').collect();
    if parts.len() != 3 {
        return date.to_string();
    }
    let mut chars = parts[1].chars();
    let month = match chars.next() {
        Some(first) => {
            first.to_ascii_uppercase().to_string() + &chars.as_str().to_ascii_lowercase()
        }
        None => parts[1].to_string(),
    };
    format!("{}-{}-{}", parts[0], month, parts[2])
}

/// Location-source confidence, derived only from what KGS's own
/// documentation actually states (file_format.html: "may be calculated
/// from footages, quarter calls, or from GPS") — no confidence level
/// invented beyond that.
pub fn derive_location_confidence(location_source: Option<&str>) -> &'static str {
    match location_source.map(str::to_lowercase) {
        Some(s) if s.contains("gps") => "gps",
        Some(s) if s.contains("footage") || s.contains("quarter") || s.contains("calc") => {
            "derived_from_legal_description"
        }
        Some(_) => "unknown",
        None => "unknown",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // --- API number fixtures (KS-04 acceptance criteria: standard,
    // missing, malformed, historical, sidetrack) ---

    #[test]
    fn standard_api_number_is_valid() {
        // Real example: a base well, no sidetrack.
        let identity = parse_api_number(Some("15059232820000"), Some("15-059-23282"));
        assert_eq!(identity.api_validation_status, ApiValidationStatus::Valid);
        assert_eq!(identity.api14.as_deref(), Some("15059232820000"));
        assert_eq!(identity.api_base.as_deref(), Some("1505923282"));
        assert_eq!(identity.api_suffix.as_deref(), Some("0000"));
        assert_eq!(identity.api_raw.as_deref(), Some("15-059-23282"));
    }

    #[test]
    fn sidetrack_api_number_has_nonzero_suffix() {
        // Real example: a well with a 4th dash segment / nonzero suffix.
        let identity = parse_api_number(Some("15009220360001"), Some("15-009-22036-0001"));
        assert_eq!(identity.api_validation_status, ApiValidationStatus::Valid);
        assert_eq!(identity.api14.as_deref(), Some("15009220360001"));
        assert_eq!(identity.api_base.as_deref(), Some("1500922036"));
        assert_eq!(identity.api_suffix.as_deref(), Some("0001"));
    }

    #[test]
    fn historical_short_form_api_number_pads_to_api14() {
        let identity = parse_api_number(Some("1500922036"), Some("15-009-22036"));
        assert_eq!(
            identity.api_validation_status,
            ApiValidationStatus::ValidShortForm
        );
        assert_eq!(identity.api14.as_deref(), Some("15009220360000"));
        assert_eq!(identity.api_base.as_deref(), Some("1500922036"));
    }

    #[test]
    fn missing_api_number_is_missing() {
        let identity = parse_api_number(None, None);
        assert_eq!(identity.api_validation_status, ApiValidationStatus::Missing);
        assert_eq!(identity.api14, None);
    }

    #[test]
    fn malformed_api_number_with_letters_is_malformed() {
        let identity = parse_api_number(Some("15AB9220360000"), Some("15-AB-22036"));
        assert_eq!(
            identity.api_validation_status,
            ApiValidationStatus::Malformed
        );
        assert_eq!(identity.api14, None);
        // api_raw is still preserved even when unparseable.
        assert_eq!(identity.api_raw.as_deref(), Some("15-AB-22036"));
    }

    #[test]
    fn malformed_api_number_with_wrong_length_is_malformed() {
        let identity = parse_api_number(Some("12345"), None);
        assert_eq!(
            identity.api_validation_status,
            ApiValidationStatus::Malformed
        );
    }

    #[test]
    fn kansas_validity_requires_both_parse_success_and_prefix() {
        let valid_ks = parse_api_number(Some("15059232820000"), None);
        assert!(api_identity_is_kansas_valid(&valid_ks));

        let valid_other_state = parse_api_number(Some("35000000000000"), None);
        assert!(!api_identity_is_kansas_valid(&valid_other_state));

        let malformed = parse_api_number(Some("not-a-number"), None);
        assert!(!api_identity_is_kansas_valid(&malformed));
    }

    // --- lifecycle status derivation ---

    #[test]
    fn dry_and_abandoned_from_real_observed_values() {
        assert_eq!(
            derive_lifecycle_status(
                Some("Plugged and Abandoned"),
                Some("D&A"),
                Some("Plugged and Abandoned")
            ),
            WellLifecycleStatus::DryAndAbandoned
        );
    }

    #[test]
    fn expired_intent_from_real_observed_values() {
        assert_eq!(
            derive_lifecycle_status(None, Some("LOC"), Some("Expired Intent to Drill (C-1)")),
            WellLifecycleStatus::IntentExpired
        );
    }

    #[test]
    fn saltwater_disposal_compound_code_from_real_observed_values() {
        assert_eq!(
            derive_lifecycle_status(None, Some("SWD-P&A"), Some("Plugged and Abandoned")),
            WellLifecycleStatus::Plugged
        );
    }

    #[test]
    fn unmapped_combination_is_unknown_not_silently_inactive() {
        assert_eq!(
            derive_lifecycle_status(Some("Xyzzy Class 7"), Some("Q9"), None),
            WellLifecycleStatus::Unknown
        );
    }

    #[test]
    fn all_fields_absent_is_unknown() {
        assert_eq!(
            derive_lifecycle_status(None, None, None),
            WellLifecycleStatus::Unknown
        );
    }

    #[test]
    fn producing_and_injection_are_distinguished() {
        assert_eq!(
            derive_lifecycle_status(None, None, Some("Active Oil Well")),
            WellLifecycleStatus::Producing
        );
        assert_eq!(
            derive_lifecycle_status(None, None, Some("Injection Well")),
            WellLifecycleStatus::Injection
        );
    }

    #[test]
    fn status_does_not_confuse_purpose_with_activity() {
        for code in ["OIL", "GAS", "SWD", "EOR", "OTHER(CATH)"] {
            assert_eq!(
                derive_lifecycle_status(None, Some(code), Some("Inactive Well")),
                WellLifecycleStatus::Inactive
            );
        }
        assert_eq!(
            derive_lifecycle_status(None, Some("OIL"), Some("Well Drilled")),
            WellLifecycleStatus::Drilled
        );
        assert_eq!(
            derive_lifecycle_status(None, Some("GAS"), Some("Recompleted")),
            WellLifecycleStatus::Completed
        );
        assert_eq!(
            derive_lifecycle_status(None, Some("OIL"), None),
            WellLifecycleStatus::Unknown
        );
        assert_eq!(
            derive_lifecycle_status(None, Some("SWD-P&A"), Some("Plugged and Abandoned")),
            WellLifecycleStatus::Plugged
        );
        assert_eq!(
            derive_well_type(None, Some("OIL-P&A")).as_deref(),
            Some("OIL")
        );
        assert_eq!(derive_well_type(None, Some("GAS")).as_deref(), Some("GAS"));
        assert_eq!(derive_well_type(None, Some("LOC")), None);
    }

    // --- date parsing ---

    #[test]
    fn parses_arcgis_slash_date() {
        assert_eq!(
            parse_kgs_date(Some("07/12/1983"), "arcgis"),
            Some(NaiveDate::from_ymd_opt(1983, 7, 12).unwrap())
        );
    }

    #[test]
    fn parses_zip_dash_month_date_uppercase() {
        assert_eq!(
            parse_kgs_date(Some("01-MAR-1967"), "zip"),
            Some(NaiveDate::from_ymd_opt(1967, 3, 1).unwrap())
        );
    }

    #[test]
    fn blank_and_missing_dates_return_none_not_panic() {
        assert_eq!(parse_kgs_date(Some(""), "arcgis"), None);
        assert_eq!(parse_kgs_date(Some("   "), "zip"), None);
        assert_eq!(parse_kgs_date(None, "arcgis"), None);
        assert_eq!(parse_kgs_date(Some("garbage"), "arcgis"), None);
    }

    // --- location confidence ---

    #[test]
    fn location_confidence_matches_kgs_documented_categories() {
        assert_eq!(
            derive_location_confidence(Some("Calc. from footages")),
            "derived_from_legal_description"
        );
        assert_eq!(
            derive_location_confidence(Some("FOOTAGES")),
            "derived_from_legal_description"
        );
        assert_eq!(derive_location_confidence(Some("GPS")), "gps");
        assert_eq!(derive_location_confidence(None), "unknown");
    }
}
