//! KS-15: document-type classification and personal-info redaction for
//! Kansas well-document metadata. Both are pure functions, deliberately —
//! the task doc's own acceptance criteria ("document-type classifier
//! fixtures") call for exactly this kind of directly-testable unit.

use serde::{Deserialize, Serialize};

/// The full set of document types the task doc requires classifying, plus
/// `Other` as the honest fallback for anything that doesn't match a known
/// form/log code — never silently dropped, never guessed into a specific
/// category it doesn't clearly belong to.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DocumentType {
    /// C-1: Notice of Intent to Drill.
    C1,
    /// ACO-1: Well Completion.
    Aco1,
    /// T-1: Change of Operator.
    T1,
    /// CP-1: Plugging Application.
    Cp1,
    /// CP-4: Plugging Record.
    Cp4,
    /// C-5: Oil Productivity / GOR Test.
    C5,
    /// CG-1: Multi-Point Back Pressure Test.
    Cg1,
    /// G-2: Gas Test.
    G2,
    /// ACO-16: New Pool Determination.
    Aco16,
    /// U-1: UIC Application.
    U1,
    /// U3-C: Annual Injection Report.
    U3c,
    /// U-7: Mechanical Integrity Test.
    U7,
    /// U-8: Injection Permit Modification.
    U8,
    /// U-9: Injection Permit Modification.
    U9,
    WirelineLog,
    Las,
    DrillerReport,
    GeologistReport,
    DirectionalSurvey,
    Plat,
    Other,
}

impl DocumentType {
    pub fn as_db_str(self) -> &'static str {
        match self {
            Self::C1 => "c1",
            Self::Aco1 => "aco1",
            Self::T1 => "t1",
            Self::Cp1 => "cp1",
            Self::Cp4 => "cp4",
            Self::C5 => "c5",
            Self::Cg1 => "cg1",
            Self::G2 => "g2",
            Self::Aco16 => "aco16",
            Self::U1 => "u1",
            Self::U3c => "u3c",
            Self::U7 => "u7",
            Self::U8 => "u8",
            Self::U9 => "u9",
            Self::WirelineLog => "wireline_log",
            Self::Las => "las",
            Self::DrillerReport => "driller_report",
            Self::GeologistReport => "geologist_report",
            Self::DirectionalSurvey => "directional_survey",
            Self::Plat => "plat",
            Self::Other => "other",
        }
    }
}

/// Classifies a raw, source-reported document label/form-code into a
/// `DocumentType`. Matching is deliberately literal (exact form-code
/// tokens and a small set of real-world label phrasings), never fuzzy —
/// the same "no aggressive similarity merging" discipline this repo
/// applies to operator names and field names. Case/whitespace-insensitive
/// only, never a guess based on partial or ambiguous text.
pub fn classify_document_type(raw_label: &str) -> DocumentType {
    let normalized = raw_label
        .trim()
        .to_ascii_uppercase()
        .replace([' ', '_'], "-");
    match normalized.as_str() {
        "C-1" | "C1" | "NOTICE-OF-INTENT-TO-DRILL" => DocumentType::C1,
        "ACO-1" | "ACO1" | "WELL-COMPLETION" | "COMPLETION-REPORT" => DocumentType::Aco1,
        "T-1" | "T1" | "CHANGE-OF-OPERATOR" => DocumentType::T1,
        "CP-1" | "CP1" | "PLUGGING-APPLICATION" => DocumentType::Cp1,
        "CP-4" | "CP4" | "PLUGGING-RECORD" => DocumentType::Cp4,
        "C-5" | "C5" | "OIL-PRODUCTIVITY-TEST" | "GOR-TEST" => DocumentType::C5,
        "CG-1" | "CG1" | "MULTI-POINT-BACK-PRESSURE-TEST" => DocumentType::Cg1,
        "G-2" | "G2" | "GAS-TEST" => DocumentType::G2,
        "ACO-16" | "ACO16" | "NEW-POOL-DETERMINATION" => DocumentType::Aco16,
        "U-1" | "U1" | "UIC-APPLICATION" => DocumentType::U1,
        "U3-C" | "U3C" | "U-3C" | "ANNUAL-INJECTION-REPORT" => DocumentType::U3c,
        "U-7" | "U7" | "MECHANICAL-INTEGRITY-TEST" | "MIT" => DocumentType::U7,
        "U-8" | "U8" => DocumentType::U8,
        "U-9" | "U9" => DocumentType::U9,
        "WIRELINE-LOG" | "WIRELINE" | "E-LOG" | "ELOG" | "ELECTRIC-LOG" => {
            DocumentType::WirelineLog
        }
        "LAS" | "LAS-FILE" => DocumentType::Las,
        "DRILLER-REPORT" | "DRILLERS-REPORT" | "DRILLER-LOG" => DocumentType::DrillerReport,
        "GEOLOGIST-REPORT" | "GEOLOGISTS-REPORT" | "GEOLOGIST-LOG" => DocumentType::GeologistReport,
        "DIRECTIONAL-SURVEY" | "DEVIATION-SURVEY" => DocumentType::DirectionalSurvey,
        "PLAT" | "LOCATION-PLAT" => DocumentType::Plat,
        _ => DocumentType::Other,
    }
}

/// Public-access classification for a document reference. Task doc:
/// "Respect confidentiality, login pages, robots/access restrictions,
/// and unavailable records. Do not attempt to acquire documents from
/// login-only libraries."
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AccessState {
    Public,
    Restricted,
    LoginRequired,
    Unavailable,
}

impl AccessState {
    pub fn as_db_str(self) -> &'static str {
        match self {
            Self::Public => "public",
            Self::Restricted => "restricted",
            Self::LoginRequired => "login_required",
            Self::Unavailable => "unavailable",
        }
    }
}

/// Real, confirmed-live path prefixes under `www.kgs.ku.edu` that
/// `robots.txt` explicitly disallows (checked directly, 2026-09-05) and
/// that are exactly Kansas well-document repositories: completion
/// records, document/transfer scans, electric-log scans, well-log
/// images, and type logs. A URL under any of these must never be fetched
/// — classified `Restricted` rather than silently skipped, so it is
/// still visible in the metadata index as "known to exist, not
/// crawlable."
const ROBOTS_DISALLOWED_KGS_PATHS: &[&str] = &[
    "/PRS/ACO/",
    "/PRS/Documents",
    "/PRS/Transfers",
    "/PRS/Elog2/",
    "/PRS/ElogScan",
    "/PRS/Images",
    "/PRS/Images2",
    "/PRS/TypeLogs",
    "/PRS/Typelog",
    "/WellLogs/",
];

/// Classifies a document reference's access state from its real URL
/// and/or the source page's own label text. `label` is checked for the
/// literal phrase KGS itself uses on real well-detail pages — "View well
/// in the Walter's Digital Library (WDL login required)" — confirmed by
/// direct fetch of a real well record (KID 1001190703) during this
/// task's discovery. That same page links to `kgslibrary.com`, which is
/// treated as login-required by host as a second, independent signal.
pub fn classify_access_state(url: &str, label: Option<&str>) -> AccessState {
    if let Some(label) = label {
        if label.to_ascii_lowercase().contains("login required") {
            return AccessState::LoginRequired;
        }
    }
    if let Ok(parsed) = url::Url::parse(url) {
        if let Some(host) = parsed.host_str() {
            if host.eq_ignore_ascii_case("www.kgslibrary.com")
                || host.eq_ignore_ascii_case("kgslibrary.com")
            {
                return AccessState::LoginRequired;
            }
        }
        let path = parsed.path();
        if ROBOTS_DISALLOWED_KGS_PATHS
            .iter()
            .any(|prefix| path.starts_with(prefix))
        {
            return AccessState::Restricted;
        }
    }
    AccessState::Public
}

/// Task doc: "Extracted text must exclude personal addresses, phone
/// numbers and surface-owner information from the customer-facing search
/// index." A conservative, pattern-based redaction — phone numbers and
/// street-address-shaped lines are replaced outright; this is a floor,
/// not a claim of perfect PII detection, and is applied to any extracted
/// text before it could ever reach a customer-facing index (no such
/// index exists yet in this repo — this function exists so that when one
/// does, the redaction step is already built and tested, not added
/// afterward under time pressure).
pub fn redact_personal_info(text: &str) -> String {
    let phone_redacted = redact_phone_numbers(text);
    redact_street_address_lines(&phone_redacted)
}

fn redact_phone_numbers(text: &str) -> String {
    let mut result = String::with_capacity(text.len());
    let chars: Vec<char> = text.chars().collect();
    let mut i = 0;
    while i < chars.len() {
        if let Some(len) = phone_match_len(&chars[i..]) {
            result.push_str("[REDACTED-PHONE]");
            i += len;
        } else {
            result.push(chars[i]);
            i += 1;
        }
    }
    result
}

/// Matches `NNN-NNN-NNNN`, `(NNN) NNN-NNNN`, and `NNN.NNN.NNNN` shaped
/// runs starting at `chars[0]`, returning how many source characters the
/// match consumed.
fn phone_match_len(chars: &[char]) -> Option<usize> {
    let mut i = 0;
    let has_paren = chars.first() == Some(&'(');
    if has_paren {
        i += 1;
    }
    take_digits(chars, i, 3)?;
    i += 3;
    if has_paren {
        if chars.get(i) != Some(&')') {
            return None;
        }
        i += 1;
        if chars.get(i) == Some(&' ') {
            i += 1;
        }
    } else {
        let sep = *chars.get(i)?;
        if sep != '-' && sep != '.' {
            return None;
        }
        i += 1;
    }
    take_digits(chars, i, 3)?;
    i += 3;
    let sep2 = *chars.get(i)?;
    if sep2 != '-' && sep2 != '.' {
        return None;
    }
    i += 1;
    take_digits(chars, i, 4)?;
    i += 4;
    Some(i)
}

fn take_digits(chars: &[char], start: usize, count: usize) -> Option<()> {
    for offset in 0..count {
        if !chars.get(start + offset)?.is_ascii_digit() {
            return None;
        }
    }
    Some(())
}

/// Redacts a whole line if it looks like a mailing address (starts with a
/// house number followed by street-name words) — conservative and
/// line-scoped so unrelated numeric content elsewhere in the document is
/// left alone.
fn redact_street_address_lines(text: &str) -> String {
    text.lines()
        .map(|line| {
            if looks_like_street_address(line) {
                "[REDACTED-ADDRESS]"
            } else {
                line
            }
        })
        .collect::<Vec<_>>()
        .join("\n")
}

fn looks_like_street_address(line: &str) -> bool {
    let trimmed = line.trim();
    let mut words = trimmed.split_whitespace();
    let Some(first) = words.next() else {
        return false;
    };
    if !first.chars().all(|c| c.is_ascii_digit()) || first.is_empty() {
        return false;
    }
    let rest: Vec<&str> = words.collect();
    if rest.len() < 2 {
        return false;
    }
    const STREET_SUFFIXES: &[&str] = &[
        "ST", "ST.", "STREET", "AVE", "AVE.", "AVENUE", "RD", "RD.", "ROAD", "DR", "DR.", "DRIVE",
        "LN", "LN.", "LANE", "BLVD", "BLVD.", "HWY", "HWY.", "HIGHWAY", "CT", "CT.", "COURT",
    ];
    rest.iter().any(|word| {
        STREET_SUFFIXES.contains(&word.trim_end_matches(',').to_ascii_uppercase().as_str())
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classifies_every_required_form_code() {
        let cases: &[(&str, DocumentType)] = &[
            ("C-1", DocumentType::C1),
            ("ACO-1", DocumentType::Aco1),
            ("T-1", DocumentType::T1),
            ("CP-1", DocumentType::Cp1),
            ("CP-4", DocumentType::Cp4),
            ("C-5", DocumentType::C5),
            ("CG-1", DocumentType::Cg1),
            ("G-2", DocumentType::G2),
            ("ACO-16", DocumentType::Aco16),
            ("U-1", DocumentType::U1),
            ("U3-C", DocumentType::U3c),
            ("U-7", DocumentType::U7),
            ("U-8", DocumentType::U8),
            ("U-9", DocumentType::U9),
            ("Wireline Log", DocumentType::WirelineLog),
            ("LAS", DocumentType::Las),
            ("Driller Report", DocumentType::DrillerReport),
            ("Geologist Report", DocumentType::GeologistReport),
            ("Directional Survey", DocumentType::DirectionalSurvey),
            ("Plat", DocumentType::Plat),
        ];
        for (raw, expected) in cases {
            assert_eq!(
                classify_document_type(raw),
                *expected,
                "failed to classify {raw:?}"
            );
        }
    }

    #[test]
    fn is_case_and_separator_insensitive() {
        assert_eq!(classify_document_type("cp-4"), DocumentType::Cp4);
        assert_eq!(classify_document_type("CP4"), DocumentType::Cp4);
        assert_eq!(classify_document_type("cp_4"), DocumentType::Cp4);
        assert_eq!(classify_document_type("  CP-4  "), DocumentType::Cp4);
    }

    #[test]
    fn unrecognized_labels_fall_back_to_other_never_a_guess() {
        assert_eq!(
            classify_document_type("Some Unknown Form XYZ"),
            DocumentType::Other
        );
        assert_eq!(classify_document_type(""), DocumentType::Other);
    }

    #[test]
    fn every_variant_has_a_stable_db_string() {
        // A change here is a schema-breaking change (the CHECK constraint
        // in migration 0027 must be updated to match) — this test exists
        // so that fact is impossible to miss.
        assert_eq!(DocumentType::C1.as_db_str(), "c1");
        assert_eq!(DocumentType::U3c.as_db_str(), "u3c");
        assert_eq!(DocumentType::Other.as_db_str(), "other");
    }

    #[test]
    fn redacts_common_phone_number_formats() {
        assert_eq!(
            redact_personal_info("Call 785-555-0123 for info"),
            "Call [REDACTED-PHONE] for info"
        );
        assert_eq!(
            redact_personal_info("Call (785) 555-0123 for info"),
            "Call [REDACTED-PHONE] for info"
        );
        assert_eq!(
            redact_personal_info("Call 785.555.0123 for info"),
            "Call [REDACTED-PHONE] for info"
        );
    }

    #[test]
    fn redacts_street_address_shaped_lines_but_leaves_other_lines_alone() {
        let text = "Operator: Test Operating LLC\n123 Main St\nSurface owner notified.";
        let redacted = redact_personal_info(text);
        assert!(redacted.contains("[REDACTED-ADDRESS]"));
        assert!(!redacted.contains("123 Main St"));
        assert!(
            redacted.contains("Operator: Test Operating LLC"),
            "unrelated lines must survive redaction untouched"
        );
    }

    #[test]
    fn does_not_redact_unrelated_numeric_content() {
        let text = "Total depth: 4500 feet\nSection 12, Township 20S, Range 10W";
        assert_eq!(redact_personal_info(text), text);
    }

    #[test]
    fn the_real_confirmed_wdl_library_link_is_login_required() {
        // Real link from a real well-detail page fetched during KS-15
        // discovery (KID 1001190703, 2026-09-05).
        assert_eq!(
            classify_access_state(
                "https://www.kgslibrary.com/index.cfm?whk=1001190703",
                Some("View well in the Walter's Digital Library (WDL login required)")
            ),
            AccessState::LoginRequired
        );
    }

    #[test]
    fn kgslibrary_host_is_login_required_even_without_a_label() {
        assert_eq!(
            classify_access_state("https://www.kgslibrary.com/index.cfm?whk=123", None),
            AccessState::LoginRequired
        );
    }

    #[test]
    fn real_robots_disallowed_kgs_paths_are_restricted() {
        // Confirmed directly against https://www.kgs.ku.edu/robots.txt,
        // 2026-09-05 — these are exactly the well-document repositories.
        for path in [
            "https://www.kgs.ku.edu/PRS/ACO/1001190703.pdf",
            "https://www.kgs.ku.edu/PRS/Documents/foo.pdf",
            "https://www.kgs.ku.edu/WellLogs/foo.tif",
        ] {
            assert_eq!(
                classify_access_state(path, None),
                AccessState::Restricted,
                "expected {path} to be restricted"
            );
        }
    }

    #[test]
    fn an_ordinary_public_kgs_url_is_public() {
        assert_eq!(
            classify_access_state(
                "https://chasm.kgs.ku.edu/ords/qualified.well_page.DisplayWell?f_kid=1001190703",
                None
            ),
            AccessState::Public
        );
    }
}
