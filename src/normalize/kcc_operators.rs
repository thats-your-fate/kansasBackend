//! KS-05: pure normalization helpers for KCC operator records. No I/O.

/// Deterministic normalization only — case, whitespace, and trailing
/// punctuation. Never fuzzy/similarity-based: KS-05 explicitly asks for
/// "preventing aggressive mergers of similarly named companies," so the
/// only thing this function may ever be used for is consistent
/// display/lookup of one already-identified operator (identity is always
/// `kcc_license_number`, never a name match).
pub fn normalize_company_name(raw: &str) -> String {
    let collapsed = raw.split_whitespace().collect::<Vec<_>>().join(" ");
    collapsed.trim_end_matches(['.', ',']).trim().to_uppercase()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ActiveStatus {
    Active,
    Inactive,
    Unknown,
}

impl ActiveStatus {
    pub fn as_db_str(self) -> &'static str {
        match self {
            ActiveStatus::Active => "active",
            ActiveStatus::Inactive => "inactive",
            ActiveStatus::Unknown => "unknown",
        }
    }
}

/// Derives `active_status` from which of KCC's two documented downloads
/// (KS-05: "active-only and all-operator downloads") a license number was
/// observed on in one run. A license seen only in the active list but not
/// the all list is a real data anomaly (the all list is expected to be a
/// superset) — still resolved to `Active` rather than fabricated as
/// `Unknown`, since it *was* reported active by the source; the
/// reconciliation report surfaces the anomaly separately rather than
/// hiding it inside this function's return value.
pub fn derive_active_status(seen_in_active_list: bool, seen_in_all_list: bool) -> ActiveStatus {
    match (seen_in_active_list, seen_in_all_list) {
        (true, _) => ActiveStatus::Active,
        (false, true) => ActiveStatus::Inactive,
        (false, false) => ActiveStatus::Unknown,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalizes_whitespace_case_and_trailing_punctuation() {
        assert_eq!(
            normalize_company_name("  Example   Operating,  LLC. "),
            "EXAMPLE OPERATING, LLC"
        );
    }

    #[test]
    fn does_not_strip_internal_punctuation() {
        assert_eq!(
            normalize_company_name("O'Brien & Sons, Inc."),
            "O'BRIEN & SONS, INC"
        );
    }

    #[test]
    fn active_status_prefers_active_list_membership() {
        assert_eq!(derive_active_status(true, true), ActiveStatus::Active);
        assert_eq!(derive_active_status(true, false), ActiveStatus::Active);
        assert_eq!(derive_active_status(false, true), ActiveStatus::Inactive);
        assert_eq!(derive_active_status(false, false), ActiveStatus::Unknown);
    }
}
