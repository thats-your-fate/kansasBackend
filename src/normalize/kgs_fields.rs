//! KS-08: pure normalization helpers. No I/O.

/// Deterministic normalization only (case/whitespace/trailing
/// punctuation) — same discipline as
/// `normalize::kcc_operators::normalize_company_name`. Never used to
/// decide that two differently-spelled field names are the same field;
/// `official_field_name` (the exact ArcGIS-reported string) is always the
/// real identity.
pub fn normalize_field_name(raw: &str) -> String {
    let collapsed = raw.split_whitespace().collect::<Vec<_>>().join(" ");
    collapsed.trim_end_matches(['.', ',']).trim().to_uppercase()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FieldActiveStatus {
    Active,
    Abandoned,
    Unknown,
}

impl FieldActiveStatus {
    pub fn as_db_str(self) -> &'static str {
        match self {
            FieldActiveStatus::Active => "active",
            FieldActiveStatus::Abandoned => "abandoned",
            FieldActiveStatus::Unknown => "unknown",
        }
    }
}

/// Confirmed live 2026-09-05: the ArcGIS layer's own `STATUS` field has
/// exactly two distinct values, `"Active"` and `"Abandoned"` — this is
/// authoritative (ArcGIS is the identity/geometry source of truth for
/// fields); a field seen only in one of the .xlsx enrichment files, never
/// in the ArcGIS layer, has no authoritative status and is `Unknown`
/// rather than guessed from which file it appeared in (the abandoned
/// file's own existence is a hint, not proof, since the ArcGIS layer is
/// the more current/authoritative source per the task doc).
pub fn derive_active_status(arcgis_status: Option<&str>) -> FieldActiveStatus {
    match arcgis_status.map(|s| s.trim().to_lowercase()) {
        Some(ref s) if s == "active" => FieldActiveStatus::Active,
        Some(ref s) if s == "abandoned" => FieldActiveStatus::Abandoned,
        _ => FieldActiveStatus::Unknown,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalizes_whitespace_case_and_trailing_punctuation() {
        assert_eq!(
            normalize_field_name("  Hugoton   Gas Area. "),
            "HUGOTON GAS AREA"
        );
    }

    #[test]
    fn derives_status_from_confirmed_arcgis_values() {
        assert_eq!(
            derive_active_status(Some("Active")),
            FieldActiveStatus::Active
        );
        assert_eq!(
            derive_active_status(Some("Abandoned")),
            FieldActiveStatus::Abandoned
        );
        assert_eq!(
            derive_active_status(Some("Something Else")),
            FieldActiveStatus::Unknown
        );
        assert_eq!(derive_active_status(None), FieldActiveStatus::Unknown);
    }
}
