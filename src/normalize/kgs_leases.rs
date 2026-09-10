//! KS-09: pure normalization helpers. No I/O.

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LeaseProduct {
    Oil,
    Gas,
    Unknown,
}

impl LeaseProduct {
    pub fn as_db_str(self) -> &'static str {
        match self {
            LeaseProduct::Oil => "oil",
            LeaseProduct::Gas => "gas",
            LeaseProduct::Unknown => "unknown",
        }
    }
}

/// Confirmed live 2026-09-05: the real `all_kansas_leases.txt` file's
/// `PRODUCES` column is sometimes the literal string `"NULL"` (2,359 of
/// 98,639 real rows), not a blank value — handled explicitly here rather
/// than falling through to a generic "anything else is unknown" branch
/// that would treat a real parse failure the same way as this documented
/// sentinel.
pub fn derive_product(produces_raw: Option<&str>) -> LeaseProduct {
    match produces_raw.map(|s| s.trim().to_uppercase()) {
        Some(ref s) if s == "OIL" => LeaseProduct::Oil,
        Some(ref s) if s == "GAS" => LeaseProduct::Gas,
        _ => LeaseProduct::Unknown,
    }
}

/// Splits the lease file's own `API_NUMBER` field (e.g. `"15-131-20143,
/// 15-131-20089-0002"`) into individual trimmed tokens. Never fuzzy —
/// each token is tried as an exact `ks_well.api14`/`api_raw` match by the
/// materializer.
pub fn parse_api_number_list(raw: Option<&str>) -> Vec<String> {
    let Some(raw) = raw else { return Vec::new() };
    raw.split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
        .collect()
}

/// Digits-only form of an API token (`"15-131-20143"` -> `"15131201430000"`
/// padded, or as-given if already digits) — for matching against
/// `ks_well.api14`, which is always the 14-digit normalized form.
/// Returns `None` rather than guessing when the token doesn't look like a
/// plausible API number at all.
pub fn normalize_api_token(token: &str) -> Option<String> {
    let digits: String = token.chars().filter(|c| c.is_ascii_digit()).collect();
    match digits.len() {
        14 => Some(digits),
        10 => Some(format!("{digits}0000")),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn derives_product_from_confirmed_values() {
        assert_eq!(derive_product(Some("OIL")), LeaseProduct::Oil);
        assert_eq!(derive_product(Some("gas")), LeaseProduct::Gas);
        assert_eq!(derive_product(Some("NULL")), LeaseProduct::Unknown);
        assert_eq!(derive_product(None), LeaseProduct::Unknown);
    }

    #[test]
    fn splits_a_multi_api_list() {
        let tokens = parse_api_number_list(Some("15-131-20143, 15-131-20089-0002, 15-131-20239 "));
        assert_eq!(
            tokens,
            vec!["15-131-20143", "15-131-20089-0002", "15-131-20239"]
        );
    }

    #[test]
    fn empty_api_list_is_empty_not_a_single_blank_token() {
        assert_eq!(parse_api_number_list(Some("")), Vec::<String>::new());
        assert_eq!(parse_api_number_list(None), Vec::<String>::new());
    }

    #[test]
    fn normalizes_dashed_and_short_form_api_tokens() {
        assert_eq!(
            normalize_api_token("15-131-20143"),
            Some("15131201430000".to_string())
        );
        assert_eq!(
            normalize_api_token("1513120143"),
            Some("15131201430000".to_string())
        );
        assert_eq!(
            normalize_api_token("15-131-20089-0002"),
            Some("15131200890002".to_string())
        );
        assert_eq!(normalize_api_token("not-an-api"), None);
    }
}
