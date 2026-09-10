pub mod kcc_operators;
pub mod kgs_documents;
pub mod kgs_fields;
pub mod kgs_leases;
pub mod kgs_production;
pub mod kgs_wells;

use chrono::NaiveDate;

/// Shared by `parse::kcc_intents`/`parse::kcc_transfers`: KCC's
/// search-result pages are assumed (unverified — kcc.ks.gov is
/// unreachable) to render dates as plain US `MM/DD/YYYY`, the same
/// convention KGS's own ArcGIS layer uses. Returns `None` — never a
/// fabricated date — on anything else.
pub fn parse_us_date(raw: Option<&str>) -> Option<NaiveDate> {
    let raw = raw?.trim();
    if raw.is_empty() {
        return None;
    }
    NaiveDate::parse_from_str(raw, "%m/%d/%Y").ok()
}

/// Parses a township/range cell that may be a bare number ("15") or a
/// number with a trailing direction letter ("15S", "1E") — the exact
/// column shape KCC uses is unconfirmed, so this handles either. Returns
/// `(number, direction)`; `direction` is `None` when the cell was bare
/// (the caller should fall back to a separate direction column, if any).
pub fn parse_township_or_range(raw: &str) -> (Option<i16>, Option<String>) {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return (None, None);
    }
    let (digits, direction) = match trimmed.chars().last() {
        Some(c) if c.is_ascii_alphabetic() => (
            &trimmed[..trimmed.len() - 1],
            Some(c.to_ascii_uppercase().to_string()),
        ),
        _ => (trimmed, None),
    };
    (digits.trim().parse::<i16>().ok(), direction)
}

pub fn normalize_direction(raw: Option<&str>) -> Option<String> {
    let raw = raw?.trim();
    if raw.is_empty() {
        return None;
    }
    Some(raw.chars().next()?.to_ascii_uppercase().to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_bare_number() {
        assert_eq!(parse_township_or_range("15"), (Some(15), None));
    }

    #[test]
    fn parses_number_with_trailing_direction() {
        assert_eq!(
            parse_township_or_range("15S"),
            (Some(15), Some("S".to_string()))
        );
        assert_eq!(
            parse_township_or_range("1e"),
            (Some(1), Some("E".to_string()))
        );
    }

    #[test]
    fn blank_is_none() {
        assert_eq!(parse_township_or_range("  "), (None, None));
    }

    #[test]
    fn normalize_direction_takes_first_letter_uppercased() {
        assert_eq!(normalize_direction(Some("south")), Some("S".to_string()));
        assert_eq!(normalize_direction(Some("")), None);
        assert_eq!(normalize_direction(None), None);
    }

    #[test]
    fn parses_us_date_and_rejects_garbage() {
        assert_eq!(
            parse_us_date(Some("01/15/2026")),
            NaiveDate::from_ymd_opt(2026, 1, 15)
        );
        assert_eq!(parse_us_date(Some("not a date")), None);
        assert_eq!(parse_us_date(None), None);
    }
}
