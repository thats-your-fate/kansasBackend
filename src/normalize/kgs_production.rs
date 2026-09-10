//! KS-10: pure normalization helpers. No I/O.

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProductionProduct {
    Oil,
    Gas,
}

impl ProductionProduct {
    pub fn as_db_str(self) -> &'static str {
        match self {
            ProductionProduct::Oil => "oil",
            ProductionProduct::Gas => "gas",
        }
    }
}

/// Confirmed live 2026-09-05 against the real decade archives: `PRODUCT`
/// is `"O"` for oil, `"G"` for gas (KGS's own lease-format documentation:
/// "G for gas, including coalbed methane" — one code, not a separate CBM
/// code). Returns `None` — never a guess — for anything else, since the
/// caller (the oil or gas file itself) already asserts which product a
/// row *should* be; a mismatch is worth surfacing, not silently coerced.
pub fn parse_product(raw: Option<&str>) -> Option<ProductionProduct> {
    match raw.map(|s| s.trim().to_uppercase()) {
        Some(ref s) if s == "O" => Some(ProductionProduct::Oil),
        Some(ref s) if s == "G" => Some(ProductionProduct::Gas),
        _ => None,
    }
}

/// Splits KGS's `MONTH-YEAR` field (confirmed live format: `"M-YYYY"` or
/// `"MM-YYYY"`, e.g. `"1-2020"`, `"12-2020"`) into `(month, year)`. Month
/// may legitimately be `-1` (the field itself can carry a leading `-`,
/// e.g. `"-1-1996"` for a starting-cumulative row) or `0` (yearly
/// summary) — both real KGS sentinels (task doc), not malformed data.
pub fn parse_month_year(raw: &str) -> Option<(i16, i32)> {
    let raw = raw.trim();
    let (month_str, year_str) = raw.rsplit_once('-')?;
    let month: i16 = month_str.parse().ok()?;
    let year: i32 = year_str.parse().ok()?;
    Some((month, year))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_confirmed_product_codes() {
        assert_eq!(parse_product(Some("O")), Some(ProductionProduct::Oil));
        assert_eq!(parse_product(Some("g")), Some(ProductionProduct::Gas));
        assert_eq!(parse_product(Some("X")), None);
        assert_eq!(parse_product(None), None);
    }

    #[test]
    fn parses_month_year_including_negative_month() {
        assert_eq!(parse_month_year("1-2020"), Some((1, 2020)));
        assert_eq!(parse_month_year("12-2020"), Some((12, 2020)));
        assert_eq!(parse_month_year("0-1996"), Some((0, 1996)));
        assert_eq!(parse_month_year("-1-1996"), Some((-1, 1996)));
    }

    #[test]
    fn rejects_garbage_month_year() {
        assert_eq!(parse_month_year("garbage"), None);
        assert_eq!(parse_month_year(""), None);
    }
}
