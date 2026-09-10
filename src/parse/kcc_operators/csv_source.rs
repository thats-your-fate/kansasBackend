//! KS-05: parses a KCC operator CSV export.
//!
//! Unverified assumption (documented per task doc §11's KS-05 entry):
//! `kcc.ks.gov` is unreachable from this development environment, so the
//! exact header names below have never been confirmed against a real
//! download. Header matching is deliberately case/spacing-tolerant across
//! several plausible spellings per logical column, rather than a single
//! hardcoded name, so a header-casing difference alone doesn't break the
//! importer — but a genuinely different schema (missing license/name
//! columns entirely) still fails loudly via `CsvParseError`, not silently.

use std::collections::BTreeMap;

use chrono::{DateTime, Utc};
use serde_json::Value;

#[derive(Debug, thiserror::Error)]
pub enum CsvParseError {
    #[error("csv parse error: {0}")]
    Csv(#[from] csv::Error),
    #[error("could not find a license-number column among headers: {0:?}")]
    MissingLicenseColumn(Vec<String>),
    #[error("could not find an operator-name column among headers: {0:?}")]
    MissingNameColumn(Vec<String>),
}

#[derive(Debug, Clone)]
pub struct RawOperatorRow {
    pub kcc_license_number: Option<String>,
    pub reported_company_name: Option<String>,
    pub source_city: Option<String>,
    pub source_updated_at: Option<DateTime<Utc>>,
    pub raw_data: Value,
}

const LICENSE_HEADER_CANDIDATES: &[&str] = &[
    "license number",
    "license",
    "license_no",
    "license no",
    "operator license",
    "kcc license number",
    "license #",
];
const NAME_HEADER_CANDIDATES: &[&str] = &["operator name", "company name", "name", "operator"];
const CITY_HEADER_CANDIDATES: &[&str] = &["city", "operator city"];
const UPDATED_HEADER_CANDIDATES: &[&str] = &[
    "last updated",
    "updated",
    "date updated",
    "last update date",
];

fn find_column(headers: &[String], candidates: &[&str]) -> Option<usize> {
    headers
        .iter()
        .position(|h| candidates.contains(&h.trim().to_lowercase().as_str()))
}

pub fn parse_operator_csv(bytes: &[u8]) -> Result<Vec<RawOperatorRow>, CsvParseError> {
    let mut reader = csv::ReaderBuilder::new()
        .has_headers(true)
        .from_reader(bytes);
    let headers: Vec<String> = reader.headers()?.iter().map(|h| h.to_string()).collect();

    let license_idx = find_column(&headers, LICENSE_HEADER_CANDIDATES)
        .ok_or_else(|| CsvParseError::MissingLicenseColumn(headers.clone()))?;
    let name_idx = find_column(&headers, NAME_HEADER_CANDIDATES)
        .ok_or_else(|| CsvParseError::MissingNameColumn(headers.clone()))?;
    let city_idx = find_column(&headers, CITY_HEADER_CANDIDATES);
    let updated_idx = find_column(&headers, UPDATED_HEADER_CANDIDATES);

    let mut rows = Vec::new();
    for record in reader.records() {
        let record = record?;
        let mut raw_map = BTreeMap::new();
        for (i, header) in headers.iter().enumerate() {
            raw_map.insert(header.clone(), record.get(i).unwrap_or("").to_string());
        }

        let license = record
            .get(license_idx)
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(str::to_string);
        let name = record
            .get(name_idx)
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(str::to_string);
        let city = city_idx
            .and_then(|i| record.get(i))
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(str::to_string);
        let updated = updated_idx
            .and_then(|i| record.get(i))
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .and_then(parse_flexible_date);

        rows.push(RawOperatorRow {
            kcc_license_number: license,
            reported_company_name: name,
            source_city: city,
            source_updated_at: updated,
            raw_data: serde_json::to_value(raw_map)
                .expect("BTreeMap<String,String> always serializes"),
        });
    }

    Ok(rows)
}

/// Tries a small set of plausible date formats. Returns `None` (never a
/// fabricated date) on anything unrecognized — a source date that fails to
/// parse is still visible in `raw_data`.
fn parse_flexible_date(text: &str) -> Option<DateTime<Utc>> {
    use chrono::NaiveDate;
    let formats = ["%m/%d/%Y", "%Y-%m-%d", "%m-%d-%Y"];
    for fmt in formats {
        if let Ok(date) = NaiveDate::parse_from_str(text, fmt) {
            return date.and_hms_opt(0, 0, 0).map(|dt| dt.and_utc());
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_a_well_formed_csv() {
        let csv = "License Number,Operator Name,City,Last Updated\n\
                    1001,Example Operating LLC,Wichita,01/15/2026\n\
                    1002,Sample Resources Inc.,Hays,02/01/2026\n";
        let rows = parse_operator_csv(csv.as_bytes()).unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].kcc_license_number.as_deref(), Some("1001"));
        assert_eq!(
            rows[0].reported_company_name.as_deref(),
            Some("Example Operating LLC")
        );
        assert_eq!(rows[0].source_city.as_deref(), Some("Wichita"));
        assert!(rows[0].source_updated_at.is_some());
    }

    #[test]
    fn is_tolerant_of_header_casing_and_spacing_variants() {
        let csv = "license_no, COMPANY NAME ,city\n2001,Variant Header Co,Salina\n";
        let rows = parse_operator_csv(csv.as_bytes()).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].kcc_license_number.as_deref(), Some("2001"));
        assert_eq!(
            rows[0].reported_company_name.as_deref(),
            Some("Variant Header Co")
        );
    }

    #[test]
    fn missing_license_column_fails_loudly() {
        let csv = "Operator Name,City\nExample LLC,Wichita\n";
        let err = parse_operator_csv(csv.as_bytes()).unwrap_err();
        assert!(matches!(err, CsvParseError::MissingLicenseColumn(_)));
    }

    #[test]
    fn blank_optional_fields_stay_none_not_empty_string() {
        let csv = "License Number,Operator Name,City\n3001,No City Co,\n";
        let rows = parse_operator_csv(csv.as_bytes()).unwrap();
        assert_eq!(rows[0].source_city, None);
    }
}
