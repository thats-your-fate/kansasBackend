//! KS-06: maps a parsed HTML results row (`source::html_table::TableRow`)
//! into a raw drilling-intent record. Header names are matched
//! tolerantly across several plausible spellings, same discipline as
//! KS-05's `csv_source` — the real column names are unconfirmed
//! (kcc.ks.gov is unreachable).

use serde_json::Value;

use crate::normalize::{normalize_direction, parse_township_or_range};
use crate::source::html_table::TableRow;

#[derive(Debug, Clone)]
pub struct RawIntentRow {
    pub source_filename: Option<String>,
    pub rejection_reason: Option<&'static str>,
    pub permit_date_raw: Option<String>,
    pub kcc_district: Option<String>,
    pub county: Option<String>,
    pub section: Option<i16>,
    pub township: Option<i16>,
    pub township_direction: Option<String>,
    pub range: Option<i16>,
    pub range_direction: Option<String>,
    pub operator_license: Option<String>,
    pub source_document_url: Option<String>,
    pub reported_lease_or_well_name: Option<String>,
    pub raw_data: Value,
}

const DISTRICT_HEADERS: &[&str] = &["district", "kcc district"];
const COUNTY_HEADERS: &[&str] = &["county"];
const SECTION_HEADERS: &[&str] = &["section", "sec", "sec."];
const TOWNSHIP_HEADERS: &[&str] = &["township", "twp", "twp."];
const RANGE_HEADERS: &[&str] = &["range", "rge", "rge."];
const RANGE_DIR_HEADERS: &[&str] = &["range direction", "rge dir", "r dir", "r. dir."];
const TOWNSHIP_DIR_HEADERS: &[&str] = &["township direction", "twp dir", "t dir", "t. dir."];
const LICENSE_HEADERS: &[&str] = &["operator license", "license", "license number", "license #"];
const DATE_HEADERS: &[&str] = &[
    "permit date",
    "date",
    "posted date",
    "filed date",
    "intent date",
];
const NAME_HEADERS: &[&str] = &["lease name", "well name", "lease/well", "lease", "well"];

pub fn row_to_raw_intent(row: &TableRow) -> RawIntentRow {
    let (township, mut township_direction) = row
        .get(TOWNSHIP_HEADERS)
        .map(parse_township_or_range)
        .unwrap_or((None, None));
    let (range, mut range_direction) = row
        .get(RANGE_HEADERS)
        .map(parse_township_or_range)
        .unwrap_or((None, None));
    if township_direction.is_none() {
        township_direction = normalize_direction(row.get(TOWNSHIP_DIR_HEADERS));
    }
    if range_direction.is_none() {
        range_direction = normalize_direction(row.get(RANGE_DIR_HEADERS));
    }

    let document_url = row.links.first().cloned();
    let source_filename = document_url.as_deref().map(filename_from_url);

    let raw_data = serde_json::to_value(&row.cells).unwrap_or(Value::Null);

    RawIntentRow {
        rejection_reason: if source_filename.is_none() {
            Some("no document link found in row — cannot derive a source filename identity")
        } else {
            None
        },
        source_filename,
        permit_date_raw: row.get(DATE_HEADERS).map(str::to_string),
        kcc_district: row.get(DISTRICT_HEADERS).map(str::to_string),
        county: row.get(COUNTY_HEADERS).map(str::to_string),
        section: row.get(SECTION_HEADERS).and_then(|s| s.trim().parse().ok()),
        township,
        township_direction,
        range,
        range_direction,
        operator_license: row.get(LICENSE_HEADERS).map(str::to_string),
        source_document_url: document_url,
        reported_lease_or_well_name: row.get(NAME_HEADERS).map(str::to_string),
        raw_data,
    }
}

fn filename_from_url(url: &str) -> String {
    url.rsplit('/').next().unwrap_or(url).to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    fn row_with(cells: &[(&str, &str)], links: &[&str]) -> TableRow {
        TableRow {
            cells: cells
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect::<BTreeMap<_, _>>(),
            links: links.iter().map(|s| s.to_string()).collect(),
        }
    }

    #[test]
    fn extracts_filename_from_first_link() {
        let row = row_with(
            &[("county", "Barton"), ("permit date", "01/15/2026")],
            &["https://www.kcc.ks.gov/docs/2026-000123.pdf"],
        );
        let intent = row_to_raw_intent(&row);
        assert_eq!(intent.source_filename.as_deref(), Some("2026-000123.pdf"));
        assert_eq!(intent.rejection_reason, None);
        assert_eq!(intent.county.as_deref(), Some("Barton"));
    }

    #[test]
    fn row_with_no_link_is_rejected() {
        let row = row_with(&[("county", "Barton")], &[]);
        let intent = row_to_raw_intent(&row);
        assert_eq!(intent.source_filename, None);
        assert!(intent.rejection_reason.is_some());
    }

    #[test]
    fn parses_combined_township_direction_cell() {
        let row = row_with(
            &[("township", "15S"), ("range", "10E")],
            &["https://x/a.pdf"],
        );
        let intent = row_to_raw_intent(&row);
        assert_eq!(intent.township, Some(15));
        assert_eq!(intent.township_direction.as_deref(), Some("S"));
        assert_eq!(intent.range, Some(10));
        assert_eq!(intent.range_direction.as_deref(), Some("E"));
    }
}
