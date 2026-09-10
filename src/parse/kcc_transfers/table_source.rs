//! KS-07: maps a parsed HTML results row into a raw approved-transfer
//! record. Same header-tolerance discipline as KS-06's `table_source` —
//! `kcc.ks.gov` is unreachable, so the real column names are unconfirmed.

use serde_json::Value;

use crate::normalize::{normalize_direction, parse_township_or_range};
use crate::source::html_table::TableRow;

#[derive(Debug, Clone)]
pub struct RawTransferRow {
    pub past_operator_name: Option<String>,
    pub past_operator_license: Option<String>,
    pub new_operator_name: Option<String>,
    pub new_operator_license: Option<String>,
    pub received_date_raw: Option<String>,
    pub effective_date_raw: Option<String>,
    pub approved_date_raw: Option<String>,
    pub lease_or_well_label: Option<String>,
    pub section: Option<i16>,
    pub township: Option<i16>,
    pub range: Option<i16>,
    pub range_direction: Option<String>,
    pub district: Option<String>,
    pub county: Option<String>,
    pub raw_data: Value,
}

const PAST_NAME_HEADERS: &[&str] = &[
    "past operator",
    "past operator name",
    "from operator",
    "previous operator",
];
const PAST_LICENSE_HEADERS: &[&str] = &["past license", "past operator license", "from license"];
const NEW_NAME_HEADERS: &[&str] = &["new operator", "new operator name", "to operator"];
const NEW_LICENSE_HEADERS: &[&str] = &["new license", "new operator license", "to license"];
const RECEIVED_HEADERS: &[&str] = &["received date", "date received", "received"];
const EFFECTIVE_HEADERS: &[&str] = &["effective date", "effective"];
const APPROVED_HEADERS: &[&str] = &["approved date", "date approved", "approved"];
const LABEL_HEADERS: &[&str] = &[
    "lease",
    "well",
    "lease/well",
    "lease or well",
    "lease name",
    "well name",
];
const SECTION_HEADERS: &[&str] = &["section", "sec", "sec."];
const TOWNSHIP_HEADERS: &[&str] = &["township", "twp", "twp."];
const RANGE_HEADERS: &[&str] = &["range", "rge", "rge."];
const RANGE_DIR_HEADERS: &[&str] = &["range direction", "rge dir", "r dir"];
const DISTRICT_HEADERS: &[&str] = &["district"];
const COUNTY_HEADERS: &[&str] = &["county"];

pub fn row_to_raw_transfer(row: &TableRow) -> RawTransferRow {
    let (township, _) = row
        .get(TOWNSHIP_HEADERS)
        .map(parse_township_or_range)
        .unwrap_or((None, None));
    let (range, mut range_direction) = row
        .get(RANGE_HEADERS)
        .map(parse_township_or_range)
        .unwrap_or((None, None));
    if range_direction.is_none() {
        range_direction = normalize_direction(row.get(RANGE_DIR_HEADERS));
    }

    RawTransferRow {
        past_operator_name: row.get(PAST_NAME_HEADERS).map(str::to_string),
        past_operator_license: row.get(PAST_LICENSE_HEADERS).map(str::to_string),
        new_operator_name: row.get(NEW_NAME_HEADERS).map(str::to_string),
        new_operator_license: row.get(NEW_LICENSE_HEADERS).map(str::to_string),
        received_date_raw: row.get(RECEIVED_HEADERS).map(str::to_string),
        effective_date_raw: row.get(EFFECTIVE_HEADERS).map(str::to_string),
        approved_date_raw: row.get(APPROVED_HEADERS).map(str::to_string),
        lease_or_well_label: row.get(LABEL_HEADERS).map(str::to_string),
        section: row.get(SECTION_HEADERS).and_then(|s| s.trim().parse().ok()),
        township,
        range,
        range_direction,
        district: row.get(DISTRICT_HEADERS).map(str::to_string),
        county: row.get(COUNTY_HEADERS).map(str::to_string),
        raw_data: serde_json::to_value(&row.cells).unwrap_or(Value::Null),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    fn row_with(cells: &[(&str, &str)]) -> TableRow {
        TableRow {
            cells: cells
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect::<BTreeMap<_, _>>(),
            links: Vec::new(),
        }
    }

    #[test]
    fn parses_a_well_formed_row() {
        let row = row_with(&[
            ("past operator", "Old Co LLC"),
            ("past license", "K1001"),
            ("new operator", "New Co LLC"),
            ("new license", "K2002"),
            ("effective date", "02/01/2026"),
            ("lease", "Smith Lease"),
            ("county", "Barton"),
        ]);
        let transfer = row_to_raw_transfer(&row);
        assert_eq!(transfer.past_operator_license.as_deref(), Some("K1001"));
        assert_eq!(transfer.new_operator_license.as_deref(), Some("K2002"));
        assert_eq!(transfer.lease_or_well_label.as_deref(), Some("Smith Lease"));
        assert_eq!(transfer.effective_date_raw.as_deref(), Some("02/01/2026"));
    }
}
