//! KS-09: parses the KGS all-leases archive. Confirmed live 2026-09-05:
//! `KGS_ALL_LEASES_ZIP` is a single ~23MB ZIP containing one
//! `all_kansas_leases.txt`, a quoted-comma-delimited CSV with header
//! `LEASE_KID,LEASE,DOR_CODE,API_NUMBER,OPERATOR,COUNTY,TOWNSHIP,TWN_DIR,
//! RANGE,RANGE_DIR,SECTION,SPOT,LATITUDE,LONGITUDE,PRODUCES,PRODUCTION,
//! YEAR_START,YEAR_STOP,URL` and 98,639 real rows. `API_NUMBER` may list
//! more than one API number, comma-separated (e.g. one lease record
//! covering several wells) — kept as a raw string here; parsed into
//! individual tokens by `materialize::kgs_leases`, never here, so the
//! staged raw value stays byte-for-byte auditable.

use std::collections::HashMap;
use std::io::Read;
use std::path::Path;

pub use crate::parse::kgs_wells::zip_source::{download_zip, DownloadedZip};

#[derive(Debug, thiserror::Error)]
pub enum LeaseZipError {
    #[error("filesystem error: {0}")]
    Io(#[from] std::io::Error),
    #[error("zip archive error: {0}")]
    Zip(#[from] zip::result::ZipError),
    #[error("csv parse error: {0}")]
    Csv(#[from] csv::Error),
    #[error("expected a single text entry in the ZIP, found {0}")]
    UnexpectedArchiveShape(String),
}

#[derive(Debug, Clone)]
pub struct RawLeaseRow {
    pub lease_kid: Option<String>,
    pub rejection_reason: Option<&'static str>,
    pub reported_lease_name: Option<String>,
    pub dor_code: Option<String>,
    pub api_number_raw: Option<String>,
    pub operator: Option<String>,
    pub county: Option<String>,
    pub township: Option<i16>,
    pub township_direction: Option<String>,
    pub range: Option<i16>,
    pub range_direction: Option<String>,
    pub section: Option<i16>,
    pub spot: Option<String>,
    pub source_latitude: Option<f64>,
    pub source_longitude: Option<f64>,
    pub produces_raw: Option<String>,
    pub production: Option<f64>,
    pub year_start: Option<i32>,
    pub year_stop: Option<i32>,
    pub source_url: Option<String>,
    pub raw_data: serde_json::Value,
}

pub fn parse_lease_zip(path: &Path) -> Result<Vec<RawLeaseRow>, LeaseZipError> {
    let file = std::fs::File::open(path)?;
    let mut archive = zip::ZipArchive::new(file)?;
    if archive.len() != 1 {
        return Err(LeaseZipError::UnexpectedArchiveShape(format!(
            "{} entries (expected exactly 1)",
            archive.len()
        )));
    }

    let mut entry = archive.by_index(0)?;
    let mut contents = String::new();
    entry.read_to_string(&mut contents)?;
    drop(entry);

    let mut reader = csv::ReaderBuilder::new()
        .has_headers(true)
        .from_reader(contents.as_bytes());
    let headers = reader.headers()?.clone();
    let column_index: HashMap<String, usize> = headers
        .iter()
        .enumerate()
        .map(|(i, name)| (name.to_string(), i))
        .collect();

    let mut rows = Vec::new();
    for record in reader.records() {
        let record = record?;
        rows.push(record_to_raw_lease(&record, &column_index));
    }
    Ok(rows)
}

fn field<'a>(
    record: &'a csv::StringRecord,
    column_index: &HashMap<String, usize>,
    name: &str,
) -> Option<&'a str> {
    column_index
        .get(name)
        .and_then(|&i| record.get(i))
        .map(str::trim)
        .filter(|s| !s.is_empty())
}

fn record_to_raw_lease(
    record: &csv::StringRecord,
    column_index: &HashMap<String, usize>,
) -> RawLeaseRow {
    let lease_kid = field(record, column_index, "LEASE_KID").map(str::to_string);
    let mut raw_map = std::collections::BTreeMap::new();
    for (name, &i) in column_index {
        if let Some(v) = record.get(i) {
            raw_map.insert(name.clone(), v.to_string());
        }
    }

    RawLeaseRow {
        rejection_reason: if lease_kid.is_none() {
            Some("no LEASE_KID present in this row")
        } else {
            None
        },
        lease_kid,
        reported_lease_name: field(record, column_index, "LEASE").map(str::to_string),
        dor_code: field(record, column_index, "DOR_CODE").map(str::to_string),
        api_number_raw: field(record, column_index, "API_NUMBER").map(str::to_string),
        operator: field(record, column_index, "OPERATOR").map(str::to_string),
        county: field(record, column_index, "COUNTY").map(str::to_string),
        township: field(record, column_index, "TOWNSHIP").and_then(|s| s.parse().ok()),
        township_direction: field(record, column_index, "TWN_DIR").map(str::to_string),
        range: field(record, column_index, "RANGE").and_then(|s| s.parse().ok()),
        range_direction: field(record, column_index, "RANGE_DIR").map(str::to_string),
        section: field(record, column_index, "SECTION").and_then(|s| s.parse().ok()),
        spot: field(record, column_index, "SPOT").map(str::to_string),
        source_latitude: field(record, column_index, "LATITUDE").and_then(|s| s.parse().ok()),
        source_longitude: field(record, column_index, "LONGITUDE").and_then(|s| s.parse().ok()),
        produces_raw: field(record, column_index, "PRODUCES").map(str::to_string),
        production: field(record, column_index, "PRODUCTION").and_then(|s| s.parse().ok()),
        year_start: field(record, column_index, "YEAR_START").and_then(|s| s.parse().ok()),
        year_stop: field(record, column_index, "YEAR_STOP").and_then(|s| s.parse().ok()),
        source_url: field(record, column_index, "URL").map(str::to_string),
        raw_data: serde_json::to_value(raw_map).unwrap_or(serde_json::Value::Null),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn write_fixture_zip(path: &Path, csv_body: &str) {
        let file = std::fs::File::create(path).unwrap();
        let mut zip = zip::ZipWriter::new(file);
        zip.start_file(
            "all_kansas_leases.txt",
            zip::write::SimpleFileOptions::default(),
        )
        .unwrap();
        use std::io::Write;
        zip.write_all(csv_body.as_bytes()).unwrap();
        zip.finish().unwrap();
    }

    #[test]
    fn parses_a_real_shaped_row_including_multi_api_list() {
        let dir = std::env::temp_dir().join(format!("ks_lease_zip_test_{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        let zip_path = dir.join("leases.zip");
        let csv_body = "\"LEASE_KID\",\"LEASE\",\"DOR_CODE\",\"API_NUMBER\",\"OPERATOR\",\"COUNTY\",\"TOWNSHIP\",\"TWN_DIR\",\"RANGE\",\"RANGE_DIR\",\"SECTION\",\"SPOT\",\"LATITUDE\",\"LONGITUDE\",\"PRODUCES\",\"PRODUCTION\",\"YEAR_START\",\"YEAR_STOP\",\"URL\"\r\n\"1001135839\",\"SNYDER\",\"122608\",\"15-131-20143, 15-131-20089-0002\",\"Buckeye West, LLC\",\"Nemaha\",\"1\",\"S\",\"14\",\"E\",\"1\",\"NENENE\",\"39.999519\",\"-95.78905\",\"OIL\",\"40859.55\",\"1985\",\"2026\",\"https://example/1001135839\"\r\n";
        write_fixture_zip(&zip_path, csv_body);

        let rows = parse_lease_zip(&zip_path).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].lease_kid.as_deref(), Some("1001135839"));
        assert_eq!(
            rows[0].api_number_raw.as_deref(),
            Some("15-131-20143, 15-131-20089-0002")
        );
        assert_eq!(rows[0].produces_raw.as_deref(), Some("OIL"));
        assert_eq!(rows[0].year_start, Some(1985));
        assert_eq!(rows[0].year_stop, Some(2026));

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn row_with_no_lease_kid_is_rejected() {
        let dir = std::env::temp_dir().join(format!("ks_lease_zip_test_{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        let zip_path = dir.join("leases.zip");
        let csv_body = "\"LEASE_KID\",\"LEASE\"\r\n\"\",\"NO KID LEASE\"\r\n";
        write_fixture_zip(&zip_path, csv_body);

        let rows = parse_lease_zip(&zip_path).unwrap();
        assert_eq!(rows.len(), 1);
        assert!(rows[0].lease_kid.is_none());
        assert!(rows[0].rejection_reason.is_some());

        std::fs::remove_dir_all(&dir).ok();
    }
}
