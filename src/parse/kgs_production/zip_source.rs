//! KS-10: streams the KGS monthly lease-production archives. Confirmed
//! live 2026-09-05: each decade file (`oil_leases_2020_present.zip` etc.)
//! is a single ZIP containing one large CSV (the real 2020-present oil
//! file alone is ~190MB uncompressed, ~670K rows) — read as a stream
//! directly from the ZIP entry, never buffered into one `String` the way
//! KS-09's much smaller lease file was, and staged in batches rather than
//! one `INSERT` per row (the sequential-round-trip scaling problem
//! documented and deliberately left unfixed at KS-08/KS-09 would be
//! genuinely unusable at this row count).

use sha2::{Digest, Sha256};

use crate::normalize::kgs_production::parse_month_year;

#[derive(Debug, thiserror::Error)]
pub enum ProductionZipError {
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
pub struct RawProductionRow {
    pub lease_kid: Option<String>,
    pub rejection_reason: Option<&'static str>,
    pub reported_lease_name: Option<String>,
    pub dor_code: Option<String>,
    pub api_number_raw: Option<String>,
    pub field_name: Option<String>,
    pub producing_zone: Option<String>,
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
    pub month_year_raw: Option<String>,
    pub year: Option<i32>,
    pub month: Option<i16>,
    pub product_raw: Option<String>,
    pub reported_contributing_wells: Option<i32>,
    pub production_volume: Option<f64>,
    pub source_url: Option<String>,
    pub raw_data: serde_json::Value,
    pub source_record_hash: String,
}

/// Opens the ZIP's single entry and streams it as CSV, invoking
/// `on_batch` synchronously once per `batch_size` records (and once more
/// for any final partial batch) rather than collecting the whole file
/// into memory first. Entirely synchronous and self-contained — callers
/// run it inside `spawn_blocking` and have `on_batch` forward each batch
/// to an async task (e.g. over an `mpsc` channel) for the actual
/// (async, IO-bound) database insert, so parsing the next batch overlaps
/// with writing the previous one instead of blocking on it.
pub fn stream_production_csv<F>(
    path: &std::path::Path,
    batch_size: usize,
    mut on_batch: F,
) -> Result<(), ProductionZipError>
where
    F: FnMut(Vec<RawProductionRow>) -> Result<(), ProductionZipError>,
{
    let file = std::fs::File::open(path)?;
    let mut archive = zip::ZipArchive::new(file)?;
    if archive.len() != 1 {
        return Err(ProductionZipError::UnexpectedArchiveShape(format!(
            "{} entries (expected exactly 1)",
            archive.len()
        )));
    }
    let entry = archive.by_index(0)?;
    let mut reader = csv::ReaderBuilder::new()
        .has_headers(true)
        .from_reader(entry);
    let headers: Vec<String> = reader.headers()?.iter().map(str::to_string).collect();

    let mut batch = Vec::with_capacity(batch_size);
    let mut record = csv::StringRecord::new();
    loop {
        if !reader.read_record(&mut record)? {
            break;
        }
        batch.push(record_to_raw_row(&record, &headers));
        if batch.len() >= batch_size {
            on_batch(std::mem::replace(
                &mut batch,
                Vec::with_capacity(batch_size),
            ))?;
        }
    }
    if !batch.is_empty() {
        on_batch(batch)?;
    }
    Ok(())
}

pub fn record_to_raw_row(record: &csv::StringRecord, headers: &[String]) -> RawProductionRow {
    let idx = |name: &str| headers.iter().position(|h| h == name);
    let field = |name: &str| -> Option<&str> {
        idx(name)
            .and_then(|i| record.get(i))
            .map(str::trim)
            .filter(|s| !s.is_empty())
    };

    let lease_kid = field("LEASE_KID").map(str::to_string);
    let month_year_raw = field("MONTH-YEAR").map(str::to_string);
    let (month, year) = month_year_raw
        .as_deref()
        .and_then(parse_month_year)
        .map_or((None, None), |(m, y)| (Some(m), Some(y)));

    let mut raw_map = std::collections::BTreeMap::new();
    for (i, header) in headers.iter().enumerate() {
        if let Some(v) = record.get(i) {
            raw_map.insert(header.clone(), v.to_string());
        }
    }
    let raw_data = serde_json::to_value(&raw_map).unwrap_or(serde_json::Value::Null);
    let mut hasher = Sha256::new();
    hasher.update(raw_data.to_string().as_bytes());
    let source_record_hash = format!("{:x}", hasher.finalize());

    RawProductionRow {
        rejection_reason: if lease_kid.is_none() {
            Some("no LEASE_KID present in this row")
        } else {
            None
        },
        lease_kid,
        reported_lease_name: field("LEASE").map(str::to_string),
        dor_code: field("DOR_CODE").map(str::to_string),
        api_number_raw: field("API_NUMBER").map(str::to_string),
        field_name: field("FIELD").map(str::to_string),
        producing_zone: field("PRODUCING_ZONE").map(str::to_string),
        operator: field("OPERATOR").map(str::to_string),
        county: field("COUNTY").map(str::to_string),
        township: field("TOWNSHIP").and_then(|s| s.parse().ok()),
        township_direction: field("TWN_DIR").map(str::to_string),
        range: field("RANGE").and_then(|s| s.parse().ok()),
        range_direction: field("RANGE_DIR").map(str::to_string),
        section: field("SECTION").and_then(|s| s.parse().ok()),
        spot: field("SPOT").map(str::to_string),
        source_latitude: field("LATITUDE").and_then(|s| s.parse().ok()),
        source_longitude: field("LONGITUDE").and_then(|s| s.parse().ok()),
        month_year_raw,
        year,
        month,
        product_raw: field("PRODUCT").map(str::to_string),
        reported_contributing_wells: field("WELLS")
            .and_then(|s| s.parse::<f64>().ok())
            .map(|v| v as i32),
        production_volume: field("PRODUCTION").and_then(|s| s.parse().ok()),
        source_url: field("URL").map(str::to_string),
        raw_data,
        source_record_hash,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn maps_a_real_shaped_record() {
        let headers: Vec<String> = [
            "LEASE_KID",
            "LEASE",
            "DOR_CODE",
            "API_NUMBER",
            "FIELD",
            "PRODUCING_ZONE",
            "OPERATOR",
            "COUNTY",
            "TOWNSHIP",
            "TWN_DIR",
            "RANGE",
            "RANGE_DIR",
            "SECTION",
            "SPOT",
            "LATITUDE",
            "LONGITUDE",
            "MONTH-YEAR",
            "PRODUCT",
            "WELLS",
            "PRODUCTION",
            "URL",
        ]
        .iter()
        .map(|s| s.to_string())
        .collect();
        let record = csv::StringRecord::from(vec![
            "1001135839",
            "SNYDER",
            "122608",
            "15-131-20143",
            "KANASKA",
            "Lansing Group",
            "Buckeye West, LLC",
            "Nemaha",
            "1",
            "S",
            "14",
            "E",
            "1",
            "NENENE",
            "39.999519",
            "-95.78905",
            "12-2020",
            "O",
            "2",
            "161.44",
            "https://example/1001135839",
        ]);

        let row = record_to_raw_row(&record, &headers);
        assert_eq!(row.lease_kid.as_deref(), Some("1001135839"));
        assert_eq!(row.month, Some(12));
        assert_eq!(row.year, Some(2020));
        assert_eq!(row.product_raw.as_deref(), Some("O"));
        assert_eq!(row.production_volume, Some(161.44));
        assert_eq!(row.reported_contributing_wells, Some(2));
    }

    #[test]
    fn row_with_no_lease_kid_is_rejected() {
        let headers: Vec<String> = ["LEASE_KID", "LEASE"]
            .iter()
            .map(|s| s.to_string())
            .collect();
        let record = csv::StringRecord::from(vec!["", "NO KID LEASE"]);
        let row = record_to_raw_row(&record, &headers);
        assert!(row.lease_kid.is_none());
        assert!(row.rejection_reason.is_some());
    }
}
