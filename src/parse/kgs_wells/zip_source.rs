//! KS-03: the KGS static-ZIP fallback path, used only when the ArcGIS
//! layer itself is unreachable (ArcGIS item resolution or initial
//! metadata/count fetch failing after retries) — never a mid-pipeline
//! switch.
//!
//! Verified live at KS-03: `KGS_WELLS_STATIC_ZIP` is a single ~47MB ZIP
//! containing one `ks_wells.txt`, a quoted-comma-delimited CSV with a
//! header row and a DIFFERENT column set/naming than the ArcGIS layer
//! (`LEASE` not `LEASE_NAME`, `ORIG_OPERATOR` not `OPERATOR_NAME`, plain
//! `LATITUDE`/`LONGITUDE` not `NAD27_LATITUDE`/`NAD27_LONGITUDE`, dates as
//! `DD-MON-YYYY` not `MM/DD/YYYY`) and — notably — **no `WELL_CLASS`
//! field at all**. Treat this path's `raw_data` as its own distinct
//! schema, not a reshaped copy of the ArcGIS one; KS-04's normalizer must
//! branch on `source_format`.

use std::collections::HashMap;
use std::io::Read;
use std::path::Path;

use futures_util::StreamExt;
use sha2::{Digest, Sha256};
use tokio::io::AsyncWriteExt;

use super::membership::classify_kansas_membership;
use super::StagedRow;
use crate::source::retry::{with_retry, RetryPolicy};

#[derive(Debug, thiserror::Error)]
pub enum ZipImportError {
    #[error("download failed: {0}")]
    Download(#[from] reqwest::Error),
    #[error("filesystem error: {0}")]
    Io(#[from] std::io::Error),
    #[error("zip archive error: {0}")]
    Zip(#[from] zip::result::ZipError),
    #[error("csv parse error: {0}")]
    Csv(#[from] csv::Error),
    #[error("expected a single text entry in the ZIP, found {0}")]
    UnexpectedArchiveShape(String),
    #[error("blocking task panicked: {0}")]
    JoinError(#[from] tokio::task::JoinError),
}

pub struct DownloadedZip {
    pub path: std::path::PathBuf,
    pub sha256: String,
    pub content_length: Option<u64>,
    pub last_modified: Option<String>,
}

/// Downloads the ZIP to `dest_path`, retrying the whole download on
/// failure (a static file has no meaningful "resume a batch" concept the
/// way paginated ArcGIS batches do). Returns the file's own checksum and
/// the source's `Last-Modified`/`Content-Length` headers for the
/// ingestion-run manifest.
pub async fn download_zip(
    client: &reqwest::Client,
    url: &str,
    dest_path: &Path,
    retry_policy: RetryPolicy,
) -> Result<DownloadedZip, ZipImportError> {
    with_retry(retry_policy, || async {
        let response = client.get(url).send().await?.error_for_status()?;
        let content_length = response.content_length();
        let last_modified = response
            .headers()
            .get(reqwest::header::LAST_MODIFIED)
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string());

        let mut file = tokio::fs::File::create(dest_path).await?;
        let mut hasher = Sha256::new();
        let mut stream = response.bytes_stream();
        while let Some(chunk) = stream.next().await {
            let chunk = chunk?;
            hasher.update(&chunk);
            file.write_all(&chunk).await?;
        }
        file.flush().await?;

        Ok::<_, ZipImportError>(DownloadedZip {
            path: dest_path.to_path_buf(),
            sha256: format!("{:x}", hasher.finalize()),
            content_length,
            last_modified,
        })
    })
    .await
}

/// Parses the downloaded ZIP's single CSV entry into staged rows. Runs
/// synchronously (the `zip`/`csv` crates are blocking) — call via
/// `tokio::task::spawn_blocking`.
pub fn parse_zip_wells(path: &Path) -> Result<Vec<StagedRow>, ZipImportError> {
    let file = std::fs::File::open(path)?;
    let mut archive = zip::ZipArchive::new(file)?;

    if archive.len() != 1 {
        return Err(ZipImportError::UnexpectedArchiveShape(format!(
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
    let column_index: HashMap<&str, usize> = headers
        .iter()
        .enumerate()
        .map(|(i, name)| (name, i))
        .collect();

    let mut rows = Vec::new();
    for record in reader.records() {
        let record = record?;
        rows.push(zip_record_to_staged_row(&record, &column_index));
    }
    Ok(rows)
}

fn field<'a>(
    record: &'a csv::StringRecord,
    column_index: &HashMap<&str, usize>,
    name: &str,
) -> Option<&'a str> {
    column_index
        .get(name)
        .and_then(|&i| record.get(i))
        .filter(|s| !s.is_empty())
}

fn zip_record_to_staged_row(
    record: &csv::StringRecord,
    column_index: &HashMap<&str, usize>,
) -> StagedRow {
    let kid = field(record, column_index, "KID").map(|s| s.to_string());
    let api_num_nodash = field(record, column_index, "API_NUM_NODASH");
    // The ZIP has no COUNTY column at all — county-based membership
    // fallback is only available via the ArcGIS path; a ZIP-sourced
    // record with a non-Kansas API prefix is always excluded.
    let latitude: Option<f64> =
        field(record, column_index, "LATITUDE").and_then(|s| s.parse().ok());
    let longitude: Option<f64> =
        field(record, column_index, "LONGITUDE").and_then(|s| s.parse().ok());
    let wgs84_lat_lon = match (latitude, longitude) {
        (Some(lat), Some(lon)) => Some((lat, lon)),
        _ => None,
    };

    let membership = classify_kansas_membership(api_num_nodash, None, wgs84_lat_lon);

    let raw_data: serde_json::Map<String, serde_json::Value> = column_index
        .iter()
        .filter_map(|(&name, &i)| {
            record
                .get(i)
                .map(|v| (name.to_string(), serde_json::Value::String(v.to_string())))
        })
        .collect();

    StagedRow {
        source_object_id: None,
        source_kid: kid,
        source_format: "zip",
        raw_data: serde_json::Value::Object(raw_data),
        longitude,
        latitude,
        membership,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_headers() -> csv::StringRecord {
        csv::StringRecord::from(vec![
            "KID",
            "API_NUMBER",
            "API_NUM_NODASH",
            "LEASE",
            "WELL",
            "LATITUDE",
            "LONGITUDE",
            "ORIG_OPERATOR",
            "CURR_OPERATOR",
            "STATUS",
            "STATUS2",
        ])
    }

    fn column_index(headers: &csv::StringRecord) -> HashMap<&str, usize> {
        headers
            .iter()
            .enumerate()
            .map(|(i, name)| (name, i))
            .collect()
    }

    #[test]
    fn zip_record_maps_kansas_well_as_included() {
        let headers = sample_headers();
        let index = column_index(&headers);
        let record = csv::StringRecord::from(vec![
            "1001184201",
            "15-007-20008",
            "15007200080000",
            "Kirkbride 'B'",
            "1",
            "37.2378121",
            "-98.6341308",
            "Petroleum, Inc.",
            "unavailable",
            "D&A",
            "Plugged and Abandoned",
        ]);

        let row = zip_record_to_staged_row(&record, &index);
        assert_eq!(row.source_format, "zip");
        assert_eq!(row.source_object_id, None);
        assert_eq!(row.source_kid.as_deref(), Some("1001184201"));
        assert_eq!(row.latitude, Some(37.2378121));
        assert_eq!(row.longitude, Some(-98.6341308));
        assert_eq!(
            row.membership.status,
            super::super::MembershipStatus::Included
        );
    }

    #[test]
    fn zip_record_with_missing_fields_does_not_panic() {
        let headers = sample_headers();
        let index = column_index(&headers);
        let record = csv::StringRecord::from(vec!["", "", "", "", "", "", "", "", "", "", ""]);
        let row = zip_record_to_staged_row(&record, &index);
        assert_eq!(row.source_kid, None);
        assert_eq!(row.latitude, None);
        assert_eq!(
            row.membership.status,
            super::super::MembershipStatus::ExcludedOtherState
        );
    }
}
