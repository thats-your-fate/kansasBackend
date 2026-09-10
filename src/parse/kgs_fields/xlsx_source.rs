//! KS-08: parses the KGS field-production `.xlsx` files. Confirmed live
//! 2026-09-05 (both the active-file and abandoned-file shapes, by
//! downloading and inspecting the real files listed on
//! `https://www.kgs.ku.edu/Magellan/Field/index.html`) — unlike KS-05/06/07,
//! this header layout is a *confirmed* real schema, not a guess:
//!
//! Active files (`fields_2020_present.xlsx` etc.): `FIELD_NAME`,
//! `PROD_ZONES`, `DISCOVERY`, `TOTAL_OIL_PROD`, `TOTAL_GAS_PROD`,
//! `ACRES`, `TOTAL_WELLS`, then repeating `<YEAR>_OIL_PROD`/
//! `<YEAR>_OIL_WELLS`/`<YEAR>_GAS_PROD`/`<YEAR>_GAS_WELLS` columns per
//! year in the file's range.
//!
//! Abandoned file (`fields_abandoned.xlsx`): `FIELD_NAME`, `PROD_ZONES`,
//! `DISCOVERY`, `TOTAL_OIL_PROD`, `TOTAL_GAS_PROD`, `ACRES`,
//! `TOTAL_WELLS` — lifetime totals only, no per-year columns, no
//! `FIELD_KID` (matching happens by exact `FIELD_NAME` only).

use std::collections::BTreeMap;
use std::io::Cursor;

use calamine::{Data, Reader, Xlsx};

/// (oil_prod, oil_wells, gas_prod, gas_wells) for one year.
pub type AnnualFieldValues = (Option<f64>, Option<i32>, Option<f64>, Option<i32>);

#[derive(Debug, thiserror::Error)]
pub enum XlsxParseError {
    #[error("xlsx open/parse error: {0}")]
    Calamine(#[from] calamine::XlsxError),
    #[error("workbook has no worksheets")]
    NoWorksheet,
}

#[derive(Debug, Clone)]
pub struct RawFieldFileRow {
    pub field_name: String,
    pub producing_zones: Option<String>,
    /// Lifetime totals, present in both file kinds.
    pub total_oil_prod: Option<f64>,
    pub total_gas_prod: Option<f64>,
    /// Per-year rows only, keyed by year: (oil_prod, oil_wells, gas_prod, gas_wells).
    pub annual: BTreeMap<i32, AnnualFieldValues>,
    pub raw_data: serde_json::Value,
}

pub fn parse_field_xlsx(bytes: &[u8]) -> Result<Vec<RawFieldFileRow>, XlsxParseError> {
    let cursor = Cursor::new(bytes.to_vec());
    let mut workbook: Xlsx<_> = Xlsx::new(cursor)?;
    let sheet_name = workbook
        .sheet_names()
        .first()
        .cloned()
        .ok_or(XlsxParseError::NoWorksheet)?;
    let range = workbook.worksheet_range(&sheet_name)?;

    let mut rows_iter = range.rows();
    let header_row = match rows_iter.next() {
        Some(r) => r,
        None => return Ok(Vec::new()),
    };
    let headers: Vec<String> = header_row.iter().map(cell_to_string).collect();

    // Per-year annual columns are named "<YEAR>_OIL_PROD" etc. — parse
    // the year prefix and column kind out of the header text rather than
    // assuming a fixed set of years, since each active-file covers a
    // different year range.
    let mut year_columns: BTreeMap<usize, (i32, &'static str)> = BTreeMap::new();
    for (i, header) in headers.iter().enumerate() {
        if let Some((year, kind)) = parse_year_column(header) {
            year_columns.insert(i, (year, kind));
        }
    }

    let field_name_idx = headers.iter().position(|h| h == "FIELD_NAME");
    let prod_zones_idx = headers.iter().position(|h| h == "PROD_ZONES");
    let total_oil_idx = headers.iter().position(|h| h == "TOTAL_OIL_PROD");
    let total_gas_idx = headers.iter().position(|h| h == "TOTAL_GAS_PROD");

    let mut out = Vec::new();
    for row in rows_iter {
        let Some(field_name) = field_name_idx
            .and_then(|i| row.get(i))
            .map(cell_to_string)
            .filter(|s| !s.trim().is_empty())
        else {
            continue;
        };

        let mut raw_map = BTreeMap::new();
        for (i, header) in headers.iter().enumerate() {
            if let Some(cell) = row.get(i) {
                raw_map.insert(header.clone(), cell_to_string(cell));
            }
        }

        let mut annual: BTreeMap<i32, AnnualFieldValues> = BTreeMap::new();
        for (&col_idx, &(year, kind)) in &year_columns {
            let Some(cell) = row.get(col_idx) else {
                continue;
            };
            let entry = annual.entry(year).or_default();
            match kind {
                "oil_prod" => entry.0 = cell_to_f64(cell),
                "oil_wells" => entry.1 = cell_to_f64(cell).map(|v| v as i32),
                "gas_prod" => entry.2 = cell_to_f64(cell),
                "gas_wells" => entry.3 = cell_to_f64(cell).map(|v| v as i32),
                _ => {}
            }
        }

        out.push(RawFieldFileRow {
            field_name,
            producing_zones: prod_zones_idx
                .and_then(|i| row.get(i))
                .map(cell_to_string)
                .filter(|s| !s.trim().is_empty()),
            total_oil_prod: total_oil_idx.and_then(|i| row.get(i)).and_then(cell_to_f64),
            total_gas_prod: total_gas_idx.and_then(|i| row.get(i)).and_then(cell_to_f64),
            annual,
            raw_data: serde_json::to_value(raw_map).unwrap_or(serde_json::Value::Null),
        });
    }

    Ok(out)
}

fn parse_year_column(header: &str) -> Option<(i32, &'static str)> {
    let (year_str, rest) = header.split_once('_')?;
    let year: i32 = year_str.parse().ok()?;
    if !(1900..=2100).contains(&year) {
        return None;
    }
    let kind = match rest {
        "OIL_PROD" => "oil_prod",
        "OIL_WELLS" => "oil_wells",
        "GAS_PROD" => "gas_prod",
        "GAS_WELLS" => "gas_wells",
        _ => return None,
    };
    Some((year, kind))
}

fn cell_to_string(cell: &Data) -> String {
    match cell {
        Data::Empty => String::new(),
        Data::String(s) => s.trim().to_string(),
        Data::Int(i) => i.to_string(),
        Data::Float(f) => {
            if f.fract() == 0.0 {
                format!("{}", *f as i64)
            } else {
                f.to_string()
            }
        }
        Data::Bool(b) => b.to_string(),
        Data::DateTime(dt) => dt.as_datetime().map(|d| d.to_string()).unwrap_or_default(),
        Data::DateTimeIso(s) | Data::DurationIso(s) => s.clone(),
        Data::Error(e) => format!("{e:?}"),
    }
}

fn cell_to_f64(cell: &Data) -> Option<f64> {
    match cell {
        Data::Empty => None,
        Data::Int(i) => Some(*i as f64),
        Data::Float(f) => Some(*f),
        Data::String(s) if !s.trim().is_empty() => s.trim().parse().ok(),
        _ => None,
    }
}

/// Builds a minimal valid `.xlsx` in memory from a header row + data rows,
/// using inline strings (`t="inlineStr"`) so no `sharedStrings.xml` part
/// is needed — this module's own test-only fixture builder.
/// `tests/kgs_fields_import.rs` has its own copy (integration tests can't
/// see `#[cfg(test)]` items from the lib they link against), same
/// precedent as `tests/kgs_wells_zip_import.rs`'s local `build_fixture_zip`.
#[cfg(test)]
fn build_fixture_xlsx(headers: &[&str], rows: &[Vec<&str>]) -> Vec<u8> {
    use std::io::Write;
    use zip::write::SimpleFileOptions;

    fn row_xml(row_num: usize, cells: &[String]) -> String {
        let mut xml = format!(r#"<row r="{row_num}">"#);
        for (i, value) in cells.iter().enumerate() {
            let col = column_letter(i);
            let escaped = value
                .replace('&', "&amp;")
                .replace('<', "&lt;")
                .replace('>', "&gt;");
            xml.push_str(&format!(
                r#"<c r="{col}{row_num}" t="inlineStr"><is><t>{escaped}</t></is></c>"#
            ));
        }
        xml.push_str("</row>");
        xml
    }

    fn column_letter(mut i: usize) -> String {
        let mut s = String::new();
        loop {
            s.insert(0, (b'A' + (i % 26) as u8) as char);
            if i < 26 {
                break;
            }
            i = i / 26 - 1;
        }
        s
    }

    let mut sheet_rows = String::new();
    sheet_rows.push_str(&row_xml(
        1,
        &headers.iter().map(|s| s.to_string()).collect::<Vec<_>>(),
    ));
    for (i, row) in rows.iter().enumerate() {
        sheet_rows.push_str(&row_xml(
            i + 2,
            &row.iter().map(|s| s.to_string()).collect::<Vec<_>>(),
        ));
    }

    let sheet_xml = format!(
        r#"<?xml version="1.0" encoding="UTF-8" standalone="yes"?><worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main"><sheetData>{sheet_rows}</sheetData></worksheet>"#
    );

    let content_types = r#"<?xml version="1.0" encoding="UTF-8" standalone="yes"?><Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types"><Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/><Default Extension="xml" ContentType="application/xml"/><Override PartName="/xl/workbook.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/><Override PartName="/xl/worksheets/sheet1.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/></Types>"#;
    let root_rels = r#"<?xml version="1.0" encoding="UTF-8" standalone="yes"?><Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships"><Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" Target="xl/workbook.xml"/></Relationships>"#;
    let workbook_xml = r#"<?xml version="1.0" encoding="UTF-8" standalone="yes"?><workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships"><sheets><sheet name="Sheet1" sheetId="1" r:id="rId1"/></sheets></workbook>"#;
    let workbook_rels = r#"<?xml version="1.0" encoding="UTF-8" standalone="yes"?><Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships"><Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" Target="worksheets/sheet1.xml"/></Relationships>"#;

    let mut buf = Vec::new();
    {
        let mut zip = zip::ZipWriter::new(std::io::Cursor::new(&mut buf));
        let options = SimpleFileOptions::default();
        zip.start_file("[Content_Types].xml", options).unwrap();
        zip.write_all(content_types.as_bytes()).unwrap();
        zip.start_file("_rels/.rels", options).unwrap();
        zip.write_all(root_rels.as_bytes()).unwrap();
        zip.start_file("xl/workbook.xml", options).unwrap();
        zip.write_all(workbook_xml.as_bytes()).unwrap();
        zip.start_file("xl/_rels/workbook.xml.rels", options)
            .unwrap();
        zip.write_all(workbook_rels.as_bytes()).unwrap();
        zip.start_file("xl/worksheets/sheet1.xml", options).unwrap();
        zip.write_all(sheet_xml.as_bytes()).unwrap();
        zip.finish().unwrap();
    }
    buf
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_a_year_column_header() {
        assert_eq!(parse_year_column("2020_OIL_PROD"), Some((2020, "oil_prod")));
        assert_eq!(
            parse_year_column("2026_GAS_WELLS"),
            Some((2026, "gas_wells"))
        );
        assert_eq!(parse_year_column("FIELD_NAME"), None);
        assert_eq!(parse_year_column("TOTAL_OIL_PROD"), None);
    }

    #[test]
    fn round_trips_a_hand_built_fixture_workbook() {
        let bytes = build_fixture_xlsx(
            &[
                "FIELD_NAME",
                "PROD_ZONES",
                "TOTAL_OIL_PROD",
                "TOTAL_GAS_PROD",
                "2020_OIL_PROD",
                "2020_OIL_WELLS",
            ],
            &[vec!["TEST FIELD", "MISSISSIPPIAN", "100", "200", "10", "2"]],
        );
        let rows = parse_field_xlsx(&bytes).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].field_name, "TEST FIELD");
        assert_eq!(rows[0].producing_zones.as_deref(), Some("MISSISSIPPIAN"));
        assert_eq!(rows[0].total_oil_prod, Some(100.0));
        assert_eq!(rows[0].total_gas_prod, Some(200.0));
        assert_eq!(
            rows[0].annual.get(&2020),
            Some(&(Some(10.0), Some(2), None, None))
        );
    }
}
