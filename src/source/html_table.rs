//! KS-06/KS-07: a generic HTML results-table parser, shared by the KCC
//! drilling-intent and approved-transfer importers. Both sources are
//! documented as search-result pages rather than confirmed CSV downloads
//! (unlike KS-05's operator page) — `kcc.ks.gov` is unreachable from this
//! development environment, so this is a best-effort parser for "a table
//! with a header row, each subsequent row one record," not a confirmed
//! shape. See docs/ops/ks-06-kcc-intents-unverified.md and
//! docs/ops/ks-07-kcc-transfers-unverified.md.

use std::collections::BTreeMap;

use scraper::{Html, Selector};

#[derive(Debug, thiserror::Error)]
pub enum TableParseError {
    #[error("no <table> found on the page")]
    NoTableFound,
    #[error("table has no header row (<th> cells)")]
    NoHeaderRow,
}

#[derive(Debug, Clone, Default)]
pub struct TableRow {
    /// Header-keyed cell text, header lowercased/trimmed.
    pub cells: BTreeMap<String, String>,
    /// Every `<a href>` found anywhere in the row, in document order —
    /// callers pick out whichever one is the document link (a row may
    /// have zero, one, or more).
    pub links: Vec<String>,
}

impl TableRow {
    pub fn get(&self, candidates: &[&str]) -> Option<&str> {
        for candidate in candidates {
            if let Some(v) = self.cells.get(*candidate) {
                if !v.trim().is_empty() {
                    return Some(v.as_str());
                }
            }
        }
        None
    }
}

/// Parses the first `<table>` on the page whose first row looks like a
/// header (has at least one `<th>`, or a plain first `<tr>` used as an
/// implicit header). Every following `<tr>` becomes one `TableRow`,
/// header-aligned by column position (not by name — HTML tables don't
/// repeat header text per cell).
pub fn parse_first_table(html: &str, base_url: &str) -> Result<Vec<TableRow>, TableParseError> {
    let base = url::Url::parse(base_url).ok();
    let document = Html::parse_document(html);
    let table_selector = Selector::parse("table").expect("valid selector");
    let row_selector = Selector::parse("tr").expect("valid selector");
    let cell_selector = Selector::parse("td,th").expect("valid selector");
    let link_selector = Selector::parse("a").expect("valid selector");

    let table = document
        .select(&table_selector)
        .next()
        .ok_or(TableParseError::NoTableFound)?;
    let mut rows_iter = table.select(&row_selector);

    // The first row is always treated as the header, whether or not it
    // uses <th> — real-world markup is inconsistent about this, and a
    // position-based header is the only assumption that can be applied
    // uniformly without guessing at the source's actual semantics.
    let header_row = rows_iter.next().ok_or(TableParseError::NoHeaderRow)?;
    let headers: Vec<String> = header_row
        .select(&cell_selector)
        .map(|c| c.text().collect::<Vec<_>>().join(" ").trim().to_lowercase())
        .collect();
    if headers.is_empty() {
        return Err(TableParseError::NoHeaderRow);
    }

    let mut rows = Vec::new();
    let mut process = |tr: scraper::ElementRef| {
        let mut cells = BTreeMap::new();
        for (i, cell) in tr.select(&cell_selector).enumerate() {
            if let Some(header) = headers.get(i) {
                let text = cell.text().collect::<Vec<_>>().join(" ").trim().to_string();
                cells.insert(header.clone(), text);
            }
        }
        let links: Vec<String> = tr
            .select(&link_selector)
            .filter_map(|a| a.value().attr("href"))
            .filter_map(|href| match &base {
                Some(b) => b.join(href).ok().map(|u| u.to_string()),
                None => Some(href.to_string()),
            })
            .collect();
        rows.push(TableRow { cells, links });
    };

    for tr in rows_iter {
        process(tr);
    }

    Ok(rows)
}

#[cfg(test)]
mod tests {
    use super::*;

    const BASE: &str = "https://www.kcc.ks.gov/notices-of-intents-search";

    #[test]
    fn parses_a_table_with_th_header_and_link_column() {
        let html = r#"
            <html><body>
            <table>
                <tr><th>Permit Date</th><th>County</th><th>Document</th></tr>
                <tr>
                    <td>01/15/2026</td>
                    <td>Barton</td>
                    <td><a href="/docs/2026-000123.pdf">2026-000123.pdf</a></td>
                </tr>
            </table>
            </body></html>
        "#;
        let rows = parse_first_table(html, BASE).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].get(&["permit date"]), Some("01/15/2026"));
        assert_eq!(rows[0].get(&["county"]), Some("Barton"));
        assert_eq!(
            rows[0].links,
            vec!["https://www.kcc.ks.gov/docs/2026-000123.pdf".to_string()]
        );
    }

    #[test]
    fn no_table_is_an_error() {
        let html = "<html><body><p>No results</p></body></html>";
        let err = parse_first_table(html, BASE).unwrap_err();
        assert!(matches!(err, TableParseError::NoTableFound));
    }

    #[test]
    fn table_with_no_th_still_parses_using_first_row_as_header() {
        let html = r#"
            <html><body>
            <table>
                <tr><td>County</td><td>District</td></tr>
                <tr><td>Barton</td><td>1</td></tr>
            </table>
            </body></html>
        "#;
        let rows = parse_first_table(html, BASE).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].get(&["county"]), Some("Barton"));
    }
}
