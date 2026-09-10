//! KS-15: extracts well-document metadata evidence from KGS's real,
//! confirmed-live "Qualified Well Database" per-well detail page
//! (`https://chasm.kgs.ku.edu/ords/qualified.well_page.DisplayWell?f_kid={KID}`,
//! fetched directly and inspected during this task's discovery — see
//! `docs/ops/ks-15-kgs-documents-live-smoke.md`).
//!
//! That page does **not** expose a downloadable per-document manifest to
//! an unauthenticated fetch: the actual scanned documents live behind
//! Walter's Digital Library (`kgslibrary.com`), which the page itself
//! labels "WDL login required," and KGS's own `robots.txt` explicitly
//! disallows crawling the underlying scan repositories
//! (`/PRS/ACO/`, `/PRS/Documents`, `/WellLogs/`, etc. — confirmed
//! directly, 2026-09-05). This module extracts exactly the two kinds of
//! *public, unauthenticated* evidence the page does expose: (1) the WDL
//! library reference itself, recorded as a `login_required` document
//! (never fetched further — "do not attempt to acquire documents from
//! login-only libraries"), and (2) the "Tops Data" table's own document
//! citations (e.g. "Completion Report" + a real date), which are visible
//! without any login at all.

use scraper::{Html, Selector};
use sha2::{Digest, Sha256};
use sqlx::PgPool;
use uuid::Uuid;

use crate::normalize::kgs_documents::{
    classify_access_state, classify_document_type, AccessState, DocumentType,
};
use crate::source::ingestion_run::{self, DownloadedPayload};
use crate::source::registry;

#[derive(Debug, thiserror::Error)]
pub enum ImportError {
    #[error("database error: {0}")]
    Database(#[from] sqlx::Error),
    #[error("ingestion run error: {0}")]
    IngestionRun(#[from] ingestion_run::IngestionRunError),
    #[error("http error: {0}")]
    Http(#[from] reqwest::Error),
    #[error("source not registered: {0}")]
    SourceNotRegistered(&'static str),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ImportStatus {
    Completed,
    SkippedIdenticalChecksum,
}

#[derive(Debug, Clone)]
pub struct ImportOutcome {
    pub run_id: Uuid,
    pub status: ImportStatus,
    pub staged_count: i64,
}

/// Fetches and stages one well's document evidence (KS-15 is
/// per-well by design — see this module's own doc comment on why no
/// bulk document-listing endpoint is used). Idempotent: an unchanged
/// page (identical content checksum) skips staging entirely rather than
/// creating a duplicate run.
pub async fn run_document_import(
    pool: &PgPool,
    client: &reqwest::Client,
    detail_page_base: &str,
    kid: &str,
    api_number: Option<&str>,
) -> Result<ImportOutcome, ImportError> {
    let source = registry::find_source_by_code(pool, "KGS_DOCUMENTS")
        .await?
        .ok_or(ImportError::SourceNotRegistered("KGS_DOCUMENTS"))?;

    let url = format!("{detail_page_base}?f_kid={kid}");
    let response = client.get(&url).send().await?;
    // KS-17: recorded for "source HTTP/content-type changes" monitoring
    // — a real page-structure change often shows up as a content-type
    // change (e.g. an HTML page starting to redirect to a login page)
    // before anyone notices the actual scraped fields are wrong.
    let content_type = response
        .headers()
        .get(reqwest::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .map(str::to_owned);
    let html = response.text().await?;
    let checksum = format!("{:x}", Sha256::digest(html.as_bytes()));

    if let Some(run_id) =
        ingestion_run::find_completed_run_by_checksum(pool, source.id, &checksum).await?
    {
        return Ok(ImportOutcome {
            run_id,
            status: ImportStatus::SkippedIdenticalChecksum,
            staged_count: 0,
        });
    }

    let run_id = ingestion_run::start_run(
        pool,
        DownloadedPayload {
            source_id: source.id,
            source_url: &url,
            retrieved_at: chrono::Utc::now(),
            source_modified_at: None,
            payload_checksum: &checksum,
            schema_fingerprint: None,
            parser_version: "ks-15-v1",
        },
    )
    .await?;
    if let Some(content_type) = content_type {
        ingestion_run::record_response_content_type(pool, run_id, &content_type).await?;
    }

    let evidence = extract_document_evidence(&html, kid, api_number);
    for item in &evidence {
        let raw_data = serde_json::json!({
            "document_type_raw": item.document_type_raw,
            "kgs_kid": item.kgs_kid,
            "api_number_raw": item.api_number_raw,
            "source_url": item.source_url,
            "filing_date_raw": item.filing_date_raw,
            "access_state": item.access_state.as_db_str(),
        });
        let record_hash = format!(
            "{:x}",
            Sha256::digest(format!("{}{}", item.source_document_id, item.source_url).as_bytes())
        );
        sqlx::query(
            r#"
            INSERT INTO ks_well_document_raw_snapshot (ingestion_run_id, document_source, source_document_id, raw_data, source_record_hash)
            VALUES ($1, 'KGS_DOCUMENTS', $2, $3, $4)
            "#,
        )
        .bind(run_id)
        .bind(&item.source_document_id)
        .bind(&raw_data)
        .bind(&record_hash)
        .execute(pool)
        .await?;
    }

    ingestion_run::complete_run(
        pool,
        run_id,
        evidence.len() as i64,
        0,
        None,
        None,
        serde_json::json!({ "kid": kid }),
    )
    .await?;

    Ok(ImportOutcome {
        run_id,
        status: ImportStatus::Completed,
        staged_count: evidence.len() as i64,
    })
}

#[derive(Debug, Clone, PartialEq)]
pub struct DocumentEvidence {
    pub source_document_id: String,
    pub document_type_raw: String,
    pub document_type: DocumentType,
    pub kgs_kid: String,
    pub api_number_raw: Option<String>,
    pub source_url: String,
    pub filing_date_raw: Option<String>,
    pub access_state: AccessState,
}

/// Parses a real KGS well-detail page (`DisplayWell?f_kid=...`) into its
/// public document evidence. `kid`/`api_number` are passed in rather than
/// re-parsed from the page, since the caller already knows them (this is
/// always fetched per-well, by KID).
pub fn extract_document_evidence(
    html: &str,
    kid: &str,
    api_number: Option<&str>,
) -> Vec<DocumentEvidence> {
    let document = Html::parse_document(html);
    let mut evidence = Vec::new();

    evidence.extend(extract_wdl_reference(&document, kid, api_number));
    evidence.extend(extract_tops_data_citations(&document, kid, api_number));

    evidence
}

fn extract_wdl_reference(
    document: &Html,
    kid: &str,
    api_number: Option<&str>,
) -> Option<DocumentEvidence> {
    let link_selector = Selector::parse("a").expect("valid selector");
    for link in document.select(&link_selector) {
        let Some(href) = link.value().attr("href") else {
            continue;
        };
        let text = link.text().collect::<Vec<_>>().join(" ");
        let access_state = classify_access_state(href, Some(&text));
        if access_state == AccessState::LoginRequired {
            return Some(DocumentEvidence {
                source_document_id: format!("wdl:{kid}"),
                document_type_raw: text,
                document_type: DocumentType::Other,
                kgs_kid: kid.to_owned(),
                api_number_raw: api_number.map(str::to_owned),
                source_url: href.to_owned(),
                filing_date_raw: None,
                access_state,
            });
        }
    }
    None
}

/// The "Tops Data" table cites a source document (e.g. "Completion
/// Report") and a date per formation row — the same citation repeats
/// once per formation top on a real page (confirmed: 7 identical
/// "Completion Report" / "Apr-15-1999" rows on KID 1001190703's real
/// page), so this dedupes to one evidence record per distinct
/// (label, date) pair rather than one per formation row.
fn extract_tops_data_citations(
    document: &Html,
    kid: &str,
    api_number: Option<&str>,
) -> Vec<DocumentEvidence> {
    let table_selector = Selector::parse("table").expect("valid selector");
    let row_selector = Selector::parse("tr").expect("valid selector");
    let cell_selector = Selector::parse("td,th").expect("valid selector");

    let mut seen = std::collections::HashSet::new();
    let mut out = Vec::new();

    for table in document.select(&table_selector) {
        let mut rows = table.select(&row_selector);
        let Some(header_row) = rows.next() else {
            continue;
        };
        let headers: Vec<String> = header_row
            .select(&cell_selector)
            .map(|c| c.text().collect::<Vec<_>>().join(" ").trim().to_lowercase())
            .collect();
        // Exact match, not `contains` — the Tops Data table's own
        // "Formation (source name)" header would otherwise false-positive
        // match "source" too (confirmed: this was a real bug caught by
        // this file's own test, not found by inspection).
        let Some(source_idx) = headers.iter().position(|h| h == "source") else {
            continue;
        };
        let Some(updated_idx) = headers.iter().position(|h| h == "updated") else {
            continue;
        };

        for row in rows {
            let cells: Vec<String> = row
                .select(&cell_selector)
                .map(|c| c.text().collect::<Vec<_>>().join(" ").trim().to_string())
                .collect();
            let Some(label) = cells
                .get(source_idx)
                .filter(|c| !c.is_empty() && *c != "\u{a0}")
            else {
                continue;
            };
            let date_raw = cells.get(updated_idx).filter(|c| !c.is_empty()).cloned();
            let key = (label.clone(), date_raw.clone());
            if !seen.insert(key) {
                continue;
            }
            let source_document_id =
                format!("tops:{kid}:{}:{}", label, date_raw.as_deref().unwrap_or(""));
            out.push(DocumentEvidence {
                source_document_id,
                document_type_raw: label.clone(),
                document_type: classify_document_type(label),
                kgs_kid: kid.to_owned(),
                api_number_raw: api_number.map(str::to_owned),
                source_url: format!(
                    "https://chasm.kgs.ku.edu/ords/qualified.well_page.DisplayWell?f_kid={kid}"
                ),
                filing_date_raw: date_raw,
                access_state: AccessState::Public,
            });
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A real page, fetched directly during KS-15 discovery (KID
    /// 1001190703, API 15-009-03905, 2026-09-05) — trimmed to the
    /// sections this parser reads.
    const REAL_WELL_PAGE: &str = r#"
        <html><body>
        <table align="center" border="1"><tr valign="top"><td>
        <strong>API: </strong>15-009-03905
        </td><td>
        <strong>County: </strong>Barton
        </td></tr>
        <tr><td colspan="2">
        <a href="https://experience.arcgis.com/experience/f26e78ef1c5343d88c3f4a3189db65f3" target="_BLANK">View well on interactive map</a>
        <br>
        <a href="https://www.kgslibrary.com/index.cfm?whk=1001190703" target="_BLANK">View well in the Walter's Digital Library (WDL login required)</a>
        </td></tr>
        </table>
        <h3 id="tops">Tops Data</h3>
        <table border="1" align="center">
        <tr><th>Formation<br>(source name)</th><th>Top</th><th>Base</th><th>Source</th><th>Updated</th></tr>
        <tr><td>Stone Corral Formation<br>(STNC)</td><td>730</td><td>&nbsp;</td><td>Completion Report</td><td>Apr-15-1999</td></tr>
        <tr><td>Topeka Limestone Formation<br>(TPKA)</td><td>2648</td><td>&nbsp;</td><td>Completion Report</td><td>Apr-15-1999</td></tr>
        <tr><td>Heebner Shale Member<br>(HEEBNER)</td><td>2934</td><td>&nbsp;</td><td>Completion Report</td><td>Apr-15-1999</td></tr>
        </table>
        </body></html>
    "#;

    #[test]
    fn extracts_the_real_wdl_login_required_reference() {
        let evidence =
            extract_document_evidence(REAL_WELL_PAGE, "1001190703", Some("15-009-03905"));
        let wdl = evidence
            .iter()
            .find(|e| e.source_document_id == "wdl:1001190703")
            .expect("WDL reference must be extracted");
        assert_eq!(wdl.access_state, AccessState::LoginRequired);
        assert_eq!(
            wdl.source_url,
            "https://www.kgslibrary.com/index.cfm?whk=1001190703"
        );
        assert_eq!(wdl.api_number_raw.as_deref(), Some("15-009-03905"));
    }

    #[test]
    fn dedupes_repeated_tops_data_citations_to_one_evidence_record() {
        let evidence = extract_document_evidence(REAL_WELL_PAGE, "1001190703", None);
        let completion_reports: Vec<_> = evidence
            .iter()
            .filter(|e| e.document_type_raw == "Completion Report")
            .collect();
        assert_eq!(
            completion_reports.len(),
            1,
            "three identical formation-row citations must collapse to one document evidence record"
        );
        assert_eq!(completion_reports[0].document_type, DocumentType::Aco1);
        assert_eq!(
            completion_reports[0].filing_date_raw.as_deref(),
            Some("Apr-15-1999")
        );
        assert_eq!(completion_reports[0].access_state, AccessState::Public);
    }

    #[test]
    fn a_page_with_no_wdl_link_and_no_tops_table_yields_no_evidence() {
        let html = "<html><body><p>No data</p></body></html>";
        assert!(extract_document_evidence(html, "1", None).is_empty());
    }
}
