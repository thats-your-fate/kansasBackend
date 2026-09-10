//! KS-05: discovers the KCC operator page's current download links from
//! its anchors, rather than embedding an undocumented filename. `kcc.ks.gov`
//! is confirmed unreachable from this development environment (see task
//! doc §11's KS-05 entry) — the matching heuristic below is a best-effort
//! design based on KCC's documented behavior ("active-only and
//! all-operator downloads"), unverified against the real page's actual
//! markup. Anyone with access should confirm it against a real fetch
//! before this importer is trusted in production; `tests/kcc_operators_import.rs`
//! exercises it only against a hand-built fixture page.

use scraper::{Html, Selector};

#[derive(Debug, thiserror::Error)]
pub enum DiscoveryError {
    #[error("could not parse base url '{0}': {1}")]
    InvalidBaseUrl(String, url::ParseError),
    #[error("no operator download links found on the page — the page structure may have changed")]
    NoLinksFound,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OperatorDownloadLinks {
    pub active_operators_url: Option<String>,
    pub all_operators_url: Option<String>,
}

/// Scans every `<a>` on `html` and classifies each as an "active
/// operators" link, an "all operators" link, or neither, based on its
/// visible text and href both containing "operator" plus a lifecycle
/// keyword ("active" or "all"). Relative hrefs are resolved against
/// `base_url`. Returns `NoLinksFound` if neither link is found — the
/// caller must treat that as a page-structure-change alert, not a
/// zero-operators result (KS-05 acceptance criteria).
pub fn discover_operator_links(
    html: &str,
    base_url: &str,
) -> Result<OperatorDownloadLinks, DiscoveryError> {
    let base = url::Url::parse(base_url)
        .map_err(|e| DiscoveryError::InvalidBaseUrl(base_url.to_string(), e))?;
    let document = Html::parse_document(html);
    let selector = Selector::parse("a").expect("'a' is a valid CSS selector");

    let mut active_url = None;
    let mut all_url = None;

    for element in document.select(&selector) {
        let Some(href) = element.value().attr("href") else {
            continue;
        };
        let text = element.text().collect::<Vec<_>>().join(" ").to_lowercase();
        let haystack = format!("{text} {}", href.to_lowercase());

        if !haystack.contains("operator") {
            continue;
        }

        let resolved = match base.join(href) {
            Ok(u) => u.to_string(),
            Err(_) => continue,
        };

        if active_url.is_none() && haystack.contains("active") {
            active_url = Some(resolved);
        } else if all_url.is_none() && haystack.contains("all") {
            all_url = Some(resolved);
        }
    }

    if active_url.is_none() && all_url.is_none() {
        return Err(DiscoveryError::NoLinksFound);
    }

    Ok(OperatorDownloadLinks {
        active_operators_url: active_url,
        all_operators_url: all_url,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    const BASE: &str = "https://www.kcc.ks.gov/kansas-oil-operators";

    #[test]
    fn finds_both_links_from_realistic_fixture() {
        let html = r#"
            <html><body>
                <h1>Kansas Oil Operators</h1>
                <p>Download the active operators list:
                    <a href="/docs/default-source/oil-gas/kansas-active-oil-operators.csv">Active Operators (CSV)</a>
                </p>
                <p>Download the complete operator roster:
                    <a href="/docs/default-source/oil-gas/kansas-all-oil-operators.csv">All Operators (CSV)</a>
                </p>
            </body></html>
        "#;
        let links = discover_operator_links(html, BASE).unwrap();
        assert_eq!(
            links.active_operators_url.as_deref(),
            Some("https://www.kcc.ks.gov/docs/default-source/oil-gas/kansas-active-oil-operators.csv")
        );
        assert_eq!(
            links.all_operators_url.as_deref(),
            Some("https://www.kcc.ks.gov/docs/default-source/oil-gas/kansas-all-oil-operators.csv")
        );
    }

    #[test]
    fn ignores_unrelated_links() {
        let html = r#"
            <html><body>
                <a href="/oil-gas-forms">Oil & Gas Forms</a>
                <a href="/notices-of-intents-search">Notices of Intent</a>
                <a href="/all-operators.csv">All Operators</a>
            </body></html>
        "#;
        let links = discover_operator_links(html, BASE).unwrap();
        assert_eq!(links.active_operators_url, None);
        assert_eq!(
            links.all_operators_url.as_deref(),
            Some("https://www.kcc.ks.gov/all-operators.csv")
        );
    }

    #[test]
    fn no_matching_links_is_an_error_not_an_empty_result() {
        let html = r#"<html><body><a href="/home">Home</a></body></html>"#;
        let err = discover_operator_links(html, BASE).unwrap_err();
        assert!(matches!(err, DiscoveryError::NoLinksFound));
    }
}
