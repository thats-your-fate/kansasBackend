use std::collections::BTreeMap;

use serde::Serialize;

/// Fixed response envelope for every successful API response, matching the
/// convention every sibling state backend has since adopted: a stable
/// `{data, meta}` shape so clients never have to special-case per-resource
/// pagination/provenance fields.
#[derive(Debug, Serialize)]
pub struct Envelope<T: Serialize> {
    pub data: T,
    pub meta: Meta,
}

#[derive(Debug, Serialize, Default)]
pub struct Meta {
    pub state: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data_as_of: Option<chrono::NaiveDate>,
    /// KGS's production publication trails the calendar (per the plan,
    /// coverage currently runs through April 2026, published August 2026).
    /// Every endpoint that surfaces production must set this rather than
    /// implying production is current through the present month.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub production_through: Option<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub source_ids: Vec<String>,
    #[serde(skip_serializing_if = "BTreeMap::is_empty")]
    pub source_compiled_dates: BTreeMap<String, chrono::NaiveDate>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub quality_warnings: Vec<String>,
}

impl<T: Serialize> Envelope<T> {
    pub fn new(data: T, meta: Meta) -> Self {
        Envelope { data, meta }
    }
}

impl Meta {
    pub fn bare() -> Self {
        Meta {
            state: crate::constants::STATE_KEY,
            ..Default::default()
        }
    }
}
