//! KS-02: the Kansas source registry. Queries `ks_source` — the DB-seeded
//! table of the 12 trackable KGS/KCC feeds and their data-use policy
//! (migration `0002_ks_source_registry.sql`). This is the authoritative,
//! testable, DB-queryable policy configuration KS-02 asks for; ingestion
//! code must call `find_source_by_code`/`list_sources` rather than
//! hardcoding a policy assumption.

use sqlx::PgPool;
use uuid::Uuid;

#[derive(Debug, Clone, PartialEq, sqlx::FromRow)]
pub struct SourcePolicy {
    pub id: Uuid,
    pub code: String,
    pub agency: String,
    pub description: String,
    pub discovery_url: Option<String>,
    pub allowed_for_public_product: bool,
    pub minimum_allowed_year: Option<i32>,
    pub contains_restricted_contact_fields: bool,
    pub document_download_policy: String,
    pub expected_cadence: String,
}

impl SourcePolicy {
    /// True when any public-facing response derived from this source must
    /// be checked with `crate::privacy::assert_no_forbidden_fields` before
    /// being returned — the KS-02 requirement that "KCC operator and
    /// intent source contact fields [are] excluded from product indexes
    /// and exports" made testable rather than left to memory.
    pub fn requires_privacy_guard(&self) -> bool {
        self.contains_restricted_contact_fields
    }

    /// True when `year` may be ingested under this source's policy.
    /// Sources with no `minimum_allowed_year` (most of them) allow every
    /// year; production sources are seeded at 1990
    /// (`constants::MIN_PRODUCTION_YEAR`) per the pre-1987 IHS-licensing
    /// restriction.
    pub fn allows_year(&self, year: i32) -> bool {
        self.minimum_allowed_year.is_none_or(|min| year >= min)
    }
}

const SELECT_COLUMNS: &str = r#"
    id, code, agency, description, discovery_url, allowed_for_public_product,
    minimum_allowed_year, contains_restricted_contact_fields,
    document_download_policy, expected_cadence
"#;

pub async fn find_source_by_code(
    pool: &PgPool,
    code: &str,
) -> Result<Option<SourcePolicy>, sqlx::Error> {
    sqlx::query_as::<_, SourcePolicy>(&format!(
        "SELECT {SELECT_COLUMNS} FROM ks_source WHERE code = $1"
    ))
    .bind(code)
    .fetch_optional(pool)
    .await
}

pub async fn list_sources(pool: &PgPool) -> Result<Vec<SourcePolicy>, sqlx::Error> {
    sqlx::query_as::<_, SourcePolicy>(&format!(
        "SELECT {SELECT_COLUMNS} FROM ks_source ORDER BY code"
    ))
    .fetch_all(pool)
    .await
}
