//! KS-06/KS-07: shared well-linking primitives. Both tickets describe a
//! layered exact-then-fuzzy-then-unresolved matching strategy against
//! `ks_well`; this module holds the actual queries so neither importer
//! reimplements them.
//!
//! Every "exact" lookup here returns `None`, not a guess, when more than
//! one well matches — an ambiguous exact match is not a match. This
//! matters most for `find_well_by_exact_plss`: a full PLSS section
//! commonly contains many wells in real Kansas data, so that tier is
//! expected to resolve only rarely on its own; it is implemented exactly
//! as the task doc specifies regardless.

use serde_json::{json, Value};
use sqlx::PgPool;
use uuid::Uuid;

#[derive(Debug, Clone, Copy, Default)]
pub struct Plss<'a> {
    pub county: Option<&'a str>,
    pub township: Option<i16>,
    pub township_direction: Option<&'a str>,
    pub range: Option<i16>,
    pub range_direction: Option<&'a str>,
    pub section: Option<i16>,
}

impl Plss<'_> {
    fn is_complete(&self) -> bool {
        self.county.is_some()
            && self.township.is_some()
            && self.range.is_some()
            && self.section.is_some()
    }
}

/// Tier: exact KCC permit, API number, or DOR lease-link identifier.
/// Checks `ks_well`'s own denormalized `api14`/`kcc_permit`/`oil_dor_id`/
/// `gas_dor_id` columns plus `ks_well_source_alias` (KS-04) for the
/// `kcc_permit`/`api_raw` alias types.
pub async fn find_well_by_exact_identifier(
    pool: &PgPool,
    token: &str,
) -> Result<Option<Uuid>, sqlx::Error> {
    if token.trim().is_empty() {
        return Ok(None);
    }
    sqlx::query_scalar::<_, Uuid>(
        r#"
        SELECT internal_well_id FROM ks_well
        WHERE api14 = $1 OR kcc_permit = $1 OR oil_dor_id = $1 OR gas_dor_id = $1
        UNION
        SELECT well_id FROM ks_well_source_alias
        WHERE alias_type IN ('kcc_permit', 'api_raw') AND alias_value = $1
        LIMIT 2
        "#,
    )
    .bind(token)
    .fetch_all(pool)
    .await
    .map(|rows: Vec<Uuid>| if rows.len() == 1 { Some(rows[0]) } else { None })
}

/// Tier: exact operator license + exact PLSS + exact well/lease name, all
/// three required together (KS-06 tier 2). The operator's reported name
/// (looked up via `ks_operator` by license — KS-05) must exactly match
/// one of `ks_well`'s own reported-operator fields; there is no direct
/// well-to-operator-license foreign key yet (that join is KS-08).
pub async fn find_well_by_license_plss_name(
    pool: &PgPool,
    operator_license: Option<&str>,
    plss: Plss<'_>,
    lease_or_well_name: Option<&str>,
) -> Result<Option<Uuid>, sqlx::Error> {
    let (Some(license), Some(name)) = (operator_license, lease_or_well_name) else {
        return Ok(None);
    };
    if !plss.is_complete() || name.trim().is_empty() {
        return Ok(None);
    }

    let operator_name: Option<String> = sqlx::query_scalar(
        "SELECT normalized_company_name FROM ks_operator WHERE kcc_license_number = $1",
    )
    .bind(license)
    .fetch_optional(pool)
    .await?;
    let Some(operator_name) = operator_name else {
        return Ok(None);
    };

    let rows: Vec<Uuid> = sqlx::query_scalar(
        r#"
        SELECT internal_well_id FROM ks_well
        WHERE county = $1 AND township = $2 AND range = $3 AND section = $4
          AND (range_direction = $5 OR ($5 IS NULL AND range_direction IS NULL))
          AND (township_direction = $6 OR $6 IS NULL)
          AND (upper(trim(lease_name)) = upper(trim($7)) OR upper(trim(well_name)) = upper(trim($7)))
          AND (upper(trim(operator_name_kgs_reported_current)) = upper(trim($8))
               OR upper(trim(operator_name_kgs_original)) = upper(trim($8)))
        LIMIT 2
        "#,
    )
    .bind(plss.county)
    .bind(plss.township)
    .bind(plss.range)
    .bind(plss.section)
    .bind(plss.range_direction)
    .bind(plss.township_direction)
    .bind(name)
    .bind(&operator_name)
    .fetch_all(pool)
    .await?;

    Ok(if rows.len() == 1 { Some(rows[0]) } else { None })
}

/// Tier: exact PLSS alone (KS-07). Ambiguous in most real data — a
/// section-level PLSS commonly contains many wells — so this resolves
/// only when it happens to land on exactly one.
pub async fn find_well_by_exact_plss(
    pool: &PgPool,
    plss: Plss<'_>,
) -> Result<Option<Uuid>, sqlx::Error> {
    if !plss.is_complete() {
        return Ok(None);
    }
    let rows: Vec<Uuid> = sqlx::query_scalar(
        r#"
        SELECT internal_well_id FROM ks_well
        WHERE county = $1 AND township = $2 AND range = $3 AND section = $4
          AND (range_direction = $5 OR ($5 IS NULL AND range_direction IS NULL))
          AND (township_direction = $6 OR $6 IS NULL)
        LIMIT 2
        "#,
    )
    .bind(plss.county)
    .bind(plss.township)
    .bind(plss.range)
    .bind(plss.section)
    .bind(plss.range_direction)
    .bind(plss.township_direction)
    .fetch_all(pool)
    .await?;
    Ok(if rows.len() == 1 { Some(rows[0]) } else { None })
}

/// Tier: exact lease/well name alone (KS-07), optionally scoped to a
/// county to reduce cross-county name collisions.
pub async fn find_well_by_exact_lease_name(
    pool: &PgPool,
    lease_or_well_label: &str,
    county: Option<&str>,
) -> Result<Option<Uuid>, sqlx::Error> {
    if lease_or_well_label.trim().is_empty() {
        return Ok(None);
    }
    let rows: Vec<Uuid> = sqlx::query_scalar(
        r#"
        SELECT internal_well_id FROM ks_well
        WHERE (upper(trim(lease_name)) = upper(trim($1)) OR upper(trim(well_name)) = upper(trim($1)))
          AND ($2::text IS NULL OR county = $2)
        LIMIT 2
        "#,
    )
    .bind(lease_or_well_label)
    .bind(county)
    .fetch_all(pool)
    .await?;
    Ok(if rows.len() == 1 { Some(rows[0]) } else { None })
}

/// Tier: exact operator license alone (KS-07) — cross-referenced by name
/// via `ks_operator` since no direct well-to-license FK exists yet.
/// Expected to resolve only when an operator happens to have exactly one
/// well in this dataset; documented as a completeness tier, not a
/// primarily load-bearing one.
pub async fn find_well_by_operator_license(
    pool: &PgPool,
    operator_license: &str,
) -> Result<Option<Uuid>, sqlx::Error> {
    let operator_name: Option<String> = sqlx::query_scalar(
        "SELECT normalized_company_name FROM ks_operator WHERE kcc_license_number = $1",
    )
    .bind(operator_license)
    .fetch_optional(pool)
    .await?;
    let Some(operator_name) = operator_name else {
        return Ok(None);
    };
    let rows: Vec<Uuid> = sqlx::query_scalar(
        r#"
        SELECT internal_well_id FROM ks_well
        WHERE upper(trim(operator_name_kgs_reported_current)) = upper(trim($1))
           OR upper(trim(operator_name_kgs_original)) = upper(trim($1))
        LIMIT 2
        "#,
    )
    .bind(&operator_name)
    .fetch_all(pool)
    .await?;
    Ok(if rows.len() == 1 { Some(rows[0]) } else { None })
}

#[derive(Debug, Clone)]
pub struct FuzzyCandidate {
    pub well_id: Uuid,
    pub similarity: f32,
}

/// Fuzzy lease/well-name candidates via `pg_trgm`'s `similarity()`,
/// optionally scoped to a county. Never used to auto-link — every caller
/// treats these as candidates requiring a recorded confidence value, per
/// both tickets' explicit "must not directly overwrite" / "requires a
/// confidence value" requirements.
pub async fn fuzzy_name_candidates(
    pool: &PgPool,
    name: &str,
    county: Option<&str>,
    limit: i64,
) -> Result<Vec<FuzzyCandidate>, sqlx::Error> {
    if name.trim().is_empty() {
        return Ok(Vec::new());
    }
    let rows: Vec<(Uuid, f32)> = sqlx::query_as(
        r#"
        SELECT internal_well_id, GREATEST(similarity(lease_name, $1), similarity(well_name, $1)) AS sim
        FROM ks_well
        WHERE ($2::text IS NULL OR county = $2)
          AND GREATEST(similarity(lease_name, $1), similarity(well_name, $1)) > 0.3
        ORDER BY sim DESC
        LIMIT $3
        "#,
    )
    .bind(name)
    .bind(county)
    .bind(limit)
    .fetch_all(pool)
    .await?;
    Ok(rows
        .into_iter()
        .map(|(well_id, similarity)| FuzzyCandidate {
            well_id,
            similarity,
        })
        .collect())
}

pub fn matched_fields_json(fields: &[(&str, Option<&str>)]) -> Value {
    json!(fields
        .iter()
        .filter_map(|(k, v)| v.map(|v| (k.to_string(), v)))
        .collect::<std::collections::BTreeMap<_, _>>())
}
