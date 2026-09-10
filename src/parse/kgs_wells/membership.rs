//! KS-03: Kansas membership classification for raw KGS well records. Pure
//! logic, no I/O — see `constants::{point_in_kansas_bounding_box,
//! is_valid_kansas_county, api_digits_belong_to_state}` for the underlying
//! rules this combines.

use crate::constants;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MembershipStatus {
    Included,
    ExcludedOtherState,
}

impl MembershipStatus {
    pub fn as_db_str(self) -> &'static str {
        match self {
            MembershipStatus::Included => "included",
            MembershipStatus::ExcludedOtherState => "excluded_other_state",
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct MembershipClassification {
    pub status: MembershipStatus,
    pub reason: Option<String>,
}

/// Classifies a raw record as a Kansas well or not, per task doc rule:
/// a valid Kansas API prefix, OR (WGS84 geometry inside the Kansas
/// bounding box AND a recognized Kansas county name). Every input is
/// optional because real KGS rows are frequently missing one or more of
/// these fields — a missing field simply fails that half of the OR, it
/// never panics or defaults to "included".
pub fn classify_kansas_membership(
    api_num_nodash: Option<&str>,
    county: Option<&str>,
    wgs84_lat_lon: Option<(f64, f64)>,
) -> MembershipClassification {
    if let Some(api) = api_num_nodash {
        if constants::api_digits_belong_to_state(api) {
            return MembershipClassification {
                status: MembershipStatus::Included,
                reason: None,
            };
        }
    }

    let county_is_valid_kansas = county
        .map(constants::is_valid_kansas_county)
        .unwrap_or(false);
    let geometry_in_kansas = wgs84_lat_lon
        .map(|(lat, lon)| constants::point_in_kansas_bounding_box(lat, lon))
        .unwrap_or(false);

    if county_is_valid_kansas && geometry_in_kansas {
        return MembershipClassification {
            status: MembershipStatus::Included,
            reason: None,
        };
    }

    // The (county valid AND geometry in Kansas) combination already
    // returned `Included` above, so it can never reach this match — the
    // wildcard exists only to satisfy exhaustiveness, not because it's a
    // real case.
    let reason = match (api_num_nodash, county, county_is_valid_kansas, geometry_in_kansas) {
        (Some(api), _, _, _) => format!("API number {api} does not have Kansas prefix 15, and geometry+county fallback did not both pass"),
        (None, Some(c), false, _) => format!("no API number; county '{c}' is not a recognized Kansas county"),
        (None, _, _, false) => "no API number; geometry is outside the Kansas bounding box (or missing)".to_string(),
        (None, None, _, true) => "no API number and no county to classify against".to_string(),
        (None, Some(_), true, true) => unreachable!("already returned Included above"),
    };

    MembershipClassification {
        status: MembershipStatus::ExcludedOtherState,
        reason: Some(reason),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn valid_kansas_api_prefix_is_included_even_with_bad_county() {
        let result = classify_kansas_membership(Some("15059232820000"), Some("Not A County"), None);
        assert_eq!(result.status, MembershipStatus::Included);
        assert!(result.reason.is_none());
    }

    #[test]
    fn oklahoma_api_prefix_with_kansas_geometry_and_county_is_still_included() {
        // The county+geometry fallback is independent of API prefix — an
        // out-of-state-prefixed API with genuinely Kansas geometry+county
        // is a real scenario KGS's own docs anticipate (e.g. a
        // re-permitted well whose API was never corrected).
        let result = classify_kansas_membership(
            Some("35000000000000"),
            Some("Sedgwick"),
            Some((37.6872, -97.3301)),
        );
        assert_eq!(result.status, MembershipStatus::Included);
    }

    #[test]
    fn oklahoma_api_prefix_with_oklahoma_geometry_is_excluded() {
        let result = classify_kansas_membership(
            Some("35000000000000"),
            Some("Comanche"),
            Some((35.4676, -97.5164)),
        );
        // "Comanche" is coincidentally also a real Kansas county name, so
        // this exercises the geometry half of the fallback failing it.
        assert_eq!(result.status, MembershipStatus::ExcludedOtherState);
        assert!(result.reason.is_some());
    }

    #[test]
    fn missing_api_with_valid_county_and_geometry_is_included() {
        let result = classify_kansas_membership(None, Some("Sedgwick"), Some((37.6872, -97.3301)));
        assert_eq!(result.status, MembershipStatus::Included);
    }

    #[test]
    fn missing_everything_is_excluded_with_a_reason() {
        let result = classify_kansas_membership(None, None, None);
        assert_eq!(result.status, MembershipStatus::ExcludedOtherState);
        assert!(result.reason.is_some());
    }

    #[test]
    fn valid_county_but_no_geometry_is_excluded() {
        let result = classify_kansas_membership(None, Some("Sedgwick"), None);
        assert_eq!(result.status, MembershipStatus::ExcludedOtherState);
    }
}
