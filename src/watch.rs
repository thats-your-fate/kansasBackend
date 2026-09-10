//! KS-14: watch areas and event matching. This repo has no auth/account
//! system yet (`ks_watch_area.owner_reference` is a placeholder — see
//! migration `0026`'s doc comment), so this module's job is narrower than
//! a full alerting/notification pipeline: given an event (already
//! carrying its typed relationship columns from `ks_event`/
//! `ks_event_well_link`) and the current set of watch areas, decide which
//! watch areas match. Delivery (email/SMS/webhook) does not exist and is
//! out of this task's scope — the task doc's acceptance criteria
//! ("radius/county/operator watch tests") are about the matching logic
//! itself, not a notification transport.

use sqlx::PgPool;
use uuid::Uuid;

#[derive(Debug, Clone, Copy, PartialEq, Eq, sqlx::Type)]
#[sqlx(type_name = "text", rename_all = "lowercase")]
pub enum WatchType {
    County,
    Operator,
    Field,
    Well,
    Radius,
}

#[derive(Debug, Clone, sqlx::FromRow)]
pub struct WatchArea {
    pub id: Uuid,
    pub owner_reference: Option<String>,
    pub watch_type: WatchType,
    pub county: Option<String>,
    pub operator_id: Option<Uuid>,
    pub field_id: Option<Uuid>,
    pub well_id: Option<Uuid>,
    pub center_latitude: Option<f64>,
    pub center_longitude: Option<f64>,
    pub radius_miles: Option<f64>,
}

/// The event-side context a watch area is matched against. Built from
/// `ks_event`'s own typed relationship columns plus `ks_event_well_link`
/// and (for a radius watch) the related well's location — never by
/// parsing `payload` JSON, which is lineage/debugging data, not a stable
/// contract.
#[derive(Debug, Clone, Default)]
pub struct EventWatchContext {
    pub county: Option<String>,
    pub operator_id: Option<Uuid>,
    pub field_id: Option<Uuid>,
    pub lease_id: Option<Uuid>,
    pub well_ids: Vec<Uuid>,
    /// (latitude, longitude) of each related well with known coordinates
    /// — a multi-well event (e.g. an operator transfer) can have more
    /// than one point to check against a radius watch.
    pub well_locations: Vec<(f64, f64)>,
}

const EARTH_RADIUS_MILES: f64 = 3958.8;

/// Great-circle (haversine) distance in miles between two WGS84 points.
pub fn distance_miles(lat1: f64, lon1: f64, lat2: f64, lon2: f64) -> f64 {
    let (lat1, lon1, lat2, lon2) = (
        lat1.to_radians(),
        lon1.to_radians(),
        lat2.to_radians(),
        lon2.to_radians(),
    );
    let dlat = lat2 - lat1;
    let dlon = lon2 - lon1;
    let a = (dlat / 2.0).sin().powi(2) + lat1.cos() * lat2.cos() * (dlon / 2.0).sin().powi(2);
    let c = 2.0 * a.sqrt().asin();
    EARTH_RADIUS_MILES * c
}

/// Case/whitespace-insensitive county-name comparison — the same
/// normalization discipline used everywhere else county names are
/// compared in this repo (deterministic, never fuzzy).
pub fn county_matches(watch_county: &str, event_county: &str) -> bool {
    watch_county
        .trim()
        .eq_ignore_ascii_case(event_county.trim())
}

/// Whether `context` matches `area`, given `area`'s type. A malformed
/// watch area (e.g. a `radius` row missing its center) never matches —
/// this function does not assume the database CHECK constraints have
/// already been satisfied by its caller.
pub fn event_matches_watch_area(area: &WatchArea, context: &EventWatchContext) -> bool {
    match area.watch_type {
        WatchType::County => match (&area.county, &context.county) {
            (Some(watch_county), Some(event_county)) => county_matches(watch_county, event_county),
            _ => false,
        },
        WatchType::Operator => {
            area.operator_id.is_some() && area.operator_id == context.operator_id
        }
        WatchType::Field => area.field_id.is_some() && area.field_id == context.field_id,
        WatchType::Well => match area.well_id {
            Some(well_id) => context.well_ids.contains(&well_id),
            None => false,
        },
        WatchType::Radius => {
            let (Some(center_lat), Some(center_lon), Some(radius)) = (
                area.center_latitude,
                area.center_longitude,
                area.radius_miles,
            ) else {
                return false;
            };
            context
                .well_locations
                .iter()
                .any(|(lat, lon)| distance_miles(center_lat, center_lon, *lat, *lon) <= radius)
        }
    }
}

/// Builds an `EventWatchContext` for `event_id` by reading its typed
/// relationship columns and any `ks_event_well_link` rows, then resolving
/// well locations for a radius check.
type EventRelationshipColumns = (
    Option<String>,
    Option<Uuid>,
    Option<Uuid>,
    Option<Uuid>,
    Option<Uuid>,
);

pub async fn event_watch_context(
    pool: &PgPool,
    event_id: Uuid,
) -> Result<EventWatchContext, sqlx::Error> {
    let (related_county, related_operator_id, related_field_id, related_lease_id, related_well_id): EventRelationshipColumns =
        sqlx::query_as("SELECT related_county, related_operator_id, related_field_id, related_lease_id, related_well_id FROM ks_event WHERE id = $1")
            .bind(event_id)
            .fetch_one(pool)
            .await?;

    let linked_well_ids: Vec<Uuid> =
        sqlx::query_scalar("SELECT well_id FROM ks_event_well_link WHERE event_id = $1")
            .bind(event_id)
            .fetch_all(pool)
            .await?;

    let mut well_ids: Vec<Uuid> = related_well_id.into_iter().collect();
    well_ids.extend(linked_well_ids);
    well_ids.sort();
    well_ids.dedup();

    let mut well_locations = Vec::new();
    if !well_ids.is_empty() {
        let rows: Vec<(f64, f64)> = sqlx::query_as(
            "SELECT ST_Y(display_geometry_4326), ST_X(display_geometry_4326) FROM ks_well WHERE internal_well_id = ANY($1) AND display_geometry_4326 IS NOT NULL",
        )
        .bind(&well_ids)
        .fetch_all(pool)
        .await?;
        well_locations = rows;
    }

    Ok(EventWatchContext {
        county: related_county,
        operator_id: related_operator_id,
        field_id: related_field_id,
        lease_id: related_lease_id,
        well_ids,
        well_locations,
    })
}

/// All active watch areas currently matching `event_id`.
pub async fn find_matching_watch_areas(
    pool: &PgPool,
    event_id: Uuid,
) -> Result<Vec<WatchArea>, sqlx::Error> {
    let context = event_watch_context(pool, event_id).await?;
    let areas: Vec<WatchArea> = sqlx::query_as(
        r#"
        SELECT id, owner_reference, watch_type, county, operator_id, field_id, well_id,
               center_latitude, center_longitude, radius_miles
        FROM ks_watch_area
        WHERE is_active
        "#,
    )
    .fetch_all(pool)
    .await?;
    Ok(areas
        .into_iter()
        .filter(|area| event_matches_watch_area(area, &context))
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn well_area(watch_type: WatchType) -> WatchArea {
        WatchArea {
            id: Uuid::new_v4(),
            owner_reference: None,
            watch_type,
            county: None,
            operator_id: None,
            field_id: None,
            well_id: None,
            center_latitude: None,
            center_longitude: None,
            radius_miles: None,
        }
    }

    #[test]
    fn county_watch_matches_case_and_whitespace_insensitively() {
        assert!(county_matches("Barton", "  barton  "));
        assert!(!county_matches("Barton", "Reno"));
    }

    #[test]
    fn county_watch_area_matches_only_its_own_county() {
        let area = WatchArea {
            county: Some("Barton".to_string()),
            ..well_area(WatchType::County)
        };
        let matching = EventWatchContext {
            county: Some("barton".to_string()),
            ..Default::default()
        };
        let not_matching = EventWatchContext {
            county: Some("Reno".to_string()),
            ..Default::default()
        };
        let no_county = EventWatchContext::default();
        assert!(event_matches_watch_area(&area, &matching));
        assert!(!event_matches_watch_area(&area, &not_matching));
        assert!(!event_matches_watch_area(&area, &no_county));
    }

    #[test]
    fn operator_watch_area_matches_only_its_own_operator() {
        let operator_id = Uuid::new_v4();
        let area = WatchArea {
            operator_id: Some(operator_id),
            ..well_area(WatchType::Operator)
        };
        let matching = EventWatchContext {
            operator_id: Some(operator_id),
            ..Default::default()
        };
        let not_matching = EventWatchContext {
            operator_id: Some(Uuid::new_v4()),
            ..Default::default()
        };
        assert!(event_matches_watch_area(&area, &matching));
        assert!(!event_matches_watch_area(&area, &not_matching));
        assert!(!event_matches_watch_area(
            &area,
            &EventWatchContext::default()
        ));
    }

    #[test]
    fn field_watch_area_matches_only_its_own_field() {
        let field_id = Uuid::new_v4();
        let area = WatchArea {
            field_id: Some(field_id),
            ..well_area(WatchType::Field)
        };
        assert!(event_matches_watch_area(
            &area,
            &EventWatchContext {
                field_id: Some(field_id),
                ..Default::default()
            }
        ));
        assert!(!event_matches_watch_area(
            &area,
            &EventWatchContext {
                field_id: Some(Uuid::new_v4()),
                ..Default::default()
            }
        ));
    }

    #[test]
    fn well_watch_area_matches_any_well_in_a_multi_well_event() {
        let watched_well = Uuid::new_v4();
        let other_well = Uuid::new_v4();
        let area = WatchArea {
            well_id: Some(watched_well),
            ..well_area(WatchType::Well)
        };
        let context = EventWatchContext {
            well_ids: vec![other_well, watched_well],
            ..Default::default()
        };
        assert!(event_matches_watch_area(&area, &context));
        let no_match_context = EventWatchContext {
            well_ids: vec![other_well],
            ..Default::default()
        };
        assert!(!event_matches_watch_area(&area, &no_match_context));
    }

    #[test]
    fn radius_watch_matches_a_well_inside_the_radius_and_not_one_outside() {
        // Great Bend, KS (Barton County) as the watch center.
        let area = WatchArea {
            center_latitude: Some(38.3646),
            center_longitude: Some(-98.7648),
            radius_miles: Some(25.0),
            ..well_area(WatchType::Radius)
        };
        // Hoisington, KS — roughly 15 miles from Great Bend.
        let nearby = EventWatchContext {
            well_locations: vec![(38.5178, -98.7681)],
            ..Default::default()
        };
        assert!(event_matches_watch_area(&area, &nearby));
        // Wichita, KS — roughly 100+ miles away.
        let far_away = EventWatchContext {
            well_locations: vec![(37.6872, -97.3301)],
            ..Default::default()
        };
        assert!(!event_matches_watch_area(&area, &far_away));
    }

    #[test]
    fn radius_watch_never_matches_without_a_well_location() {
        let area = WatchArea {
            center_latitude: Some(38.0),
            center_longitude: Some(-98.0),
            radius_miles: Some(10.0),
            ..well_area(WatchType::Radius)
        };
        assert!(!event_matches_watch_area(
            &area,
            &EventWatchContext::default()
        ));
    }

    #[test]
    fn malformed_radius_watch_area_never_matches_rather_than_panicking() {
        let area = well_area(WatchType::Radius); // no center/radius set at all
        let context = EventWatchContext {
            well_locations: vec![(38.0, -98.0)],
            ..Default::default()
        };
        assert!(!event_matches_watch_area(&area, &context));
    }

    #[test]
    fn distance_is_symmetric_and_zero_for_the_same_point() {
        assert_eq!(distance_miles(38.0, -98.0, 38.0, -98.0), 0.0);
        let a_to_b = distance_miles(38.0, -98.0, 39.0, -97.0);
        let b_to_a = distance_miles(39.0, -97.0, 38.0, -98.0);
        assert!((a_to_b - b_to_a).abs() < 1e-9);
    }
}
