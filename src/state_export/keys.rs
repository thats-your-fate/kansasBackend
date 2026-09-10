//! KS-12/KS-13: `state-export-v1` source keys.
//!
//! Every Kansas canonical table already carries a stable internal UUID
//! primary key (KS-04 through KS-10's own "never use the KID/permit/KCC
//! license as the primary key" discipline — see e.g. `ks_well.internal_well_id`,
//! `ks_operator.id`, `ks_field.id`, `ks_lease.id`, `ks_event.id`,
//! `ks_lease_production_month.id`). Unlike a source that has to synthesize
//! a stable key from natural fields, Kansas's export keys are simply
//! `ks:<entity>:<internal uuid>` — the internal id IS already the stable
//! identity these tickets ask for. `county` is the one entity with no
//! internal table/uuid of its own (it is a free-text column on several
//! Kansas tables), so it is keyed by a normalized-name slug instead.

pub fn county(name: &str) -> String {
    format!("ks:county:{}", slug(name))
}

pub fn operator(operator_id: uuid::Uuid) -> String {
    format!("ks:operator:{operator_id}")
}

pub fn field(field_id: uuid::Uuid) -> String {
    format!("ks:field:{field_id}")
}

pub fn lease(lease_id: uuid::Uuid) -> String {
    format!("ks:lease:{lease_id}")
}

pub fn well(well_id: uuid::Uuid) -> String {
    format!("ks:well:{well_id}")
}

pub fn well_identifier(
    well_source_key: &str,
    identifier_type: &str,
    normalized_value: &str,
) -> String {
    format!(
        "ks:well_identifier:{well_source_key}:{}:{normalized_value}",
        slug(identifier_type)
    )
}

pub fn wellbore(well_source_key: &str) -> String {
    format!("ks:wellbore:{well_source_key}:main")
}

pub fn regulatory_record_intent(intent_id: uuid::Uuid) -> String {
    format!("ks:regulatory_record:drilling_intent:{intent_id}")
}

pub fn regulatory_record_transfer(
    transfer_id: uuid::Uuid,
    well_source_key: Option<&str>,
) -> String {
    match well_source_key {
        Some(key) => format!("ks:regulatory_record:operator_transfer:{transfer_id}:{key}"),
        None => format!("ks:regulatory_record:operator_transfer:{transfer_id}:unmatched"),
    }
}

pub fn event(event_id: uuid::Uuid) -> String {
    format!("ks:event:{event_id}")
}

pub fn production_monthly(production_row_id: uuid::Uuid) -> String {
    format!("ks:production_monthly:{production_row_id}")
}

pub fn tombstone(entity_type: &str, entity_source_key: &str) -> String {
    format!("ks:tombstone:{entity_type}:{entity_source_key}")
}

pub fn slug(value: &str) -> String {
    value
        .trim()
        .to_ascii_lowercase()
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() {
                character
            } else {
                '-'
            }
        })
        .collect::<String>()
        .split('-')
        .filter(|part| !part.is_empty())
        .collect::<Vec<_>>()
        .join("-")
}

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;

    #[test]
    fn county_keys_are_deterministic() {
        assert_eq!(county("Barton"), county("Barton"));
        assert_eq!(county("Barton County"), county("barton   county"));
    }

    #[test]
    fn entity_keys_carry_the_internal_uuid_not_a_reconstructed_natural_key() {
        let id = Uuid::new_v4();
        assert_eq!(well(id), format!("ks:well:{id}"));
        assert_eq!(operator(id), format!("ks:operator:{id}"));
    }

    #[test]
    fn relationship_keys_embed_the_parent_entity_key() {
        let well_id = Uuid::new_v4();
        let well_key = well(well_id);
        let identifier_key = well_identifier(&well_key, "kgs_kid", "1234567890");
        assert!(identifier_key.contains(&well_key));
        let wellbore_key = wellbore(&well_key);
        assert!(wellbore_key.contains(&well_key));
    }

    #[test]
    fn unmatched_transfer_links_are_distinguishable_from_matched_ones() {
        let transfer_id = Uuid::new_v4();
        let well_key = well(Uuid::new_v4());
        assert_ne!(
            regulatory_record_transfer(transfer_id, Some(&well_key)),
            regulatory_record_transfer(transfer_id, None)
        );
    }
}
