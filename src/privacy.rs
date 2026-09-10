//! Hard data-use control (task doc §3.7 item 2): KCC's intent and operator
//! pages carry a statutory commercial-solicitation certification
//! concerning names and addresses. Product search results and exports
//! must never surface a personal/contact field from that data — this
//! module is the enforcement mechanism, not a comment someone has to
//! remember to add. Every public-facing operator/intent/transfer response
//! type's own test suite must call `assert_no_forbidden_fields` against a
//! populated instance, starting with KS-006 (operators), KS-007 (drilling
//! intents), and KS-008 (transfers).

use serde::Serialize;
use serde_json::Value;

/// Substrings that, if found (case-insensitively) in any JSON key at any
/// nesting depth of a serialized public response, indicate a personal or
/// contact field that must not be exposed. Deliberately broad (substring,
/// not exact match) so `contact_phone`, `phone_number`, and `mailing_address`
/// are all caught by `phone`/`address` without listing every variant.
pub const FORBIDDEN_PUBLIC_FIELDS: &[&str] = &[
    "address",
    "phone",
    "fax",
    "email",
    "contact_name",
    "contact_person",
];

/// Serializes `value` and fails with the offending key path(s) if any
/// forbidden contact/address field name appears anywhere in the result.
pub fn assert_no_forbidden_fields<T: Serialize>(value: &T) -> Result<(), Vec<String>> {
    let json = serde_json::to_value(value).expect("value must serialize to JSON");
    let mut hits = Vec::new();
    walk(&json, "$", &mut hits);
    if hits.is_empty() {
        Ok(())
    } else {
        Err(hits)
    }
}

fn walk(value: &Value, path: &str, hits: &mut Vec<String>) {
    match value {
        Value::Object(map) => {
            for (key, v) in map {
                let normalized = key.to_lowercase();
                let child_path = format!("{path}.{key}");
                if FORBIDDEN_PUBLIC_FIELDS
                    .iter()
                    .any(|f| normalized.contains(f))
                {
                    hits.push(child_path.clone());
                }
                walk(v, &child_path, hits);
            }
        }
        Value::Array(items) => {
            for (i, item) in items.iter().enumerate() {
                walk(item, &format!("{path}[{i}]"), hits);
            }
        }
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::Serialize;

    #[derive(Serialize)]
    struct CleanOperator {
        license_number: String,
        operator_name: String,
        status: String,
    }

    #[derive(Serialize)]
    struct LeakyOperator {
        license_number: String,
        operator_name: String,
        mailing_address: String,
        contact_phone: String,
    }

    #[test]
    fn clean_struct_passes() {
        let operator = CleanOperator {
            license_number: "12345".to_string(),
            operator_name: "Example Operating LLC".to_string(),
            status: "active".to_string(),
        };
        assert!(assert_no_forbidden_fields(&operator).is_ok());
    }

    #[test]
    fn leaky_struct_is_caught() {
        let operator = LeakyOperator {
            license_number: "12345".to_string(),
            operator_name: "Example Operating LLC".to_string(),
            mailing_address: "123 Main St".to_string(),
            contact_phone: "555-0100".to_string(),
        };
        let hits = assert_no_forbidden_fields(&operator).unwrap_err();
        assert_eq!(hits.len(), 2);
        assert!(hits.iter().any(|h| h.contains("mailing_address")));
        assert!(hits.iter().any(|h| h.contains("contact_phone")));
    }

    #[test]
    fn catches_forbidden_fields_nested_in_arrays() {
        #[derive(Serialize)]
        struct Wrapper {
            operators: Vec<LeakyOperator>,
        }
        let wrapper = Wrapper {
            operators: vec![LeakyOperator {
                license_number: "1".to_string(),
                operator_name: "A".to_string(),
                mailing_address: "hidden".to_string(),
                contact_phone: "hidden".to_string(),
            }],
        };
        let hits = assert_no_forbidden_fields(&wrapper).unwrap_err();
        assert!(hits.iter().any(|h| h == "$.operators[0].mailing_address"));
    }
}
