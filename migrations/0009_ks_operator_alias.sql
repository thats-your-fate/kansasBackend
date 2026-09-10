-- KS-05: operator name-variant/alias table.
--
-- Rollback (plain single-file migrations, no paired .down.sql):
--   DROP TABLE IF EXISTS ks_operator_alias;
--
-- Records every distinct spelling/punctuation variant of a company name
-- ever observed for a given kcc_license_number. Deliberately keyed off
-- the license number (never off name similarity) so KS-05's "prevent
-- aggressive mergers of similarly named companies" holds structurally:
-- two operators with genuinely different license numbers can never share
-- a row here just because their names look alike.
CREATE TABLE IF NOT EXISTS ks_operator_alias (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    operator_id uuid NOT NULL REFERENCES ks_operator(id) ON DELETE CASCADE,
    reported_company_name text NOT NULL,
    normalized_company_name text NOT NULL,
    first_seen_ingestion_run_id uuid NOT NULL REFERENCES ks_ingestion_run(id),
    created_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT ks_operator_alias_unique UNIQUE (operator_id, reported_company_name)
);

CREATE INDEX IF NOT EXISTS ks_operator_alias_operator_idx ON ks_operator_alias (operator_id);
