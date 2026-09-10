-- KS-02: Kansas source registry and data-policy configuration.
--
-- Rollback (this project uses plain single-file migrations, per
-- `0001_extensions.sql`'s convention — no paired .down.sql):
--   DROP TABLE IF EXISTS ks_source;
-- Safe to run standalone as long as 0003_ks_ingestion_run.sql (which
-- REFERENCES ks_source) is rolled back first: DROP TABLE IF EXISTS
-- ks_source_rejection, ks_ingestion_run; then this file's DROP TABLE.
--
-- Registers the 12 trackable KGS/KCC feeds as first-class rows, each
-- carrying the policy fields that gate whether/how its data may reach the
-- public product. This is a narrower, ingestion-relevant subset of
-- `constants::SOURCES` (which also lists catalog/format/discovery-only
-- pages that aren't themselves ingestible datasets) — every code here has
-- a corresponding `constants::INGESTION_SOURCE_CODES` entry that ingestion
-- code must reference, never a raw string literal.
--
-- Design note (recorded per the KS-02 source-granularity decision): this
-- table is Kansas-internal. It is deliberately NOT modeled as 12 rows in
-- futureBackend's shared `data_sources` table — every existing state
-- registers exactly one `data_sources` row per state backend (see KS-01),
-- and Kansas keeps that precedent. This registry is where the requested
-- per-feed lineage and policy granularity actually lives.

CREATE TABLE IF NOT EXISTS ks_source (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    code text NOT NULL,
    agency text NOT NULL,
    description text NOT NULL,
    -- Nullable: some registered codes (e.g. KGS_DOCUMENTS) don't have a
    -- confirmed machine-fetchable URL yet — see the KS-002 caveat that
    -- discovery_url values need a discovery/audit pass before being
    -- treated as a stable request target.
    discovery_url text,
    allowed_for_public_product boolean NOT NULL DEFAULT true,
    -- NULL means "no year-based restriction applies" (most sources).
    -- Set for production sources per the pre-1987 IHS-licensing rule
    -- (task doc §3.7 item 1 / constants::MIN_PRODUCTION_YEAR).
    minimum_allowed_year integer,
    contains_restricted_contact_fields boolean NOT NULL DEFAULT false,
    -- 'metadata_only' | 'download_when_public' | 'not_applicable'
    document_download_policy text NOT NULL DEFAULT 'not_applicable',
    -- Free-text cadence label (e.g. 'nightly', 'monthly',
    -- 'daily_rolling_365', 'daily_rolling_90', 'continuous', 'irregular')
    -- rather than an enum — KGS/KCC cadences don't map cleanly onto a
    -- small fixed set and a new one showing up shouldn't require a
    -- migration.
    expected_cadence text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT ks_source_code_unique UNIQUE (code),
    CONSTRAINT ks_source_agency_check CHECK (agency IN ('KGS', 'KCC')),
    CONSTRAINT ks_source_document_download_policy_check
        CHECK (document_download_policy IN ('metadata_only', 'download_when_public', 'not_applicable'))
);

INSERT INTO ks_source (code, agency, description, allowed_for_public_product, minimum_allowed_year, contains_restricted_contact_fields, document_download_policy, expected_cadence) VALUES
    ('KGS_WELLS_ARCGIS', 'KGS', 'KGS hosted ArcGIS well-master layer (resolved from KGS_WELLS_ARCGIS_ITEM_ID at runtime).', true, NULL, false, 'not_applicable', 'nightly'),
    ('KGS_WELLS_ZIP', 'KGS', 'KGS static full-extract ZIP of the well master.', true, NULL, false, 'not_applicable', 'nightly'),
    ('KGS_FIELDS', 'KGS', 'KGS field database and field GIS.', true, NULL, false, 'not_applicable', 'irregular'),
    ('KGS_LEASES', 'KGS', 'KGS lease entity extract.', true, NULL, false, 'not_applicable', 'monthly'),
    -- 1990 mirrors constants::MIN_PRODUCTION_YEAR — see
    -- tests/ks_source_registry.rs for the test that keeps them in sync.
    ('KGS_PRODUCTION_OIL', 'KGS', 'KGS monthly oil lease production archives (1990-present only; pre-1987 production is IHS-licensed and never ingested).', true, 1990, false, 'not_applicable', 'monthly'),
    ('KGS_PRODUCTION_GAS', 'KGS', 'KGS monthly gas lease production archives (1990-present only; pre-1987 production is IHS-licensed and never ingested).', true, 1990, false, 'not_applicable', 'monthly'),
    ('KGS_DOCUMENTS', 'KGS', 'KGS well-document pages (C-1, ACO-1, T-1, CP-1/CP-4, tests, logs, plugging, injection). Metadata-first — never bypass a confidential/restricted marker.', true, NULL, false, 'metadata_only', 'irregular'),
    ('KCC_OPERATORS', 'KCC', 'KCC operator license search/list. Operator name and license status are product data; contact/address fields are not.', true, NULL, true, 'not_applicable', 'continuous'),
    ('KCC_INTENTS', 'KCC', 'KCC notice-of-intent-to-drill search, rolling 365-day window. Must snapshot daily.', true, NULL, true, 'not_applicable', 'daily_rolling_365'),
    ('KCC_TRANSFERS', 'KCC', 'KCC notices-of-transfers-approved search, rolling 90-day window. Must snapshot daily.', true, NULL, false, 'not_applicable', 'daily_rolling_90'),
    ('KCC_CONSERVATION_DOCKETS', 'KCC', 'KCC conservation dockets via KCC-Connect (post-November-2025 filings; do not build against legacy eStar for new filings).', true, NULL, false, 'metadata_only', 'daily'),
    ('KCC_KOLAR_DOCUMENTS', 'KCC', 'Public KCC/KOLAR document records. Metadata-first.', true, NULL, false, 'metadata_only', 'irregular')
ON CONFLICT (code) DO UPDATE SET
    agency = EXCLUDED.agency,
    description = EXCLUDED.description,
    allowed_for_public_product = EXCLUDED.allowed_for_public_product,
    minimum_allowed_year = EXCLUDED.minimum_allowed_year,
    contains_restricted_contact_fields = EXCLUDED.contains_restricted_contact_fields,
    document_download_policy = EXCLUDED.document_download_policy,
    expected_cadence = EXCLUDED.expected_cadence,
    updated_at = now();
