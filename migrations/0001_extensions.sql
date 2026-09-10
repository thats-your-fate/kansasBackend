-- KS-001 scaffold: enable the extensions the fixed architecture depends on
-- (PostGIS for well/field geometry, pg_trgm for search). No business
-- tables here — those start once source ingestion lands (KS-004+) per
-- docs/architecture/kansas-source-boundary.md.

CREATE EXTENSION IF NOT EXISTS postgis;
CREATE EXTENSION IF NOT EXISTS pg_trgm;
