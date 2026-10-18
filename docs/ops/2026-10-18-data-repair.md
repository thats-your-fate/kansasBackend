# Kansas data mapping and ingestion repair

Verified before repair: the real export has 477,274 wells, zero production and
zero events. 85,858 wells are mapped Producing; 14,492 explicitly say Inactive Well.
The canonical upstream database has only fixture production/lease rows.

KGS STATUS is a type/purpose code (including OIL/GAS/O&G/CBM and P&A suffixes),
while STATUS2 adds lifecycle information. Map type separately, match whole status
words, and keep Unknown for unmapped combinations. Plugged SWD wells are Plugged,
with Saltwater Disposal retained as their type. Mapping repairs emit no events.

Official field definitions:
- https://www.kgs.ku.edu/PRS/file_format.html
- https://www.kgs.ku.edu/Magellan/Elog/status.html
- https://www.kgs.ku.edu/Magellan/Field/lease_file_format.html

Operational commands (run from the backend directory after migrations):

Scope updated: deliver **2026 production only**. The all-years materializer was
stopped; older partially materialized rows stay in the source database and are
excluded from this delivery. Do not resume the historical runner for this task.
Use the two completed `2020_present` official archive runs with
`ks_ingest materialize-production RUN_UUID --year 2026` after marking them as
backfills. Export with `--production-year 2026` (or
`KS_EXPORT_PRODUCTION_YEAR=2026` in the existing exporter service). The year filter
also excludes annual/cumulative sentinel rows from the monthly export as before.
Local regression tests verify year-limited processing, idempotence, lease-level
volumes, and exclusion of other years from an otherwise full entity snapshot.

Original broad repair commands, retained for operator reference:

1. `ks_ingest repair-well-mappings`
2. `ks_ingest snapshot-leases`, then `materialize-verified-leases RUN_UUID`.
3. `ks_ingest snapshot-production`, mark each completed archive run as backfill,
   then `materialize-production RUN_UUID` for all eight configured archives.
4. `ks_ingest backfill-well-history` explicitly builds dated historical facts.
   It excludes future/implausibly old dates and is idempotent. Export titles retain
   historical labels and original dates; this does not invent status transitions.
5. Use the server's existing manual export/upload tooling. Verify the package and
   import with the existing Kansas sync runner, then acknowledge and run live smoke checks.

Verified lease associations use product-specific KGS identifiers, unique DOR IDs,
or the APIs listed by KGS. Names alone are excluded. Each lease/month is exported
once with `well_source_key=null`, `source_allocation_type=lease`; associated wells
carry verified link metadata for shared-platform lookups. Volumes must never be
summed across well views. Year-total/cumulative sentinels are not calendar months.

Failed runs retain provenance and may retry the same checksum after being marked
failed. A running/completed checksum remains unique. Statewide lease staging uses
batch writes. Set `KS_DB_MAX_CONNECTIONS` (default 4) to bound pool demand.

Rollback: restore previous API binary and previous export package; source rows
and historical lineage are retained. Re-import the previous package with explicit
baseline policy if a rollback of canonical data is necessary. Do not erase history
or overwrite source raw fields to undo display mappings.
