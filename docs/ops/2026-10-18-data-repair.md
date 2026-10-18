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

2026 source processing completed: 50,300 oil and 84,957 gas monthly records,
January through June; zero invalid months/products. No later reporting months
were present. No inactive descriptions remain mapped Producing.

The database-wide publication monitor still reports an existing 1985 gate-test
fixture and a synthetic phone-number operator fixture; neither belongs to the
real-ingestion export. Its address detector also flags public well/lease
identifiers such as `6 LANE TAP` and `53 STATE STREET`. These are explicitly
classified here as KGS well/lease names in the allowed identifier fields, rather
than operator contact-address fields. Preserve those identifiers; do not silently
rewrite source names or disable the monitor. This is not a claim that the global
database-wide gate passed. The delivered production scope must independently
verify as 2026 only and exclude fixture ingestion provenance.

Export/upload completed with the existing source server tooling:
`5d725381-cf85-48ac-bc32-bea6affe51a4`, 724 parts, 208,009,261 compressed bytes.
Manifest SHA-256:
`5884791ac2da38d5b9c8fe4ac984220657dfb121b9e24e4fa9ffeb19a7b6a828`.
The package has 477,274 wells, 1,466,259 identifiers, 477,274 wellbores,
98,661 leases, 904,477 recorded historical KGS events, and 135,257 monthly
production records. A separate scan of every exported production record verified
2026 January–June only, null well allocation keys, lease allocation basis,
50,300 oil rows totaling 12,250,797.33, and 84,957 gas rows totaling 55,225,444.
Production remains one record per lease/product/month; associated well views
must not turn those volumes into direct well production or duplicate state totals.

The exporter service now has
`~/.config/systemd/user/future-wells-ks-export.service.d/30-production-year.conf`
setting `KS_EXPORT_PRODUCTION_YEAR=2026`; change that explicit scope when another
year is requested. The Kansas API pool was restored to two connections. The
all-years backfill workers remain stopped.

Validation: 149 Kansas tests passed (126 library, 10 production materialization,
2 repair/year-scope tests, 11 exporter tests), and strict library/binary Clippy
passed. The shared-platform lease-association regression also passed, including
shared lease visibility without duplicating state totals and profile month counts.
Five broader shared-platform well-suite failures were reproduced against its
pre-change baseline; this delivery does not claim the entire existing suite passes.
Activity coverage is recorded KGS history; the live KCC host remains unavailable
through DNS from the ingestion host.

Target import completed and both acknowledgements were confirmed for this export.
Import run: `897b1472-f6be-4045-bb6c-561fc4da2bd0`; 724 completed batches,
1,138,395 inserted, 523,388 updated, 1,897,524 unchanged, zero failed/deactivated.
The source acknowledgement needed a retry because ordinary upstream connection
slots were exhausted; that shared database capacity issue is still separate work.
The target counts match the manifest: 477,274 wells, 904,477 events, and 135,257
monthly production records. 430,029 wells have a mapped type; zero inactive
descriptions are labelled Producing. Live list, profile, and production endpoints
confirmed verified lease coverage and profile month counts; an inactive well was
excluded by the Producing filter. Imported oil/gas totals match the separate
package audit exactly. The active production periods are January–June 2026 only.

Server verification reports:
- Source: `~/state-sync/kansas-repair/export-2026-audit.json`.
- Target: `~/state-sync/kansas-repair/verification-2026.json`.
- Sync/receipts: `~/state-sync/kansas-sync/latest.json` on Future Wells Co.

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
