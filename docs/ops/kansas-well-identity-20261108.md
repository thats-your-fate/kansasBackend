# Kansas well names in shared exports

KGS's `WELL` (ArcGIS) / `WELL_NAME` (ZIP) field is the well identifier,
including alphanumeric values such as `W-5`. The lease name is separately
reported as `LEASE` / `LEASE_NAME`. The native database preserves these source
fields as `ks_well.well_name` and `ks_well.lease_name`.

The shared exporter previously copied the identifier into `WellV1.well_name`
and left `well_number` empty. Future Wells Co consequently displayed `34`
instead of the available name `SOUTH MYNATT`.

The exporter now emits `SOUTH MYNATT #34` as `well_name` and `34` as
`well_number`. It trims surrounding whitespace, keeps an available name or
identifier when the other is missing, and preserves the original fields in
`source_payload.lease_name` and `source_payload.source_well_name`. Native KGS
records, API identities, source keys and production relationships are unchanged.

This mapping applies to every full export, including the manual admin export.
Existing platform records require a corrected full package through
`scripts/kansas_sync.py --export-id UUID --replace-baseline` on Future Wells Co;
rebuilding only the regional frontend cannot repair the imported records.

Regression checks: `cargo test --lib identity_tests`, plus the canonical-to-
package assertions in `tests/state_export_v1.rs`. The mapping examples include
SOUTH MYNATT #34, L-J #1, WRESTLER #19 and an alphanumeric identifier.

Production QA on 2026-11-08: the native `ks_ingest` release build and exporter
preflight passed. Package `904a4909-001b-4bc7-85e7-1cb783afaf0e` was generated
and delivered through the existing manual exporter. All 477,520 well records
were checked against their original lease name and identifier; compressed
well-part checksums and record counts also passed. The actual Future Wells Co
search page displayed SOUTH MYNATT #34 / 34, L-J #1 / 1 and WRESTLER #19 / 19
after the well batches imported. The active well count remained 477,520;
474,602 source records have a nonempty identifier and 475,044 have a nonempty
lease name. Missing source fields remain missing.

The full import and read-model refresh completed at 12:11:55 UTC. It updated
477,520 well records, inserted 3,300 newly collected records, reported zero
failures and zero deactivations, and confirmed acknowledgement on both sides.
