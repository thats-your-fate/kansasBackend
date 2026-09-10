# KS-03 — KGS Wells Snapshot Importer: Live Smoke Check

Date: 2026-09-05
Task: KS-03 (KGS statewide well snapshot importer)

## Purpose

The automated test suite (`tests/kgs_wells_arcgis_import.rs`,
`tests/kgs_wells_zip_import.rs`) runs entirely against `wiremock` fixtures
and never touches the live KGS service — that's deliberate (fast,
deterministic, no load on a public service). This doc records the one
manual, real-service check run during KS-03's development, and how to
re-run it if the ArcGIS layer's shape is ever suspected to have drifted.

**Do not add this as an automated `cargo test` case.** A full run against
the live layer touches ~260 requests for 519k records; even the bounded
smoke check below should be run manually, occasionally, not on every CI
run.

## How to run it

```bash
export DATABASE_URL=postgresql://<user>:<pass>@<host>:5432/future_wells_kansas
cargo run --bin ks_ingest -- snapshot-wells --limit-object-ids 6 --count-tolerance-ratio 0.999
```

- `--limit-object-ids 6` caps the run to the first 6 object IDs (a real
  but tiny slice) — this is a testing/ops escape hatch
  (`ImportOptions.limit_object_ids`) that must never be set in a real
  production run.
- `--count-tolerance-ratio 0.999` is set artificially high here only so
  the run doesn't need to worry about tuning it for this smoke check —
  with only 6 of ~519,092 records extracted, the run **will** still end
  up `Rejected` by the count-tolerance gate, and that's correct: it
  proves the QA gate itself is working on live data, not a bug.

## What this verified (2026-09-05)

Run against the real `KGS_WELLS_ARCGIS` item
(`514428df866b46c893b6834ff0bf303d`):

1. **Item resolution**: resolved live to
   `https://services2.arcgis.com/ZOdjAzAQ2B0f85zi/arcgis/rest/services/Oil_and_Gas_Wells_Download/FeatureServer`.
2. **Layer metadata**: real `maxRecordCount` (2000), real field list (53
   fields), real `editingInfo.dataLastEditDate`.
3. **Total count**: `519092` — matches an independent manual `curl` check
   made earlier in the same session.
4. **Object-ID pagination**: cursor-based (`OBJECTID > :after`) paging
   worked correctly against the real endpoint.
5. **Feature batch fetch**: real attributes + real WGS84 geometry
   (`outSR=4326`) for 6 real wells.
6. **Membership classification**: all 6 fetched wells were real Barton
   County, Kansas wells (API prefix `15-009-...`) and were correctly
   classified `included`.
7. **Source re-read + tolerance gate**: metadata was re-read after
   extraction (unchanged, correctly not flagged as a source change), and
   the count-tolerance gate correctly rejected the run (6 extracted vs.
   519,092 estimate) rather than silently publishing a tiny partial
   snapshot.

Sample of what was staged (see `ks_well_raw_snapshot`, since purged after
this check):

| source_object_id | source_kid | api | county | status |
|---|---|---|---|---|
| 150927405 | 1001190703 | 15-009-03905 | Barton | included |
| 150927406 | 1001190737 | 15-009-21936 | Barton | included |
| 150927410 | 1001190836 | 15-009-22036-0001 | Barton | included |

## What this did NOT verify

- A real full 519,092-record run (would take a long time and place real
  load on KGS's public service — not done in this session, and not
  something to do routinely).
- The ZIP fallback path against the real 47MB `KGS_WELLS_STATIC_ZIP` file
  (verified instead via a small synthetic fixture ZIP in
  `tests/kgs_wells_zip_import.rs`, matching the real file's format,
  confirmed separately via a one-time manual `curl`/`unzip -l` check
  during development — see task doc §11's KS-03 entry).
- Retry-after-failure behavior against the real service (verified via
  `wiremock` instead, since deliberately triggering a real 500 from KGS
  isn't practical).

## When to re-run this

- If KS-04 (normalization) reports unexpected field values not seen
  during KS-03 development.
- If `constants::api_digits_belong_to_state`/membership classification
  starts rejecting wells that should be included, or vice versa.
- Roughly every few months, or before a real full production import, as a
  cheap sanity check that KGS hasn't changed the layer's field names, item
  ID, or `maxRecordCount` without notice.
