# KS-08 — KGS Field Importer: Live Smoke Check

Date: 2026-09-05
Task: KS-08 (import Kansas fields and field geometry)

## Purpose

Unlike KS-05/KS-06/KS-07 (all blocked on `kcc.ks.gov` being unreachable),
KGS itself is confirmed reachable from this development environment —
this importer was designed against real live schema discovery and then
verified end-to-end against the real service, the same confidence level
as KS-03/KS-04. The automated test suite
(`tests/kgs_fields_import.rs`, `tests/kgs_fields_materialize.rs`) runs
entirely against `wiremock`/hand-built fixtures — this doc records the
real, live run made during development.

**Do not add this as an automated `cargo test` case.** A full run
downloads all 6 active-field `.xlsx` files, the abandoned-field file, and
(without `--limit-object-ids`) all 8,089 real ArcGIS field features —
real load on a public service, not something to repeat on every CI run.

## How to run it

```bash
export DATABASE_URL=postgresql://<user>:<pass>@<host>:5432/future_wells_kansas
cargo run --bin ks_ingest -- snapshot-fields --limit-object-ids 20 --count-tolerance-ratio 1.0
cargo run --bin ks_ingest -- materialize-fields <run_id>
```

- `--limit-object-ids 20` caps the ArcGIS half of the fetch to 20 real
  features — the testing/ops escape hatch (`ImportOptions.limit_object_ids`),
  same convention as KS-03's wells importer. It does **not** limit the
  six active-file/one abandoned-file `.xlsx` downloads — those are always
  fetched and parsed in full, since they're a fixed, comparatively small
  set of files, not a 8,089-row paginated layer.
- `--count-tolerance-ratio 1.0` avoids the count-tolerance gate rejecting
  a deliberately tiny 20-of-8,089 ArcGIS sample, same reasoning as KS-03's
  own smoke-check doc.

## What this verified (2026-09-05)

Real discovery, before any code was written:
1. **ArcGIS item resolution**: the field layer's item ID
   (`390ee1b9eade40bca42ee3534a813b20`) was found by resolving the KGS
   field index page's ArcGIS Hub link and confirming it via the sharing
   API directly (`curl https://www.arcgis.com/sharing/rest/content/items/390ee1b9eade40bca42ee3534a813b20?f=json`)
   — resolves to `https://services2.arcgis.com/ZOdjAzAQ2B0f85zi/arcgis/rest/services/Oil_and_Gas_Fields/FeatureServer`.
2. **Real schema**: 27 real fields including `FIELD_KID`, `FIELD_NAME`,
   `STATUS` (confirmed exactly two values, `"Active"`/`"Abandoned"`),
   `FIELD_TYPE`, `FORMATIONS`, `APPROXACRE`, `CUMM_OIL`/`CUMM_GAS`,
   `MAXOILWELL`/`MAXGASWELL`, polygon geometry. Real total: 8,089
   features.
3. **`f=geojson` works directly against this service** and correctly
   reprojects to `outSR=4326` — used instead of hand-converting esri's
   native `{"rings": [...]}` ring format, letting the server do the
   outer-ring/hole separation instead of reimplementing that algorithm.
4. **The six active-field `.xlsx` files and the one abandoned-field
   `.xlsx` file are real, confirmed-live downloads** at
   `https://www.kgs.ku.edu/Magellan/Field/<filename>.xlsx` — columns
   confirmed by downloading and inspecting each file directly:
   `FIELD_NAME`, `PROD_ZONES`, `DISCOVERY`, `TOTAL_OIL_PROD`,
   `TOTAL_GAS_PROD`, `ACRES`, `TOTAL_WELLS`, plus repeating
   `<YEAR>_OIL_PROD`/`<YEAR>_OIL_WELLS`/`<YEAR>_GAS_PROD`/`<YEAR>_GAS_WELLS`
   columns in the active files. **A real inconsistency found during
   discovery**: different files use different XML namespace URIs inside
   their `.xlsx` XML parts (some `http://schemas.openxmlformats.org/spreadsheetml/2006/main`,
   others `http://purl.oclc.org/ooxml/spreadsheetml/main`) — a hand-rolled
   Python inspection script that hardcoded one namespace silently returned
   zero rows for files using the other. `calamine` (the Rust crate this
   importer uses) handles both correctly, confirmed live — see below.

Real live run: `snapshot-fields --limit-object-ids 20 --count-tolerance-ratio 1.0`
against the real service:
- 20 real ArcGIS field features staged (real item resolution, real
  metadata, real count of 8,089, real cursor pagination, real GeoJSON
  polygon geometry).
- All 6 active-file `.xlsx` downloads succeeded and parsed:
  **42,639 total rows** across the six files, real production data
  spanning 1970–2026.
- The 1 abandoned-file `.xlsx` download succeeded and parsed:
  **620 rows** (matches the file's own dimension, `A1:G621`, confirmed
  independently while inspecting the file during discovery).

Then `materialize-fields` against that run's staged data:
- **8,078 canonical `ks_field` rows created**, 0 updated (first run).
- **2 real invalid geometries found and correctly flagged, not silently
  repaired**: KGS's own `Chase-Silica` and `Lost Springs` field polygons
  both have a self-intersecting ring in the live ArcGIS data itself (a
  real KGS data-quality issue, not an artifact of this importer) —
  `geometry_is_valid = false` with `ST_IsValidReason`'s exact
  self-intersection coordinates stored in `geometry_validity_reason`, and
  the geometry itself is still stored (never discarded or auto-repaired
  via `ST_MakeValid`, which would have silently altered KGS's own
  reported boundary).
- Active-status distribution in this run:
  `active=20` (the real ArcGIS sample), `abandoned=620` (the abandoned
  file), `unknown=7,438` (fields seen only in the active-file `.xlsx`
  enrichment data, not in the deliberately tiny 20-feature ArcGIS
  sample — a real full run with the ArcGIS limit removed would resolve
  nearly all of these to `active`, since ArcGIS is the authoritative
  status source and the active-file rows by definition describe fields
  with production activity).
- Real per-year annual production correctly merged, e.g. `HUGOTON GAS AREA`:
  2026 partial-year gas production of 17,812,365 MCF across 6,831
  contributing wells, alongside its lifetime cumulative totals
  (`CUMM_GAS = 27,246,498,244`) from the ArcGIS layer itself.
- 20 `ks_field_source_alias` rows (`field_kid`), exactly matching the 20
  real ArcGIS-sourced fields in this sample.

## A real performance characteristic found during this run

Materializing ~8,000 distinct field names took several minutes — each
field involves multiple sequential `await`ed round-trips (an upsert, an
alias insert when applicable, and one insert per year of annual
production, which for a field active since the 1970s can be 50+ rows).
This is not a correctness bug (every test above passed, and the live
counts above are correct), but it is a real scaling characteristic worth
flagging before this runs unattended against the full ~8,000-field
dataset in production: a future optimization pass should batch the
per-year production inserts (e.g. `UNNEST`-based multi-row `INSERT`)
rather than one `INSERT` per `(field, year)` pair. Not fixed in this task
since correctness, not throughput, was the acceptance bar — but a real
production cron run should expect this to take on the order of minutes,
not seconds, until that optimization lands.

## What this did NOT verify

- A full, un-limited 8,089-feature ArcGIS run (would take real time and
  load on KGS's public service — the 20-feature sample above is
  sufficient to prove the pipeline is correct end-to-end; a full run
  would only add more of the same, well-understood field rows).
- Real multi-part field geometry merging (`ST_Collect`/`ST_Multi`) against
  a real multi-part field specifically — confirmed live that multi-part
  fields exist (e.g. `FOREST CITY COAL GAS AREA` and `LEIKER EAST` each
  appear as 2 separate ArcGIS features with the same `FIELD_NAME`), but
  neither happened to fall inside this run's first-20-object-IDs sample;
  the merge logic itself is covered by `tests/kgs_fields_materialize.rs`
  fixtures instead.
- Well-to-field reconciliation against real `ks_well` data — no wells
  were materialized in this same database at the time of this run; the
  join logic itself is covered by a fixture test.

## When to re-run this

- If `parse::kgs_fields::xlsx_source`'s header-matching starts silently
  dropping columns — worth checking whether KGS renamed a column.
- Before a real production import, as a cheap sanity check that KGS
  hasn't changed the ArcGIS item ID, the `.xlsx` filenames, or the field
  layer's schema without notice.
- If `FieldWellCountReconciliationReport` starts showing large,
  systematic discrepancies between KGS's reported max well counts and
  this codebase's own materialized well counts for the same field name.
