# KS-04 — Well Normalizer/Materializer: Live Smoke Check

Date: 2026-09-05
Task: KS-04 (normalize Kansas wells, identifiers, dates and locations)

## Purpose

The automated test suite (`tests/kgs_wells_materialize.rs`) runs entirely
against hand-built fixture rows inserted directly into
`ks_well_raw_snapshot` — deliberately, so tombstone/identity/status-mapping
edge cases (malformed API numbers, sidetracks, historical short-form APIs,
unmapped status combinations) can be constructed precisely rather than
hoped for in real data. This doc records the one manual, real-data check
run during KS-04's development, chaining a real KS-03 snapshot straight
into `materialize-wells` against actual production KGS records, and how to
re-run it.

**Do not add this as an automated `cargo test` case.** Same reasoning as
`docs/ops/ks-03-kgs-wells-live-smoke.md`: it depends on the live KGS
service and is meant to be run manually, occasionally.

## How to run it

```bash
export DATABASE_URL=postgresql://<user>:<pass>@<host>:5432/future_wells_kansas

# 1. Stage a small real snapshot. Unlike the KS-03-only smoke check, this
#    run must reach `completed` (not `rejected`) since materialize-wells
#    only ever reads a completed run — so the count-tolerance gate has to
#    be set wide enough that a tiny real sample doesn't trip it.
cargo run --bin ks_ingest -- snapshot-wells \
  --limit-object-ids 10 --count-tolerance-ratio 1.0

# 2. Materialize the run's staged rows into ks_well. Use the run_id
#    printed by step 1.
cargo run --bin ks_ingest -- materialize-wells <run_id>

# 3. Confirm no unmapped lifecycle-status combination was silently
#    dropped.
cargo run --bin ks_ingest -- lifecycle-qa-report
```

- `--limit-object-ids 10` and `--count-tolerance-ratio 1.0` are the same
  kind of testing/ops escape hatch as KS-03's doc: real network calls,
  real records, but a deliberately tiny slice, and a tolerance wide enough
  that a 10-of-519,092 sample doesn't get rejected by the QA gate that
  exists precisely to reject small/partial extractions in production. Never
  use either flag in a real production run.

## What this verified (2026-09-05)

Chaining a real KS-03 run (10 real Barton County wells, same source as
`docs/ops/ks-03-kgs-wells-live-smoke.md`) into `materialize-wells`:

1. **Identity resolution**: all 10 wells resolved a valid API-14
   (`api_validation_status = 'valid'`), no `kid_fallback` needed for this
   sample.
2. **Lifecycle status derivation**: 9 of 10 wells mapped correctly
   (`planned`, `intent_expired`, `plugged`) against their real
   `WELL_CLASS`/`STATUS`/`STATUS2` combination. The 10th —
   `WELL_CLASS="DEVELOPMENT"` / `STATUS="OIL"` / `STATUS2="DEVELOPMENT"`, a
   combination not seen anywhere during development or in the fixture
   tests — correctly resolved to `unknown` rather than being guessed, and
   appeared in `lifecycle-qa-report`'s output. This is the intended
   behavior (task doc §3.5's "no silent default"), not a bug to fix by
   special-casing one sample.
3. **Geometry**: since all 10 rows came from the ArcGIS path,
   `display_geometry_4326` was populated for all 10 (the `'zip'`-only
   `location_warning` path was not exercised here — it's covered instead
   by `tests/kgs_wells_materialize.rs`'s fixtures).
4. **Source lineage**: every materialized row carried its real
   `source_format = 'arcgis'`, `source_ingestion_run_id`, and
   `source_object_id`, traceable back to the exact KS-03 run.

## What this did NOT verify

- Materialization of a `'zip'`-sourced run against real data (the ZIP path
  is covered by fixtures in `tests/kgs_wells_materialize.rs` and
  `tests/kgs_wells_zip_import.rs` instead — a live ZIP-to-materialize chain
  hasn't been run against the real 47MB file in this environment).
- The tombstone rule (two consecutive missing snapshots) against real
  data across real successive days — verified instead via
  `tests/kgs_wells_materialize.rs`'s 4-run fixture sequence, since forcing
  two real days of KGS snapshots isn't practical in one development
  session.
- A real API-10 short-form (`kid_fallback`) identity match — none of the
  10 sampled wells needed it; covered instead by
  `src/normalize/kgs_wells.rs`'s own unit tests and
  `tests/kgs_wells_materialize.rs`'s `kid_fallback` fixture test.

## When to re-run this

- If KS-05 (KCC operators) or any later task reports well data that looks
  inconsistent with what KGS actually publishes.
- If `lifecycle-qa-report` starts showing a large or growing number of
  `unknown` combinations in production — worth checking whether KGS
  changed its `WELL_CLASS`/`STATUS`/`STATUS2` vocabulary.
- Roughly on the same cadence as `docs/ops/ks-03-kgs-wells-live-smoke.md`
  (its "when to re-run" list applies here too, since this check is
  downstream of that one).
