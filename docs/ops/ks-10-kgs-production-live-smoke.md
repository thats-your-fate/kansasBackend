# KS-10 — KGS Monthly Lease Production Importer: Live Smoke Check

Date: 2026-09-05
Task: KS-10 (import Kansas monthly lease production from 1990-present)

## Purpose

KGS is confirmed reachable from this development environment, as with
KS-08/KS-09. This importer was built against real live discovery and
verified at **full real scale**, not just fixtures — the dataset here
(11.68M real rows) is an order of magnitude larger than KS-08/KS-09's,
so the batch-insert and pagination-index fixes below were only found by
actually running it end to end. The automated test suite
(`tests/kgs_production_import.rs`, `tests/kgs_production_materialize.rs`)
still runs entirely against hand-built fixtures — this doc records the
real runs made during development.

**Do not add the full-scale import as an automated `cargo test` case.**
The 8 real archives total roughly 190MB compressed and 11.68M rows —
fine to fetch occasionally, not on every CI run.

## Real discovery before any code was written

1. Downloaded and inspected all 8 real decade archives
   (`KGS_PRODUCTION_FILES`: oil/gas × 2020-present/2010-2019/
   2000-2009/1990-1999) directly. Real header, identical across all 8
   files: `LEASE_KID,LEASE,DOR_CODE,API_NUMBER,FIELD,PRODUCING_ZONE,
   OPERATOR,COUNTY,TOWNSHIP,TWN_DIR,RANGE,RANGE_DIR,SECTION,SPOT,
   LATITUDE,LONGITUDE,MONTH-YEAR,PRODUCT,WELLS,PRODUCTION,URL`.
2. This is genuinely the file KS-09's own `KGS_LEASE_FORMAT` discovery
   note pointed at (KS-09's live-smoke doc flagged that the documented
   format page described a *different* file than the all-leases
   archive it was importing) — confirmed here: `PRODUCT`, `WELLS`, and
   `MONTH-YEAR` are real columns in this file, not the all-leases one.
3. **`MONTH-YEAR` is a hyphen-joined `"M-YYYY"` token, not a date type**
   — confirmed directly, including real negative-month sentinel values.
   A real `"0-1996"` row (yearly-summary sentinel for 1996) was found in
   the actual downloaded 1990-1999 oil archive during discovery,
   confirming KGS genuinely emits both the `0` (yearly total) and `-1`
   (starting cumulative) sentinels described in the task doc, not just
   a theoretical edge case.
4. **`PRODUCT` is `O` or `G`** in every real row sampled — confirmed
   directly; `normalize::kgs_production::parse_product` rejects any
   other value rather than guessing.
5. Confirmed all 8 files share the same `LEASE_KID` identity space as
   KS-09's lease dimension (spot-checked several real `LEASE_KID`
   values from the production files against the all-leases archive).

## What this verified (2026-09-05)

**`snapshot-production` against all 8 real files, full scale:**
- All 8 real archives downloaded and staged in one run.
- **11,678,706 real rows staged, 0 rejected**, in approximately 9
  minutes 15 seconds (roughly 21,000 rows/sec sustained, via the
  `sqlx::QueryBuilder`-batched staging insert — the fix built into this
  task from the start, given KS-08/KS-09's documented-but-unfixed
  per-row scaling problem).
- No pre-1990 rows were blocked in the real run, since the earliest
  configured archive (`1990-1999`) starts exactly at the enforced
  floor; the pre-1990 guard itself is covered by real fixture tests
  (`pre_1990_rows_are_blocked_unless_the_legal_review_override_is_enabled`,
  `pre_1990_override_flag_allows_ingestion_when_explicitly_enabled`),
  not by a real out-of-range row, since KGS does not appear to publish
  one in these 8 files.

**A real O(n²) pagination bug found and fixed during this task's own
live verification (not visible at fixture scale):**
- The first `materialize-production` attempt against the largest
  single completed run (670,863 rows) was still running after 2+
  minutes, having processed only ~44,000 rows — versus staging's
  ~21,000 rows/sec, this was over 100x slower than expected.
- Root cause: keyset pagination (`WHERE ingestion_run_id = $1 AND id >
  $2 ORDER BY id LIMIT N`) was backed only by a plain `(ingestion_run_id)`
  index in migration `0019_ks_lease_production_staging.sql`, forcing
  Postgres to scan and sort the entire remaining tail of the run on
  every page — classic O(n²) behavior that only shows up once a run
  has hundreds of thousands of rows.
- **Fix**: edited `0019_ks_lease_production_staging.sql` in place
  (nothing had shipped yet, same precedent as KS-04) to replace the
  plain index with a composite `(ingestion_run_id, id)` index.
- **Verified with a real before/after comparison**: recreated the test
  database fresh, re-ran the identical full `snapshot-production`
  import again (11,678,706 rows across all 8 files, same result), then
  re-ran `materialize-production` against the same 670,863-row run.
  **Completed in 51 seconds** (~13,150 rows/sec) — a ~36x improvement.
  Recorded as a general lesson in `docs/architecture/kansas-source-boundary.md`
  (item 62): keyset pagination requires a composite index on exactly
  `(filter_column, sort_column)`, not just the filter column alone.

**Full-scale `materialize-production` across all 8 real runs — complete:**
- Materialized all 8 completed real runs with the fixed index:
  670,863 (the run used for the before/after timing comparison above)
  + 1,213,220 + 1,303,009 + 2,365,784 + 1,096,832 + 2,089,592 +
  1,317,536 + 1,621,870 = **11,678,706 rows created, 0 updated, 0
  skipped for an unmapped month, 0 skipped for a product mismatch.**
- **This exactly matches the 11,678,706 rows staged** — confirmed
  independently with `SELECT count(*) FROM ks_lease_production_month`
  against the real materialized table, not just by summing the CLI's
  own per-run output. Every real staged row was materialized into
  exactly one canonical row; nothing was silently dropped or
  double-counted.
- Confirmed correction-safe, real oil/gas non-collision, and real
  sentinel/monthly `record_kind` separation at full scale (not just in
  fixtures) by spot-checking `ks_lease_production_month` counts and
  `record_kind` distributions against the known per-file row counts.

## What this did NOT verify

- A real materialize-refuses-a-non-completed-run check against production
  data specifically (covered by fixture test only — not relevant to a
  full-scale run, since all 8 real runs did complete).
- A real reconciliation-report discrepancy at full scale (the
  reconciliation logic itself, including a deliberately-constructed
  disagreement between summed monthly rows and a reported yearly
  total, is covered by `tests/kgs_production_materialize.rs`'s
  `reconciliation_report_flags_a_real_discrepancy` fixture test) — no
  full real state/county/field reconciliation run against all
  11.68M materialized rows was performed in this session, since the
  reconciliation report's own cost (aggregating the full materialized
  table) was deprioritized once the pagination fix was confirmed
  correct at the 670K-row scale.
- Real well-to-lease-to-production linking end-to-end (this importer
  keys directly off `lease_kid`/`product`/`year`/`month`, matching the
  task doc's specified upsert key; it does not itself join through
  `ks_well`, so no additional real well-linking verification was
  needed beyond what KS-09 already covered for the lease dimension).

## When to re-run this

- Before a real production import, as a cheap sanity check that any of
  the 8 files' internal filename, header row, or `MONTH-YEAR`/`PRODUCT`
  conventions haven't changed.
- If a future KGS publication introduces a sentinel value other than
  `0`/`-1` — `normalize::kgs_production::parse_month_year` should be
  checked against the real file first, not assumed.
- Once a full real reconciliation-report run is prioritized, re-run it
  against all 11.68M materialized rows and update this doc with real
  discrepancy counts (if any) against KGS's own yearly totals.
