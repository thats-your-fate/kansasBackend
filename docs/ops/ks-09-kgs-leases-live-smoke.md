# KS-09 — KGS Lease Dimension Importer: Live Smoke Check

Date: 2026-09-05
Task: KS-09 (build the Kansas lease dimension and well-to-lease relationships)

## Purpose

Like KS-08, KGS is confirmed reachable from this development environment
— this importer was built against real live discovery and verified
against the real service. The automated test suite
(`tests/kgs_leases_import.rs`, `tests/kgs_leases_materialize.rs`) runs
entirely against hand-built fixtures — this doc records the real runs
made during development.

**Do not add the full-scale import as an automated `cargo test` case.**
The real archive is a ~4.3MB ZIP (~23MB uncompressed) with 98,639 rows —
fine to fetch occasionally, not on every CI run.

## Real discovery before any code was written

1. Downloaded and inspected the real `KGS_ALL_LEASES_ZIP`
   (`https://www.kgs.ku.edu/PRS/Ora_Archive/all_kansas_leases.zip`)
   directly: a single `all_kansas_leases.txt`, quoted-comma CSV, header
   `LEASE_KID,LEASE,DOR_CODE,API_NUMBER,OPERATOR,COUNTY,TOWNSHIP,TWN_DIR,
   RANGE,RANGE_DIR,SECTION,SPOT,LATITUDE,LONGITUDE,PRODUCES,PRODUCTION,
   YEAR_START,YEAR_STOP,URL`, 98,639 real data rows, zero duplicate
   `LEASE_KID` values.
2. **A real, confirmed discrepancy between the task doc and the actual
   file**: the task doc's field list includes "field" (a field-name
   association), but the real file has no `FIELD` column at all.
   `ks_lease.field_name` exists as a real column but is always `NULL`
   until a different source is wired in — documented in migration
   `0018_ks_lease.sql`'s own comment, not silently dropped from the
   schema.
3. **`PRODUCES` is sometimes the literal string `"NULL"`** (2,359 of the
   98,639 real rows), not a blank value — confirmed by directly counting
   values in the downloaded file. `normalize::kgs_leases::derive_product`
   handles this explicitly.
4. **`API_NUMBER` can list more than one API number**, comma-separated
   (e.g. `"15-131-20143, 15-131-20089-0002, 15-131-20239"` for a single
   lease record covering several wells) — confirmed directly in the real
   file, and the mechanism behind the task doc's "API numbers listed in
   production data" well-linking tier.
5. Fetched `KGS_LEASE_FORMAT`'s documented page and found it describes a
   **different file's** columns (the monthly production file — `PRODUCT`,
   `WELLS`, `MONTH-YEAR`, `FIELD`) than the all-leases archive actually
   contains — a reminder that a documented format page and a real
   downloaded file are not automatically the same shape; the real
   downloaded file's own header row is the ground truth used here.
6. Confirmed KGS's own documented note on `LEASE_KID`: *"Not necessarily
   permanent"* — the same justification already used for well `KID` at
   KS-04, applied identically here (`ks_lease.id` is the real primary key;
   `LEASE_KID`/`DOR_CODE` live in `ks_lease_source_alias`).

## What this verified (2026-09-05)

**`snapshot-leases` against the real service** (no limit flag exists or
is needed — this is a single flat file, not a paginated layer):
- Real 4,289,823-byte ZIP downloaded, `Last-Modified: Fri, 07 Aug 2026
  22:07:56 GMT`.
- **All 98,639 real rows staged successfully, 0 rejected** (every real
  row had a `LEASE_KID`).

**`materialize-leases` against a 200-row real sample** (copied from the
full real run's staged rows into a separate completed run, rather than
materializing all 98,639 — see "known scaling characteristic" below):
- **200 real leases created**, 0 updated (first run).
- **231 real API-number tokens found across those 200 leases**, all
  correctly reported via `unresolved_lease_apis_report` — expected, since
  no `ks_well` rows existed yet in this particular database at the time
  of this run to resolve them against. This is exactly the intended
  behavior of the "unresolved production APIs are reported" acceptance
  criterion: a token that can't be resolved is recorded, not silently
  dropped and not silently guessed.
- Real lease fields (`reported_lease_name`, `operator`, `county`, PLSS,
  `source_latitude`/`source_longitude`, `cumulative_production`,
  `year_start`/`year_stop`) all populated from real KGS data.

## A real scaling characteristic found during this task (same shape as KS-08's)

Materializing even 200 real leases took several seconds; each lease
involves sequential round-trips (an upsert, up to two alias upserts, and
one query per listed API number). Extrapolating to the full 98,639-row
archive at the same observed per-row rate would take on the order of
hours, not minutes — meaningfully worse than KS-08's ~8,000-field case,
since a full production run here is over 10x larger. **This task did not
run a full-scale materialize** for that reason; correctness was verified
against the 200-row real sample and the full fixture-based test suite
instead. The same future optimization recommended for KS-08 (batch
inserts, e.g. `UNNEST`-based multi-row `INSERT`) applies here, more
urgently given the larger row count — see
`docs/architecture/kansas-source-boundary.md`'s KS-09 items.

## What this did NOT verify

- A full-scale materialize of all 98,639 real lease rows (see above).
- Real well-to-lease linking against real `ks_well` data — no wells were
  materialized in the same database at the time of the 200-row lease
  sample run (a real wells snapshot was started but not completed in
  this session due to unrelated slowness fetching from the live ArcGIS
  service; abandoned for time rather than investigated further). The
  well-linking tiers themselves (exact identifier scoped by product,
  exact API number, name-match fallback, and the oil/gas
  non-collision guarantee) are covered by `tests/kgs_leases_materialize.rs`
  fixtures instead, including a deliberate same-numeric-value
  oil-vs-gas collision test.

## When to re-run this

- If `parse::kgs_leases::zip_source`'s column-name matching starts
  silently dropping a column — worth checking whether KGS renamed one.
- Before a real production import, as a cheap sanity check that the ZIP's
  internal filename, header row, or `PRODUCES`/API-number conventions
  haven't changed.
- Once the per-row materialize performance is optimized, re-run at full
  scale and update this doc with real end-to-end well-linking numbers
  against real `ks_well` data.
