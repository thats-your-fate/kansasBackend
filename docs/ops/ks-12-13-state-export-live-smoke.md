# KS-12/KS-13 — `state-export-v1` Exporter: Live Smoke Check

Date: 2026-09-05
Task: KS-12 (Kansas map layer + clustering pipeline), KS-13 (Kansas well/lease/operator/field/county details)

## Why this doc exists, and what "verified" means here

Both tickets explicitly forbid building a standalone Kansas map or a
standalone Kansas entity-detail UI — they require extending the existing
shared Future Wells Co platform instead. That platform lives in
`futureBackend` (the shared map/clustering/entity-detail API) and
`futureMarketing` (the shared UI), not in this repo. Before writing any
code, this session got explicit access to both sibling repos and had them
independently investigated:

- `futureBackend` already has a mature, state-agnostic map/clustering API
  (zoom-tiered clustering, point-only well layer, a separate wellbore/
  lateral overlay endpoint, generic well/operator/county/regulatory-area
  detail routes) and Kansas was **already registered** there as a
  `state-export-v1` data source (`data_sources.source_code =
  'kansas_backend'`, `jurisdictions.rollout_stage = 'flagged'`) — this
  repo's own architecture doc had been tracking that gap for months under
  the internal placeholder name "KS-020" (see item 18 in
  `docs/architecture/kansas-source-boundary.md`).
- `futureMarketing` already has a mature, multi-state map (MapLibre GL,
  server-side clustering already consumed correctly) and generic
  well/operator/county/field-or-lease detail pages, with Kansas already
  stubbed into `lib/user-area/capabilities.ts` as `"planned"`.
- Neither shared repo has field/county boundary geometry, a lease entity
  with its own detail route, or a cache layer, for **any** state, not just
  Kansas — these are real, concrete gaps in the shared platform itself,
  not something a Kansas-side export can work around. See
  `docs/ops/ks-12-13-shared-infra-handoff.md`.

Given that, **this repo's entire real scope for KS-12/KS-13 is the
`state-export-v1` exporter** — the mechanism by which Kansas's canonical
tables (`ks_well`, `ks_operator`, `ks_field`, `ks_lease`,
`ks_lease_production_month`, `ks_drilling_intent`,
`ks_operator_transfer`, `ks_event`) reach the shared platform's own
database at all. This doc records what was verified about that exporter.
Building the futureBackend/futureMarketing-side gaps (schema additions,
cache, UI wiring) is out of this session's scope by explicit user
decision — see the handoff doc's introduction.

## What this verified (2026-09-05)

**A real, working reference implementation existed and was read before
writing any code**: New Mexico's backend (`newMexicoBackend/src/
state_export/`) already implements `state-export-v1` against
`futureBackend`'s real importer, including a CLI (`state-export-v1
preflight|full|incremental|verify|acknowledge|status|cleanup-partials`)
already wired into `futureBackend`'s own `state-sync-v1` orchestration
tool (`futureBackend/src/bin/state_sync_v1.rs`), which invokes a source
state backend's compiled binary as a subprocess and expects a specific
JSON envelope (`"command_version": "state-sync-cli-v1"`) on every command.
Kansas's exporter was built to be a drop-in-compatible `source_command`
for that same orchestration tool, not just a same-named CLI — `contract.rs`,
`hashing.rs`, and `package.rs` are ported at the byte level (they are
the shared protocol, not Kansas-specific); `keys.rs` and `exporter.rs`
are Kansas-specific, built against the real schemas in this repo's own
migrations.

**Real end-to-end CLI run**, via the actual compiled `ks_ingest` binary
(`cargo build --release`, not just library unit tests) against a real
local Postgres instance:

```
$ ks_ingest state-export-v1 preflight
state-export-v1 preflight passed

$ ks_ingest state-export-v1 full --output-dir <dir> --json
{"command_version":"state-sync-cli-v1","success":true,"state_code":"KS",
 "source_code":"kansas_backend","schema_version":"state-export-v1",
 "export_id":"83dedc4e-...","mode":"full","records_total":187,
 "records_by_entity":{"counties":5,"operators":9,"regulatory_areas":18,
 "wells":29,"well_identifiers":25,"wellbores":29,"regulatory_records":20,
 "events":18,"production_monthly":10,"tombstones":24, ...0 for the rest},
 ...}

$ ks_ingest state-export-v1 verify <dir>/<export_id>
state-export-v1 package verified: export_id=83dedc4e-... files=15

$ ks_ingest state-export-v1 acknowledge --export-id <id>
state-export-v1 acknowledged: consumer=future-wells-co export_id=83dedc4e-...
$ ks_ingest state-export-v1 acknowledge --export-id <id>   # run again
Error: no unacknowledged export 83dedc4e-... found for consumer future-wells-co
```

This confirms, against real compiled code (not mocks): the package
directory layout, manifest/checksum format, and real zstd-compressed
NDJSON part files are all produced correctly and independently
re-verifiable (`verify_package` re-hashes every part file against the
manifest); the acknowledge/status bookkeeping table
(`ks_state_export_run`, migration `0021`) round-trips correctly and
rejects a duplicate acknowledgement rather than silently succeeding.

A decoded sample record from the real run (zstd-decompressed by hand for
inspection):

```json
{"source_key":"ks:well:0834a1e0-...","source_updated_at":"2026-09-05T19:28:42Z",
 "source_payload":{"is_tombstoned":true,"total_depth":null,"status_raw":null,...},
 "normalized_api_number":"15009111110000","current_status":"unknown", ...}
```

confirming the exported shape matches `contract::WellV1` exactly and that
`comments` is genuinely absent from `source_payload` (see architecture
doc item 70) — grepped for directly in the decompressed output, not just
asserted in a test.

**11 new integration tests** (`tests/state_export_v1.rs`), covering: a
well's geometry/identity/raw-status export and the hard "never export
`comments`" guarantee (checked via `privacy::assert_no_forbidden_fields`
too); a tombstoned well being both flagged in its own record and emitted
as a separate `TombstoneV1`; a multi-well operator transfer producing one
`regulatory_record` per confirmed link, a `fuzzy_candidate`-only link
correctly *not* counting as a confirmed match, and a zero-match transfer
still producing exactly one `unresolved` record; drilling-intent events
carrying a resolved well link only when actually matched; sentinel
production rows (`record_kind` other than `'monthly'`) never appearing in
`production_monthly`, and every production row being explicitly
`well_source_key = None` / `relationship_resolution_status =
"unresolved"` per the contract's own lease-level-production guidance;
fields and leases both exporting as `regulatory_areas` with distinct
`area_type`s; and the acknowledge/status round trip. All pass on a clean
fresh-database run alongside the full existing suite — **189 tests total**
(up from 172 before this task), `cargo clippy --all-targets -- -D
warnings` clean, release build succeeds.

## What this did NOT verify

- **A full-scale export against the complete real materialized dataset**
  (all real KGS/KCC data from KS-03 through KS-10, including the full
  11,678,706-row production table). The CLI smoke run above was against a
  database holding only this test suite's own fixture rows (187 records
  total across all entities) — enough to prove the binary, package
  format, and every entity's mapping genuinely work end to end, but not a
  timing/scale characterization the way KS-10's live-smoke doc has one for
  `snapshot-production`/`materialize-production`. Given the production
  table alone is ~11.7M rows and every row's `source_payload` adds
  overhead, a full real run's size and duration should be measured before
  this export is scheduled to run automatically against production data.
- **Any real `futureBackend` import.** This session did not run
  `futureBackend`'s own `state_import_v1` binary against a Kansas package,
  and does not have write access to a shared `futureBackend` database to
  do so safely. The package produced here was verified for internal
  self-consistency (`verify_package`, matching `futureBackend`'s own
  `package.rs`/`contract.rs` logic byte-for-byte) but not by actually
  importing it into a running `futureBackend` instance.
- **`futureBackend`'s own `state-sync-v1` orchestration**, end to end (the
  tool that would actually invoke this binary as a subprocess in
  production). The CLI's argument shapes and JSON envelope were matched
  against `state_sync_v1.rs`'s expectations by reading its source, not by
  running it against this exporter.

## When to re-run this

- Before scheduling automated exports in production, run a full-scale
  export against the real, fully materialized Kansas dataset and record
  its size/duration here (the same discipline KS-10 followed for its own
  full-scale run).
- If `futureBackend`'s `contract::v1::mod.rs` schema changes, re-diff it
  against this repo's `src/state_export/contract.rs` copy — they must
  stay byte-for-byte identical, since it is a shared protocol, not
  Kansas's own.
- Once someone with `futureBackend` write access is available, do a real
  end-to-end `state_import_v1` dry run against a package this exporter
  produced, to confirm the two sides agree in practice, not just on paper.
