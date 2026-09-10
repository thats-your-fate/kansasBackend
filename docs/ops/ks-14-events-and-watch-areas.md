# KS-14 — Kansas Activity Events and Watch-Area Alerts

Date: 2026-09-05
Task: KS-14 (generate Kansas activity events and watch-area alerts)

## Scope

This is a `kansasBackend`-internal task with no cross-repo dependency —
unlike KS-11/12/13, nothing here requires `futureBackend`/`futureMarketing`
access. Delivery (actually notifying a user — email/SMS/webhook) is out
of scope: this repo has no account/notification system yet (an
explicitly-deferred item since KS-001). What KS-14 actually builds is the
event generation and watch-area *matching* — the mechanism a future
notification system would consume.

## Event types

All 10 required types are implemented as real emitters, plus the 3
reserved-but-not-yet-emitted types the task doc asks for:

| Event type | Emitted by | Notes |
|---|---|---|
| `INTENT_POSTED` | `materialize::kcc_intents` | Existing (KS-06), extended with backfill suppression + relationship columns |
| `INTENT_EXPIRED` | `materialize::kcc_intents::expire_stale_intents` | New — see below |
| `SPUD_RECORDED` | `materialize::kgs_wells` | New — fires on a well that already existed, when `spud_date` is newly set or corrected |
| `COMPLETION_RECORDED` | `materialize::kgs_wells` | Same pattern, `completion_date` |
| `PLUGGING_RECORDED` | `materialize::kgs_wells` | Same pattern, `plug_date` |
| `WELL_STATUS_CHANGED` | `materialize::kgs_wells` | Fires when `lifecycle_status` differs from the prior materialize |
| `OPERATOR_TRANSFER_APPROVED` | `materialize::kcc_transfers` | Existing (KS-07), extended the same way |
| `FIRST_LEASE_PRODUCTION_REPORTED` | `materialize::kgs_production` | New — the very first monthly row ever for a (lease_kid, product) |
| `LEASE_PRODUCTION_UPDATED` | `materialize::kgs_production` | New — every later month, or a correction to an existing one |
| `DOCUMENT_ADDED` | reserved for KS-15 | Constant exists (`event::DOCUMENT_ADDED`), no emitter yet — KS-15 builds the document index this depends on |
| `CONSERVATION_DOCKET_FILED` | reserved | Constant exists, valid per the DB CHECK constraint, no emitter (no conservation-docket importer exists) |
| `CONSERVATION_ORDER_ISSUED` | reserved | Same |
| `UIC_REPORT_UPDATED` | reserved | Same |

None of the four wells events (`SPUD_RECORDED`/`COMPLETION_RECORDED`/
`PLUGGING_RECORDED`/`WELL_STATUS_CHANGED`) can fire on a well's true
first-ever materialize — there is no "prior" state to transition from —
which structurally rules out a "180,000 new-well events on day one" flood
independent of the backfill flag.

## `occurred_at` / `published_at` / `observed_at`

Populated per the task doc's three-way distinction wherever the source
supports it:
- `occurred_at`: the agency/effective date when known (a well's own
  spud/completion/plug date; a production month's first day; an
  intent's permit date; a transfer's effective/approved date). `None`
  when genuinely unknown (e.g. `WELL_STATUS_CHANGED` and
  `INTENT_EXPIRED` — KGS/KCC report neither an exact status-change date
  nor a permit-expiration date).
- `published_at`: left `None` throughout — no source confirmed to expose
  a distinct publication date separate from its own effective date (same
  finding KS-06/KS-07 already made).
- `observed_at`: always `Utc::now()` at materialize time — when this
  platform actually recorded the fact, regardless of the source's own
  dates.

## Baseline-event suppression

`constants::should_emit_event(is_baseline_backfill, occurred_at, cutoff,
force_historical)` is the single decision function every emitter calls.
`ks_ingestion_run.is_baseline_backfill` (migration `0022`) is a per-run
flag an operator sets explicitly via `ks_ingest mark-run-backfill
<run_id>` immediately after a snapshot completes and before running the
matching `materialize-*` command; `force_historical_events` (same
migration) is the "deliberate historical timeline job" override, set via
`ks_ingest mark-run-historical <run_id>`.

`LIVE_MONITORING_CUTOFF` (`constants.rs`) is a **fixed constant**
(`2026-09-05T00:00:00Z`, the date this capability shipped), not
env-configurable. That was a deliberate choice, not an oversight: an
env-var cutoff would be process-global mutable state, unsafe to touch
from a `#[tokio::test]` running concurrently with the rest of this
repo's shared-database test suite. A fixed constant keeps
`should_emit_event` itself fully unit-testable.

**A real bug found only by integration-testing this, not by reasoning
about it**: the first implementation let a backfill run's suppression
check fall back to `observed_at` when `occurred_at` was unknown (e.g.
`WELL_STATUS_CHANGED`). Since `observed_at` during a backfill is always
"whenever the backfill happened to run" — i.e. always "now," always at
or after any sane cutoff — that fallback silently defeated suppression
for exactly the event type with no source-reported date, the one most
likely to matter in a real backfill. Caught by
`baseline_backfill_run_suppresses_historical_transition_events` in
`tests/kgs_wells_materialize.rs`. Fixed: when `occurred_at` is unknown
during a backfill, the safe default is to suppress, not to guess recency
from processing time — `should_emit_event` no longer takes `observed_at`
as a parameter at all.

## Revision-safe dedupe and late-arriving records

Every dedup key embeds the observed *value*, not just an identifier —
e.g. a well-status event's key is `well_id:old_status:new_status`; a
production event's key is `lease_kid:product:year:month:volume`. Because
`ks_event` dedupes on `(event_type, dedup_key)`, re-observing an
unchanged fact is silently absorbed, while a genuine correction (a
different date, status, or volume observed later) produces a new event.
Since every date used for `occurred_at` comes from the source record
itself rather than "now," a record arriving late (e.g. a delayed
snapshot correction) still gets the *correct* historical `occurred_at`,
not today's date — "late-arriving source records supported" falls out of
that design rather than needing special-case handling.

**A second real bug found only by testing, not reasoning**: the first
production-event implementation classified `FIRST_LEASE_PRODUCTION_REPORTED`
vs. `LEASE_PRODUCTION_UPDATED` from the per-row `RETURNING (xmax = 0)`
insert flag. A plain re-observation of an unchanged value is always an
`UPDATE` at the SQL level (the row already exists), which flipped its
event_type from `FIRST_LEASE_PRODUCTION_REPORTED` (the original
classification) to `LEASE_PRODUCTION_UPDATED` on replay — and because
`(event_type, dedup_key)` uniqueness is scoped *per event_type*, the
identical dedup_key under a new event_type inserted a spurious second
notification for a value that never changed. Fixed by resolving
"has this exact fact already been reported, under either event type"
*before* ever deciding which type to assign — see
`materialize::kgs_production`'s own doc comment for the corrected
design. Caught by `revision_safe_dedupe_for_a_production_correction` in
`tests/kgs_production_materialize.rs`.

## Watch areas

`ks_watch_area` (migration `0026`) supports `county`/`operator`/`field`/
`well`/`radius` watches. No account system exists yet, so a watch area's
`owner_reference` is a free-text placeholder, not a real foreign key —
documented in the migration itself. `src/watch.rs` provides:
- Pure, directly unit-tested matching predicates (`event_matches_watch_area`,
  `distance_miles` for the haversine radius check, `county_matches`) —
  the task doc's "radius/county/operator watch tests" acceptance
  criterion.
- `event_watch_context`/`find_matching_watch_areas`, which build an
  event's watchable context from `ks_event`'s own typed relationship
  columns (`related_well_id`/`related_operator_id`/`related_field_id`/
  `related_county`) plus `ks_event_well_link` for multi-well events (a
  transfer can link several wells at once), and resolve well locations
  for radius matching from `ks_well.display_geometry_4326`.

## Every event links back to its source

Every `ks_event` row carries `source_ingestion_run_id` (a real FK to
`ks_ingestion_run`), satisfying "every event links back to its official
source or source snapshot" structurally — confirmed by
`every_emitted_event_links_back_to_its_source_ingestion_run` in
`tests/kgs_wells_materialize.rs`.

## A real, pre-existing bug this task exposed in KS-12/13's exporter

`state_export::exporter::export_events` (built at KS-12/13, before any
event type besides `INTENT_POSTED`/`OPERATOR_TRANSFER_APPROVED` existed)
joined `ks_drilling_intent` via `e.event_type = 'INTENT_POSTED' AND
di.id = e.dedup_key::uuid`. Once KS-14 introduced event types with
non-UUID dedup keys (e.g. `"lease_kid:product:2020:3:100"`), a real
integration test crashed the whole export: Postgres's planner is free to
evaluate a hash join keyed on `e.dedup_key::uuid` for *every* row of
`ks_event` before the `event_type` predicate ever applies, so the cast
failed on the first non-`INTENT_POSTED` row it hashed. Fixed two ways:
(1) `export_events` was simplified to read `ks_event`'s own typed
`related_well_id`/`related_operator_id`/`related_field_id` columns
directly (now that KS-14 added them) instead of deriving a well link via
a fragile join, which incidentally also eliminated the unsafe cast
entirely; (2) documented the general lesson (a multi-predicate join
condition does not guarantee left-to-right short-circuit evaluation the
way a `CASE WHEN` expression does) in case a similar pattern is ever
needed elsewhere.

## Verification

- `cargo test --all-targets`: **217 tests** (up from 189 after KS-12/13),
  0 failures, across three repeated clean fresh-DB runs.
- New test files: `tests/ks_watch_area.rs` (4 tests, DB-backed watch
  matching). Extended `tests/kgs_wells_materialize.rs` (+6),
  `tests/kgs_production_materialize.rs` (+3), `tests/kcc_intents_materialize.rs`
  (+2, and the whole file gained `#[serial]` — see below). Unit tests for
  `should_emit_event` (`src/constants.rs`, 5) and `src/watch.rs` (9).
- `cargo clippy --all-targets -- -D warnings` clean; `cargo build
  --release` succeeds.
- Real CLI smoke test against a live local Postgres database (migrated
  via `sqlx migrate run`, not just `cargo test`'s own migration-on-connect):
  `ks_ingest mark-run-backfill <id>` and `ks_ingest mark-run-historical
  <id>` against a real inserted run row, confirmed via a direct query
  that both flags were set.

**A real test-isolation gap found and fixed along the way**: adding
`expire_stale_intents`'s global scan (mirroring `ks_well`'s own
tombstone-scan design, which already required `#[serial]` in
`tests/kgs_wells_materialize.rs`) to `materialize_intents_from_run` gave
`tests/kcc_intents_materialize.rs` the identical hazard — confirmed
empirically (other tests' still-unresolved intents got expired mid-run,
and this file's own fixtures were sometimes expired a full run early)
before `#[serial]` (listed before `#[tokio::test]`, per the established
convention) was added to every test in that file.

## What this did NOT verify

- Real delivery/notification (no transport exists).
- A real KGS/KCC live run exercising these new event paths end-to-end at
  scale (KS-10's production materializer already has a real 11.68M-row
  verified run from before this task; re-running it with the new event
  logic active, with `is_baseline_backfill` set, to confirm zero events
  are emitted at that scale, was not done in this session — the unit and
  integration tests cover the same logic at fixture scale).
- The narrow, documented same-page multi-month edge case in
  `materialize::kgs_production` (see that module's own doc comment) at
  real scale.
