# KS-12/KS-13 — Shared Infrastructure Handoff (`futureBackend` / `futureMarketing`)

Date: 2026-09-05
Task: KS-12 (Kansas map layer + clustering pipeline), KS-13 (Kansas well/lease/operator/field/county details)

## Purpose

Both tickets ask to *extend* the shared Future Wells Co map and
entity-detail platform, not build a Kansas-specific one. This session was
explicitly granted access to `futureBackend` and `futureMarketing` to
investigate what already exists there before writing any code, and the
user then explicitly scoped this session to the `kansasBackend`-side
piece only (the `state-export-v1` exporter — see
`docs/ops/ks-12-13-state-export-live-smoke.md`). This document is the
precise, itemized handoff of what a future session with `futureBackend`/
`futureMarketing` as its primary scope needs to do to actually finish
KS-12/KS-13 — every item below was found by real investigation (reading
the actual source in both repos), not guessed at.

## What already exists (do not rebuild)

- `futureBackend`: a real, working, zoom-tiered server-side clustering map
  API (`GET /user-area/map/wells`, `GET /user-area/map/wellbores`) that
  already never serializes a full state's well table (bounded per-zoom
  point/grid/state-cluster responses with a `truncated` flag), already
  exposes horizontal/directional booleans per well, and already curates
  its point-layer popup fields to exclude raw source data. Generic
  well/operator/county/regulatory-area detail routes already exist and
  are state-agnostic.
- `futureMarketing`: a real, working multi-state map component
  (`components/user-area/map-workspace.tsx`, MapLibre GL, already
  consuming server-side clusters correctly) and generic entity-detail
  pages (`live-well-detail-overlay.tsx`, `live-entities-workspace.tsx`)
  already serving Colorado/Texas/New Mexico/Oklahoma/North Dakota. Kansas
  is already stubbed into `lib/user-area/capabilities.ts` (all
  capabilities `"planned"`) and `lib/user-area/domain.ts`'s `StateCode`
  type already includes `"KS"`.
- Both repos already know how to receive Kansas data at all: `kansasBackend`
  is already registered in `futureBackend` as a `state-export-v1` data
  source (`data_sources.source_code = 'kansas_backend'`), and
  `jurisdictions.rollout_stage = 'flagged'` for Kansas (from an earlier
  KS-01 migration in `futureBackend`, confirmed by
  `futureBackend/tests/kansas_state_import_source.rs`).
- `kansasBackend` now has a real, tested `state-export-v1` exporter
  (`src/state_export/`, this task) that produces a verifiable package —
  see the live-smoke doc for what was actually run and confirmed.

## What is genuinely missing — concrete, itemized gaps found during this task

### 1. No field or county boundary geometry anywhere in the shared schema

`futureBackend`'s `counties` table has no geometry column at all, and
"field" is not a modeled entity there — it is free text pulled out of
`wells.source_payload->>'field_name'`, used only for text-match filtering.
KS-12 asks for "county boundaries" and "field overlays" as map layers;
neither can be built today without first adding boundary-geometry columns
(or a separate cadastral/tile layer, matching the pattern
`futureMarketing`'s `cadastralConfigs` already uses for NM/TX/OK/CO/ND
county/PLSS overlays) to `futureBackend`'s schema, plus a `KS` entry in
that `CadastralState` config on the `futureMarketing` side.

**What `kansasBackend` already has ready to feed this**: `ks_field.
display_geometry` (a real, validated `MultiPolygon` in EPSG:4326, with
`geometry_is_valid`/`geometry_validity_reason` already computed) — this
export currently only forwards it as raw GeoJSON inside
`RegulatoryAreaV1.source_payload.source_geometry_geojson` (see
architecture doc item 71) because `RegulatoryAreaV1` has no typed geometry
field. `kansasBackend` does **not** have county boundary geometry of its
own — that would need to come from KGS's county layer or a shared
reference package (the repo notes `futurewells_county_reference_v1/`
already exists as an example of the latter for another state).

### 2. `ProductionMonthlyV1` has no lease/regulatory-area link

The contract's typed production record only carries `well_source_key`
and `operator_source_key` — there is no `lease_source_key` or generic
`regulatory_area_source_key`. Kansas lease production is lease-level, not
allocated to an individual well (correctly modeled per the contract's own
"Unresolved Relationships" guidance: `well_source_key = None`,
`relationship_resolution_status = "unresolved"`), but there is nowhere
typed to record *which lease*. This export stuffs `lease_kid` and related
fields into `source_payload` as a stopgap.

**Concrete ask**: add a `regulatory_area_source_key` (or
`lease_source_key`) field to `contract::ProductionMonthlyV1` on the
`futureBackend` side, and update the importer to resolve it the same way
`well_source_key` is resolved today. Without this, KS-13's lease detail
page ("show lease-level monthly production and all linked wells") cannot
be served by the shared platform from this data alone — it would need a
custom query joining on `source_payload->>'lease_kid'`, which defeats the
purpose of a typed contract.

### 3. No cache layer anywhere in `futureBackend`

Confirmed by grep across `futureBackend/src` — no Redis, no in-process
LRU/TTL cache, no `Cache-Control` response headers on any map or detail
route. KS-12 explicitly asks for a "tile/query cache keyed by state and
filters." This is real, net-new infrastructure work in `futureBackend`,
not a Kansas-specific gap — every state's map/detail traffic currently
hits Postgres on every request.

### 4. Kansas is not yet turned on anywhere in the shared UI

- `futureMarketing/lib/user-area/capabilities.ts`: Kansas's row has every
  capability set to `"planned"` — needs to flip to real levels once data
  is actually flowing (i.e., after a `state-export-v1` import has run).
- `futureMarketing`'s map state dropdown (`states` array in
  `map-workspace.tsx`) does not list Kansas at all yet — needs one entry.
- The map's filter panel only wires up State + Well type today, even
  though the underlying `LiveWellFilters` API contract already supports
  `operatorId`, `status`, `eventFrom`/`eventTo`, etc. — KS-12's
  operator/status/type/date filter requirement is mostly a *panel UI*
  task, not a new API contract.
- The map popup is missing an explicit field name and a source-freshness
  indicator (both exist in the fuller `WellProfileResponse`/
  `WellInspector` already, just not surfaced in the lightweight map
  popup) — a small, well-scoped addition, not new plumbing.

### 5. `futureBackend`'s `BACKEND_URL` gateway question

`futureMarketing` proxies all `/api/v1/user-area/...` calls to a single
configured `BACKEND_URL` — there is no per-state backend URL on the
frontend. Whoever owns that gateway/backend needs to confirm how a
`state-export-v1` import of Kansas data actually lands in the database
that `BACKEND_URL` serves (this may already be solved by the shared
Postgres schema architecture — `futureBackend` owns one schema, keyed by
`jurisdiction_id`/`state_code`, and every state's imported rows already
live in the same tables — but it is worth an explicit confirmation before
relying on it, since this session did not have write access to verify it
directly).

## Suggested order of operations for a future session

1. Run a real `state-export-v1` full export from `kansasBackend` (this
   repo, already built) and import it into a `futureBackend` dev/staging
   database with `futureBackend`'s own `state_import_v1` binary — the one
   thing this session could not do (no `futureBackend` database write
   access) and the most important thing to confirm before anything else.
2. Add the `ProductionMonthlyV1.regulatory_area_source_key` (or
   equivalent) contract field and re-export/re-import once it exists —
   otherwise lease-level production can only be joined on
   `source_payload`, an unstable basis to build a detail page against.
3. Flip Kansas's `capabilities.ts` row and add it to the map's state
   dropdown once #1 is confirmed — this alone should make wells,
   clusters, and generic entity details work for Kansas through the
   existing shared UI with zero further `futureMarketing` code changes
   beyond that.
4. Add the operator/status/date filter UI (contract already supports it)
   and the two missing map-popup fields (field name, source freshness) —
   small, scoped `futureMarketing` UI tasks.
5. Field/county boundary geometry and the cache layer are the two
   largest remaining pieces of real, novel shared-infrastructure work —
   both affect every state, not just Kansas, and are reasonably deferred
   as their own tickets rather than bundled into a "finish Kansas" pass.
