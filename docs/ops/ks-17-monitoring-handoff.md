# KS-17 — Kansas Freshness, QA, and Source-Health Monitoring: Handoff

Date: 2026-09-06
Task: KS-17 (Kansas freshness, QA and source-health monitoring)

## Scope of this session

The task doc asks for source-specific health checks, a recommended
per-source cadence table, a Kansas data-coverage panel "inside Future
Wells Co" (`futureBackend`/`futureMarketing`), a simplified public
coverage page on the regional marketing site (`kansasMarketing`), and two
hard publication gates. Consistent with KS-12/KS-13's precedent, this
session was scoped to `kansasBackend` only — the module that computes and
exposes every one of these checks against this repo's own tables. The two
cross-repo UI pieces (the internal panel, the public page) and "alerts
route through existing monitoring" are **not** built here; this document
is the itemized handoff for whoever builds those next.

## What was built in `kansasBackend`

- `src/monitoring.rs` — the whole KS-17 surface: `source_freshness_report`
  (stale vs. failed as independent states), `row_count_change_report`,
  `schema_fingerprint_change_report`, `content_type_change_report` (all
  three compare the two most recent completed runs per source and — after
  a real bug fix, see below — correctly include a source with zero run
  history rather than silently omitting it), `rejected_run_anomalies_report`,
  `well_data_quality_report` (duplicate/invalid API, missing geometry,
  out-of-state exclusions, unknown status combinations),
  `link_confidence_report` (operator/intent/transfer/lease match-method
  distributions, including "unresolved" leases), `production_reconciliation_out_of_tolerance_count`
  (reuses KS-10's own reconciliation query), and the two hard gates:
  `restricted_year_violations` and `personal_field_leakage_report`,
  combined into `publication_gate`.
- `ks_ingest health-report` — prints every report above. Always exits 0;
  it is a human/log-scrapable summary, not a gate.
- `ks_ingest publication-gate-check` — runs `publication_gate` and exits
  non-zero (with `eprintln!` detail lines) the moment either hard rule is
  violated: **"any production row before the permitted year should fail
  publication"** (`restricted_year_violations`, a standing re-check
  against the canonical `ks_lease_production_month` table, independent of
  KS-10's ingest-time gate) and **"any newly detected contact/address
  field should fail indexing until explicitly classified"**
  (`personal_field_leakage_report`, scans every free-text column this repo
  actually exports/indexes for phone-number-shaped text using KS-15's own
  redaction detector — deliberately excludes `ks_well.comments`, which is
  already excluded from every export, so it is a known field, not a
  "newly detected" one).
- `deploy/systemd/user/future-wells-ks-health-check.{service,timer}` —
  runs `health-report` then `publication-gate-check` once daily at 22:00
  America/Chicago (after both existing KCC daily jobs), mirroring the
  existing `future-wells-ks-ingest-{intents,transfers}` timer pattern. A
  failed unit run *is* the alert for a publication-gate violation today —
  there is no other alerting wired up (see "What this does not do" below).
- `tests/ks_monitoring.rs` (9 tests) plus a pure unit test in
  `src/monitoring.rs` itself for the cadence-to-staleness mapping.

## A real bug found and fixed during this task

Three of the report queries (`row_count_change_report`,
`schema_fingerprint_change_report`, `content_type_change_report`) were
originally built `FROM ks_ingestion_run JOIN ks_source` — the "fact
table" — rather than `FROM ks_source LEFT JOIN ks_ingestion_run` — the
"dimension table." A source with zero completed runs ever (e.g. one
whose importer doesn't exist yet) was silently absent from the report
entirely instead of appearing with `None` fields. This is exactly the
"no data at all" anomaly a coverage panel most needs to surface, so
silently hiding it defeated the purpose. Caught by
`schema_fingerprint_change_is_none_when_unknown_and_some_when_known`
failing against a genuinely zero-history source; fixed in all three
functions and covered by all 9 `ks_monitoring` tests, which now pass.

A second, independent bug was found and fixed while writing this
document: `staleness_threshold_hours` matched `expected_cadence` by exact
string (`"nightly"`, `"daily"`, `"weekly"`, `"monthly"`), but
`ks_source`'s real seeded values (migration `0002`) include
`daily_rolling_365`, `daily_rolling_90`, and `continuous` — none of which
equal `"daily"` as a literal string. `KCC_INTENTS`, `KCC_TRANSFERS`, and
`KCC_OPERATORS` therefore never had a staleness threshold at all and
would never report stale no matter how out of date they became. Fixed by
matching on prefix (`starts_with("daily")`/`starts_with("nightly")`) plus
an explicit `"continuous"` case; covered by a new pure unit test,
`monitoring::tests::every_seeded_cadence_value_resolves_as_expected`.

## Recommended per-source cadence table

This is the real `expected_cadence` value already seeded per source in
`ks_source` (migration `0002`), which is what `source_freshness_report`
actually evaluates against today — not a separate table that needs to be
kept in sync by hand.

| Source | Agency | `expected_cadence` | Staleness threshold |
|---|---|---|---|
| `KGS_WELLS_ARCGIS` | KGS | `nightly` | 30h |
| `KGS_WELLS_ZIP` | KGS | `nightly` | 30h |
| `KGS_FIELDS` | KGS | `irregular` | none (no source-grounded expectation) |
| `KGS_LEASES` | KGS | `monthly` | 960h |
| `KGS_PRODUCTION_OIL` | KGS | `monthly` | 960h |
| `KGS_PRODUCTION_GAS` | KGS | `monthly` | 960h |
| `KGS_DOCUMENTS` | KGS | `irregular` | none |
| `KCC_OPERATORS` | KCC | `continuous` | 30h |
| `KCC_INTENTS` | KCC | `daily_rolling_365` | 30h |
| `KCC_TRANSFERS` | KCC | `daily_rolling_90` | 30h |
| `KCC_CONSERVATION_DOCKETS` | KCC | `daily` | 30h (no importer built yet — always reports stale, correctly) |
| `KCC_KOLAR_DOCUMENTS` | KCC | `irregular` | none (no importer built yet) |

`irregular` intentionally never reports stale — KGS publishes field and
document updates on no fixed schedule, and inventing a threshold would be
a false precision this repo has avoided everywhere else (see
`docs/architecture/kansas-source-boundary.md`).

## What the deferred cross-repo panel/page need from this module

Everything both the internal Future Wells Co panel and the public
`kansasMarketing` coverage page would show is already real, queryable
data — no new `kansasBackend` work should be needed to build either, only
a way to reach this data from outside the process:

1. **Internal panel** (`futureBackend`/`futureMarketing`): the natural
   shape is a thin HTTP endpoint in `future_wells_ks_api` (the existing
   Axum binary, currently just `/health` per `tests/health.rs`) that
   calls `monitoring::source_freshness_report` and
   `monitoring::publication_gate` and returns them as JSON, which
   `futureBackend` polls or which `futureMarketing` calls directly if
   Kansas's API is reachable from there. No such endpoint exists yet —
   today the only way to get this data is the `ks_ingest health-report`
   CLI against a direct database connection.
2. **Public coverage page** (`kansasMarketing`): needs a *subset* of the
   above — source name, last successful import date, and cadence are
   safe to publish; row counts, schema fingerprints, link-confidence
   breakdowns, and anything from `well_data_quality_report` are internal
   QA detail, not public-facing. A future session building this page
   should add a second, deliberately narrower endpoint (or a query
   parameter) rather than exposing the full `health-report` payload
   publicly.
3. **Alerting** ("alerts route through existing monitoring", per the task
   doc): this repo has no visibility into what "existing monitoring"
   means outside `kansasBackend` — same reasoning KS-12/13 already
   established for cross-repo UI work. Today, `future-wells-ks-health-check.service`
   failing (a non-zero `publication-gate-check` exit) is visible only via
   `journalctl`/systemd unit status on the host it runs on. Wiring that
   into a real alerting channel (Slack, PagerDuty, email) is entirely a
   deployment/ops decision outside this repo's scope, not a code gap.

## The task doc's 16-item monitor list, checked against what was built

| Item | Status |
|---|---|
| Last successful import per source | ✓ `source_freshness_report.last_successful_import` |
| Source freshness / staleness | ✓ `SourceFreshness::is_stale` |
| Most-recent-run failure, independent of staleness | ✓ `SourceFreshness::is_failed` |
| Row-count change vs. previous run | ✓ `row_count_change_report` |
| Duplicate API values | ✓ `well_data_quality_report` (raw-snapshot scoped — the canonical table has a real unique index, so this can never manifest there) |
| Invalid API count | ✓ `well_data_quality_report` |
| Missing geometry count | ✓ `well_data_quality_report` |
| Out-of-state exclusions | ✓ `well_data_quality_report` |
| Unknown status combinations | ✓ `well_data_quality_report` |
| Operator-link confidence | ✓ `link_confidence_report` |
| Lease-link confidence | ✓ `link_confidence_report` (includes "unresolved" bucket) |
| Monthly production reconciliation | ✓ `production_reconciliation_out_of_tolerance_count` |
| Schema fingerprint changes | ✓ `schema_fingerprint_change_report` (only populated for sources that record a fingerprint at ingest time — most don't yet) |
| Zero-result/anomalous-run detection | ✓ `rejected_run_anomalies_report` (counts `rejected`-status runs; this repo's existing zero-result/partial-result safeguards all reject via that status) |
| Source HTTP/content-type changes | ✓ `content_type_change_report` (only populated for `KGS_DOCUMENTS` so far — the only importer wired to `ingestion_run::record_response_content_type`; every other source reports `None` on both sides, correctly treated as "unknown," not "unchanged") |
| Restricted-year / personal-field hard gates | ✓ `publication_gate`, enforced via `ks_ingest publication-gate-check`'s exit code |

**One documented gap**: the task doc's phrasing "latest source coverage
date" (as distinct from "last successful import") most likely means *the
most recent date of data the source itself covers* (e.g., the newest
production month actually present, not when the import ran) — this is
not separately tracked per source category today.
`source_freshness_report` only reports *import* recency. A future
session wanting this would need a per-source-category query (e.g. `max(year,
month)` from `ks_lease_production_month` for the production sources,
`max(occurred_at)` from `ks_event` for activity-derived sources) — there
is no single generic column to read this from across all twelve sources,
since "coverage date" means something different for a well-master source
than a production-archive source.
