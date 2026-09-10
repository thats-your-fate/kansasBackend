# KS-07 — KCC Operator-Transfer Importer: Unverified-Against-Live-Service Notice

Date: 2026-09-05
Task: KS-07 (ingest approved KCC operator transfers)

## Why this doc exists

Same `kcc.ks.gov` unreachability as KS-05/KS-06 (confirmed again at the
start of this task). This importer is built and tested only against a
hand-built HTML results table — not the real
`config::SourceUrls::kcc_transfer_results` endpoint, which
`src/config.rs` already documents as "form-driven."

## What is and isn't confirmed

**Confirmed** (structural):
- `ks_operator_transfer` rows are immutable and permanent (only
  `last_observed_at`/`source_ingestion_run_id` are ever updated on
  re-import) — satisfies "transfer history remains after source
  expiration" regardless of what the source's 90-day window shows later.
- `received_date`/`effective_date`/`approved_date` are three distinct
  nullable columns, never collapsed into one.
- One transfer can link to multiple wells: `ks_operator_transfer_well` is
  a join table, and `resolve_wells_for_transfer` runs every exact tier
  (not just the first hit), tested explicitly in
  `tests/kcc_transfers_materialize.rs::a_transfer_can_link_to_multiple_distinct_wells_from_different_tiers`.
- Fuzzy lease-name candidates are only ever generated when every exact
  tier found nothing, are tagged `match_method = 'fuzzy_candidate'`, and
  nothing in this codebase reads that tag to justify writing to `ks_well`
  or any operator-history table (`ks_well_operator_history` doesn't exist
  yet — that join is KS-08).
- The idempotent-import identity (`past_operator_license`,
  `new_operator_license`, `effective_date`, `lease_or_well_label`,
  `county`) uses a `COALESCE`-wrapped unique index, not a plain `UNIQUE`
  constraint — a real bug was found and fixed during this task's own
  testing: Postgres treats `NULL` as distinct from `NULL` in a plain
  unique constraint, so a transfer missing even one identity field (e.g.
  no county) would never conflict on re-import and would duplicate every
  day instead of upserting. See migration `0013_ks_operator_transfer.sql`'s
  comment and `docs/architecture/kansas-source-boundary.md` item 38.

**Not confirmed** (depends entirely on the real page, never fetched):
- **Whether a plain GET against `kcc_transfer_results` with no query
  parameters actually returns the 90-day rolling window**, or whether the
  "form-driven" search requires specific POST/query parameters to return
  anything at all. If it requires parameters this importer doesn't send,
  every run will hit the zero-result safeguard.
- **Whether the page is server-rendered HTML at all** (same
  JavaScript-rendering risk as KS-06).
- **The real column names** — `src/parse/kcc_transfers/table_source.rs`'s
  header-matching is a guess across several plausible spellings.
- **The "KCC/DOR identifiers" match tier's actual field.** The task doc
  lists this as the first exact-match strategy, but the "Capture:" field
  list has no separate identifier column — this importer tries the
  lease/well label itself as a candidate identifier token, which may
  never fire if real labels don't embed one. Same caveat as KS-06's tier 1.
- **Which operator (past or new) is the more useful cross-reference for
  the "exact operator license" tier.** This importer uses the *new*
  operator's license (the one taking over) — a defensible but unconfirmed
  choice; a real DOR/well-to-license join (KS-08) would make this tier
  far more reliable than the current name-based cross-reference through
  `ks_operator`.

## How to verify this for real

1. From an environment that can reach `kcc.ks.gov`, load
   `https://www.kcc.ks.gov/notices-of-transfers-approved-search`,
   actually submit its search form, and inspect whether the resulting
   `transfersSearch.php` URL/response carries query parameters this
   importer would need to replicate.
2. Compare the real results table's headers against
   `src/parse/kcc_transfers/table_source.rs`'s `*_HEADERS` constants.
3. Run `cargo run --bin ks_ingest -- snapshot-transfers` for real and
   inspect the manifest against known recent approved transfers.
4. Once verified, replace this doc with a live-smoke-check doc in the
   style of `docs/ops/ks-03-kgs-wells-live-smoke.md`.

## What the automated tests do cover

`tests/kcc_transfers_import.rs` (4 tests) and
`tests/kcc_transfers_materialize.rs` (9 tests) exercise the full pipeline
against a hand-built HTML table: table parsing, the zero-result and
partial-result safeguards, idempotent re-import (including the NULL
identity-field bug above), every well-linking tier individually, a
multi-well link from two different tiers in one transfer, the
fuzzy-candidate-only-as-a-fallback rule, the unmatched-transfers report,
and event deduplication. They prove the code does what it's designed to
do against input shaped the way this document assumes KCC's real page is
shaped — they cannot prove that assumption itself.
