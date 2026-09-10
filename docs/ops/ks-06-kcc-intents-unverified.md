# KS-06 — KCC Drilling-Intent Importer: Unverified-Against-Live-Service Notice

Date: 2026-09-05
Task: KS-06 (ingest daily KCC Notices of Intent to Drill)

## Why this doc exists

Same situation as KS-05's `docs/ops/ks-05-kcc-operators-unverified.md`:
`kcc.ks.gov` is unreachable from this development environment, confirmed
again at the start of this task (`curl`/`nslookup`/`WebFetch`, identical
`SERVFAIL`/`ENOTFOUND` result to every prior check). This importer is
built and tested only against a hand-built HTML results table modeled on
the task doc's own field list — not the real page.

## What is and isn't confirmed

**Confirmed** (structural):
- The `ks_ingestion_run` state machine, idempotency, and reject-on-failure
  discipline apply identically to this importer.
- `ks_drilling_intent` rows are permanent — nothing in this codebase ever
  deletes one, so the "retains records after they disappear from the
  365-day window" requirement holds regardless of what the source's own
  page shows on any given day.
- Surface-owner names, contacts, addresses, and phone numbers are never
  parsed: this importer only ever reads the search-result table's own
  cells and the document *link*, never the linked C-1/KSONA document's
  contents.
- The event dedup mechanism (`ks_event`, unique on `(event_type,
  dedup_key)`) and the well-linking tier order are exercised end-to-end
  by `tests/kcc_intents_materialize.rs`.

**Not confirmed** (depends entirely on the real page, never fetched):
- **The page's actual markup.** `source::html_table::parse_first_table`
  assumes a single `<table>` with a header row — if the real page is a
  JavaScript-rendered results grid (common for modern search UIs) rather
  than server-rendered HTML, a plain HTTP GET won't see any rows at all,
  and every run will hit the zero-result safeguard and reject. This would
  be visible immediately in `ks_ingestion_run.failure_message`.
- **Whether the source exposes any identifier at all beyond a document
  filename.** The task doc's "Parse:" field list has no permit/intent
  number column — `source_filename` is used as the natural identity key,
  and match tier 1 ("exact KCC permit or API") tries to extract an
  API-14 or permit token from that filename. If real filenames don't
  encode either, tier 1 will simply never fire, which is a silent
  degradation (everything falls through to tier 2+) rather than a loud
  failure — worth specifically checking once the page is reachable.
- **`reported_lease_or_well_name`** — not in the task doc's literal field
  list, added because match tier 2 requires it. If the real page has no
  such column, tier 2 (and the fuzzy tier 3) can never fire either.
- **Column header text** — `parse::kcc_intents::table_source` matches
  several plausible header spellings per field; a header outside that set
  fails silently (the field stays `None`), not loudly.
- **`published_at`** — always `NULL` today. The task doc asks to "keep
  occurred_at, published_at when available"; whether the source exposes a
  distinct posted-timestamp separate from the permit date has never been
  checked.

## How to verify this for real

1. From an environment that can reach `kcc.ks.gov`, fetch
   `https://www.kcc.ks.gov/notices-of-intents-search` and view its
   rendered HTML source (not just the initial response, if the page is
   JS-rendered — check with browser dev tools whether results appear in
   the raw HTTP response body at all).
2. Compare the real table's header names against
   `src/parse/kcc_intents/table_source.rs`'s `*_HEADERS` constants; add
   any real spelling that's missing.
3. Check whether any column carries a permit/intent number distinct from
   the document filename; if so, add it as its own field and prefer it
   over the filename-token heuristic in
   `materialize::kcc_intents::resolve_well`'s tier 1.
4. Run `cargo run --bin ks_ingest -- snapshot-intents` for real and
   inspect the manifest and `lifecycle`-style counts against what you
   already know about that day's real intent postings.
5. Confirm the actual publication time against the stated "around 6 p.m.
   Central" — adjust `deploy/systemd/user/future-wells-ks-ingest-intents.timer`'s
   `OnCalendar` if it's meaningfully different.
6. Once verified, replace this doc with a live-smoke-check doc in the
   style of `docs/ops/ks-03-kgs-wells-live-smoke.md`.

## What the automated tests do cover

`tests/kcc_intents_import.rs` (5 tests) and
`tests/kcc_intents_materialize.rs` (6 tests) exercise the full pipeline
against a hand-built HTML table: table parsing, the zero-result and
partial-result safeguards, idempotent checksums, all four well-linking
tiers (exact identifier, license+PLSS+name, probabilistic with a recorded
confidence, unresolved), and event deduplication across re-materialization.
They prove the code does what it's designed to do against input shaped
the way this document assumes KCC's real page is shaped — they cannot
prove that assumption itself.
