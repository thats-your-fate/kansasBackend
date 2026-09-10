# KS-05 — KCC Operator Importer: Unverified-Against-Live-Service Notice

Date: 2026-09-05
Task: KS-05 (import and normalize KCC operator licenses)

## Why this doc exists, and why it isn't a live-smoke-check doc like KS-03/KS-04's

`docs/ops/ks-03-kgs-wells-live-smoke.md` and
`docs/ops/ks-04-materialize-wells-live-smoke.md` both record a real,
manual run against the live KGS service. KS-05 has no equivalent: `kcc.ks.gov`
is unreachable from this development environment. This was checked four
separate times across two sessions, by three independent methods, with the
same result every time:

1. A research agent's `WebFetch` attempt during the original KS-05 work: DNS `ENOTFOUND`.
2. Direct `curl -sI https://www.kcc.ks.gov/...`: exit code 6, "couldn't resolve host."
3. Direct `nslookup kcc.ks.gov`: `SERVFAIL` from the local resolver (`127.0.0.53`).
4. Re-checked again at the start of this session, both via `curl`/`nslookup`
   and directly via the `WebFetch` tool: identical `SERVFAIL`/`ENOTFOUND`
   result (`getaddrinfo ENOTFOUND www.kcc.ks.gov`).

Per the explicit instruction to "go further" on KS-05 rather than block on
this, `src/parse/kcc_operators/`, `src/normalize/kcc_operators.rs`, and
`src/materialize/kcc_operators.rs` were built and tested against a
hand-built fixture HTML page and CSV bodies modeled on KCC's own
documented description of the source ("continuously updated," exposing
"active-only and all-operator downloads") — not the real page. This is a
materially lower confidence level than every other importer in this repo.

## What is and isn't confirmed

**Confirmed** (structural, doesn't depend on the live page):
- The `ks_ingestion_run`/`ks_source` state-machine and idempotency
  guarantees apply identically to this importer (same code path as
  KS-03/KS-04).
- `ks_operator` has no address/phone/email/contact-person column, by
  schema, not by convention.
- Identity is always `kcc_license_number`; `ks_operator_alias` is keyed
  off it, never off name similarity.
- `PublicOperator` (the only struct any future public-facing surface may
  build a response from) passes `privacy::assert_no_forbidden_fields`.

**Not confirmed** (depends entirely on the real page/file, never fetched):
- The actual anchor text/href pattern KCC uses for its two downloads —
  `source::kcc_html::discover_operator_links`'s matching heuristic
  (anchor text+href containing "operator" plus "active" or "all") is a
  reasonable guess, not a confirmed pattern.
- The actual CSV (or other format entirely — could be XLSX, could be a
  paginated HTML table with no downloadable file) the links point to.
  `parse::kcc_operators::csv_source` assumes CSV with a header row and
  tries several plausible header spellings per column
  (`LICENSE_HEADER_CANDIDATES` etc. in that file) — if the real format
  isn't CSV at all, this importer will fail at the parse step, which is
  the correct failure mode (loud, via `ImportError::Csv`/`fail_run`) but
  means the importer needs real rework, not just a header-name tweak.
- Whether KCC's "all operators" download is genuinely a superset of
  "active operators" (assumed for `active_status` derivation and the
  reconciliation report) — this is what KCC's own page copy implies, but
  has never been checked against real data.
- Whether either download exposes a per-record "last updated" timestamp
  at all (`source_updated_at` is `NULL` whenever the assumed column isn't
  found, which — until verified — is every real run).

## How to verify this for real

1. From an environment that can reach `kcc.ks.gov`, fetch
   `https://www.kcc.ks.gov/kansas-oil-operators` and inspect its anchors.
2. Compare against `discover_operator_links`'s matching rule
   (`src/source/kcc_html.rs`). If the real anchors don't match, fix the
   heuristic there — it's isolated in one function specifically so this
   is a small, contained change.
3. Download whatever the two links point to and compare its actual
   header row against `LICENSE_HEADER_CANDIDATES`/`NAME_HEADER_CANDIDATES`/
   `CITY_HEADER_CANDIDATES`/`UPDATED_HEADER_CANDIDATES` in
   `src/parse/kcc_operators/csv_source.rs`. If it isn't CSV, `csv_source.rs`
   needs to be replaced with a parser for the real format.
4. Run `cargo run --bin ks_ingest -- snapshot-operators` for real (no
   `--limit`-style flag exists for this importer — unlike KS-03's wells
   importer, there's no equivalently large object-ID range to worry about
   pulling a small slice of) and inspect the printed manifest/reconciliation
   counts against what you already know about KCC's operator roster.
5. Once verified, replace this doc with a live-smoke-check doc in the
   style of `docs/ops/ks-03-kgs-wells-live-smoke.md`, and update
   `docs/architecture/kansas-source-boundary.md` item 30 to say so.

## What the automated tests do cover

`tests/kcc_operators_import.rs` (4 tests) and
`tests/kcc_operators_materialize.rs` (5 tests) exercise the full pipeline
end-to-end against `wiremock` — link discovery, CSV parsing, idempotent
checksums, the "no links found" and "zero records parsed" reject-not-silently-empty
paths, identity resolution, active/all-derived status, and alias
retention. They prove the code does what it's designed to do against
input shaped the way this document assumes KCC's real page is shaped —
they cannot prove that assumption itself.
