# KS-15 — Kansas Well-Document Metadata Index: Live Smoke Check

Date: 2026-09-05
Task: KS-15 (index Kansas well-document metadata)

## Purpose

Unlike KCC/KOLAR (`kcc.ks.gov`, confirmed unreachable again this task —
`https://www.kcc.ks.gov` and `https://kolar.kcc.ks.gov` both returned no
response at all), KGS's document-adjacent infrastructure **is reachable**,
and its real structure was discovered by direct fetch during this task —
not assumed. This doc records exactly what was found and verified live.

## Real discovery

1. `https://www.kgs.ku.edu/PRS/petroDB.html` (200, reachable — already
   used as `kgs_wells_catalog`) links to
   `https://apps.kgs.ku.edu/web/qualified/index`, a JavaScript
   DataTables app whose backing AJAX data endpoint was **not**
   successfully reverse-engineered (probing plausible paths like
   `/data`, `/api/data`, `/records` all returned bare 500s with no
   further signal in the time available).
2. Following a different real link from
   `https://www.kgs.ku.edu/Magellan/Qualified/index.html` led to
   `https://www.kgs.ku.edu/Magellan/Qualified/kid.html` — a real,
   server-rendered HTML form (`POST
   https://chasm.kgs.ku.edu/ords/qualified.well_page.wellsKID`, field
   `f_k`) for looking up a well by KID. This confirmed the same
   `chasm.kgs.ku.edu` ORDS/Oracle APEX host already used (unrelated
   task) for `kgs_horizontal_wells`.
3. A real KID (`1001190703`, API `15-009-03905`) was obtained from the
   already-confirmed-live KGS wells ArcGIS layer and fed into that form.
   The response contained a **direct link to the per-well detail page**:
   `https://chasm.kgs.ku.edu/ords/qualified.well_page.DisplayWell?f_kid=1001190703`
   — a plain `GET`, no form submission needed once the KID is known.
   This is the real URL `config::SourceUrls::kgs_well_detail_page`
   (`KGS_WELL_DETAIL_PAGE`) points at.
4. That detail page (fetched directly, in full) does **not** expose a
   downloadable per-document manifest to an unauthenticated client. It
   contains real well metadata (API, KID, lease, operator, dates,
   coordinates, depth) plus exactly two document-relevant things:
   - A direct link, verbatim: **"View well in the Walter's Digital
     Library (WDL login required)"**, pointing at
     `https://www.kgslibrary.com/index.cfm?whk=1001190703`. This is the
     real repository where scanned well documents (C-1, ACO-1, logs,
     etc.) actually live, and the page's own label confirms it requires
     a login — exactly the "login-only library" the task doc says never
     to bypass.
   - A "Tops Data" table citing, per formation top, a source document
     and date — for this real well, seven identical
     `"Completion Report"` / `"Apr-15-1999"` citations (one per
     formation). This is genuine public metadata (visible without any
     login) evidencing a real ACO-1 (well completion) document's
     existence and filing date, even though the document image itself
     is not fetchable from this page.
5. `https://www.kgs.ku.edu/robots.txt` (fetched directly) explicitly
   disallows exactly the paths where Kansas well-document scans/logs
   live: `/PRS/ACO/`, `/PRS/Documents`, `/PRS/Transfers`, `/PRS/Elog2/`,
   `/PRS/ElogScan`, `/PRS/Images`, `/PRS/Images2`, `/PRS/TypeLogs`,
   `/PRS/Typelog`, `/WellLogs/`. `chasm.kgs.ku.edu` and
   `www.kgslibrary.com` both have no `robots.txt` at all (404) — the
   detail page host itself is not robots-restricted, only the actual
   document repositories are.

## Design consequences (not guesses — each maps to a specific finding above)

- **Metadata-first is not just a policy choice here — it is what the
  live source actually allows.** A KGS well-detail page's own public
  content is exactly "does this well have documents" (WDL reference) and
  "what kind, filed when" (Tops Data citations) — not the documents
  themselves. The `parser_status`/`public_access_state` split (migration
  `0027`) and the CHECK constraint preventing a non-public document from
  ever being queued are a direct, structural encoding of what was
  actually found live, not a hypothetical safeguard.
- **Per-well, not bulk.** No statewide document-listing endpoint was
  confirmed to exist (the JS-driven "qualified" app's data API wasn't
  reverse-engineered in the time available) — `snapshot-documents-for-well`
  is per-KID by design, mirroring how the real KID-lookup form itself
  works. A full statewide index would mean one fetch per Kansas well
  (~180,000), a real scale characteristic documented here (matching
  KS-08/KS-09's precedent) rather than executed in this session.
- **`classify_access_state`'s two rules are both drawn directly from
  real, confirmed data**: the `kgslibrary.com` host match and the "login
  required" label-text match both come from the one real WDL link found;
  the `ROBOTS_DISALLOWED_KGS_PATHS` list is transcribed directly from
  the real `robots.txt` fetched during this task.

## What this verified (2026-09-05)

Real, live, end-to-end run via the compiled `ks_ingest` binary:

```
$ ks_ingest snapshot-documents-for-well 1001190703 --api-number 15-009-03905
run_id: d2ca1d04-f031-4b98-8312-e6e01662ea27
status: Completed
staged_count: 2

$ ks_ingest materialize-documents d2ca1d04-f031-4b98-8312-e6e01662ea27
created: 2
updated: 0
events_emitted: 2
unresolved: 2
```

Confirmed in the resulting real rows:
- `wdl:1001190703` → `document_type=other`, `public_access_state=login_required`,
  `parser_status=skipped_restricted` — the real WDL reference, correctly
  never queueable.
- `tops:1001190703:Completion Report:Apr-15-1999` → `document_type=aco1`,
  `filing_date=1999-04-15` (parsed from the real `Apr-15-1999` KGS date
  format), `public_access_state=public`, `parser_status=not_queued` — the
  real completion-report citation, correctly classified and dated.

`unresolved: 2` is expected and correct here: no `ks_well` row for KID
`1001190703` existed in this particular local database at smoke-test
time (a real wells snapshot was not re-run in the same session pass) —
the well relationship resolves correctly whenever a matching `ks_well`
row does exist, per the fixture-based integration tests.

## What this did NOT verify

- The actual JS-driven "qualified" app's bulk document-listing data API
  (never reverse-engineered — see discovery item 1).
- KCC/KOLAR (`KCC_KOLAR_DOCUMENTS`) — confirmed unreachable this task
  (`https://www.kcc.ks.gov` and `https://kolar.kcc.ks.gov` both returned
  no response), same finding as every other KCC source in this repo. No
  importer exists for it: `run_document_import` is specific to
  `KGS_DOCUMENTS`'s real, confirmed page shape (source code and
  `document_source` value both hardcoded), not a generic per-source
  design — building a "generic" extractor against KOLAR's unknown real
  markup would be guesswork, the same reasoning KS-05/06/07 already
  established for not building against an unreachable, unconfirmed KCC
  page shape. `ks_well_document`/`ks_well_document_raw_snapshot` both
  already accept `KCC_KOLAR_DOCUMENTS` as a valid `document_source`
  value (migrations `0027`/`0028`), so a future KOLAR-specific importer
  needs no schema change, only its own extraction module once the real
  page structure can be confirmed.
- Real login-walled Walter's Digital Library content — deliberately
  never attempted, per the task doc's explicit instruction.
- A full statewide document-metadata index (would require ~180,000
  per-well fetches — see "Design consequences" above).
- Actual document download/extraction (the "controlled queue" mechanism
  — `enqueue_for_download`/`next_queued_documents`/`mark_downloaded`/
  `mark_extraction_failed` — is built and tested against fixtures, but
  no real document was ever downloaded in this session, consistent with
  the task doc's "do not mass-download" instruction).

## Addendum (2026-09-06, from KS-16)

KS-16's own research found that this doc's conclusion above is
**vintage-dependent**, not universal: a well's detail page can carry a
second, entirely different "ACO-1 and Driller's Logs" section (not
present on the 1980 well sampled here) with direct, real, unauthenticated
PDF links to `kgsimages.blob.core.windows.net` — a host with no
`robots.txt` and no login wall, genuinely different from the WDL library
this doc describes above. Real completion reports (including, for some
wells, genuine directional-survey data and as-drilled plats) are
reachable through that channel for wells filed roughly since the early
2010s. See `docs/research/ks-16-horizontal-trajectory/` for the full
findings — this repo's document-metadata importer (`parse::kgs_documents`)
was not changed to consume this newly-found channel; that remains future
work, tracked there rather than retrofitted into this already-shipped
task.

## When to re-run this

- If `www.kgs.ku.edu/robots.txt`'s disallow list changes, re-diff it
  against `ROBOTS_DISALLOWED_KGS_PATHS` in
  `src/normalize/kgs_documents.rs`.
- If the Walter's Digital Library ever changes its login requirement or
  domain, re-verify `classify_access_state`'s `kgslibrary.com`/"login
  required" detection against a real fetch before trusting it.
- If someone gets time to reverse-engineer the JS "qualified" app's real
  data API, that would upgrade this from per-well to bulk indexing —
  worth revisiting given the real ~180,000-well scale.
