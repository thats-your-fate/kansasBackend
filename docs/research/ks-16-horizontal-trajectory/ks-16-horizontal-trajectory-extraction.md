# KS-16 — Research: Kansas Horizontal-Well Trajectory Extraction

Date: 2026-09-06
Task: KS-16 (research spike — not a production importer)

## Status and scope discipline

**This task built no code, no migration, and no geometry table.** Per
the task doc's own framing ("this is a research spike, not an immediate
production importer") and acceptance criteria ("no trajectory appears in
production from this task"), everything below is research output:
findings, fixtures (`fixtures/`), a sample manifest
(`sample-manifest.md`), and the recommendation this document ends with.
The horizontal/directional badge KS-04/KS-14 already built
(`ks_well.horizontal_or_directional`, surfaced as a
`WellboreV1.trajectory_kind` in the `state-export-v1` exporter) is
**derived from KGS's own well-class/status text, not from any trajectory
geometry**, and nothing here touches it — it remains available
independently of whatever this task recommends, exactly as required.

## What this task actually did

Real, live investigation of Kansas horizontal-well completion documents,
building directly on KS-15's discovery (the KGS per-well detail page and
its document-adjacent infrastructure). Three real wells were
deep-sampled — their actual public completion-report attachments were
fetched and read in full — spanning three different operators, three
different counties, and three genuinely different document eras/formats.
See `sample-manifest.md` for exactly which wells and how they were
selected, and `fixtures/` for real, redaction-safe excerpts of what was
found (these are public regulatory filings — well completion reports are
public record in Kansas, not personal data — reproduced here for
engineering reference, with no operator contact/address information
carried into any fixture, consistent with this repo's existing privacy
discipline).

## A finding that revises KS-15's own conclusion

KS-15 sampled one vertical well from 1980 and concluded that KGS's
document repository is effectively inaccessible without a Walter's
Digital Library login. **That conclusion does not hold for wells with a
sufficiently recent filing (roughly the KOLAR electronic-filing era,
observed starting around 2012-2013 in this task's sample).** Those
wells' detail pages carry a second, entirely different section —
**"ACO-1 and Driller's Logs"** — with direct links to real PDF files
hosted at `kgsimages.blob.core.windows.net`, a plain Azure Blob Storage
host that:
- returns the real document over a plain, unauthenticated `GET` (no
  login wall, confirmed via direct fetch — `HTTP/1.1 200 OK`,
  `Content-Type: application/pdf`, real byte counts, in every case
  checked in this task);
- has **no `robots.txt` at all**, and is not one of the
  `www.kgs.ku.edu`-hosted paths KS-15 found disallowed (`/PRS/ACO/`,
  `/PRS/Documents`, etc. — those disallowed paths are a different host's
  policy and do not extend to this blob-storage domain).

This means **real ACO-1 completion reports and their attachments are, in
fact, reachable through a compliant, public, unauthenticated channel**
for wells filed since roughly the early 2010s — a materially different
and more optimistic starting point than KS-15's own doc implies. Recorded
here (rather than by retroactively rewriting KS-15) since KS-16 is the
task that actually tested this.

Older wells (this task's 1928 sample; KS-15's own 1980 sample) still
show **no** "ACO-1 and Driller's Logs" section at all and fall back to
the WDL login wall exactly as KS-15 found — the two findings are not in
conflict, they describe different document-vintage populations.

## Format taxonomy (from three real, deep-sampled documents)

| Format | Real example | Machine-readable? | Directional content | Extraction method needed |
|---|---|---|---|---|
| **A — narrative memo** | Kinney Oil, Strahm Farms 1-27 (Nemaha, 2013-2014) | Digital text, but unstructured prose | 2 named points only (landing/"heel", TD/"toe"): MD, TVD, inclination, azimuth. No N/S, E/W, lat/long, datum. | Regex/NLP extraction against a controlled vocabulary of phrasing (see below) — genuinely fragile, operator-specific wording |
| **B — structured digital table + vector plat** | Wilson County Holdings, TL012 (Wilson, 2014-2015), contractor REI Drilling | Genuinely digital, tabular text; final page is a vector CAD drawing | Full station-by-station MD/azimuth/inclination/EW/NS across 9 named branches (a real multilateral). A real vector as-drilled plat on the final page. No datum/projection stated anywhere in the 40-page document. | PDF table-text extraction (high confidence) for the tabular pages; vector-graphics coordinate extraction (a different, harder technique) for the plat page; **not** OCR for either |
| **C — scanned historical image** | Roxana Petroleum Corp., "Gally" No. 1 (Cowley, 1928) | Image only, no text layer | None — this is a formation-tops/casing log, not a directional survey, because the well predates directional drilling | OCR (e.g. Tesseract), with real accuracy risk from period typewriter fonts, foxing/water damage, and handwritten annotations |

A fourth theoretical cell — **a scanned image of a genuine directional
survey** (OCR needed to recover real trajectory numbers, the hardest
real combination) — was not observed in this small sample and must be
treated as an open, unconfirmed risk, not assumed rare, until a larger
sample is drawn (see "measurable threshold," below).

A real, additional finding cutting across formats: **KOLAR's own
document-type label is not always reliable.** One attachment labeled
"Well Map" (which should be a plat/graphical drawing per its name)
contained the identical narrative-memo text as the completion report's
own attached directional summary — not a map at all. Any future
extraction pipeline must classify a document by its actual content, not
trust the source's own label as ground truth.

## Extraction-success statistics (honest, from this sample size)

**3 of 3** deep-sampled documents were successfully read and understood
by this task (using this session's own multimodal document reading, not
a purpose-built extraction pipeline). Of those 3:
- **1 of 3** (Format B) contained genuinely complete, structured,
  station-by-station directional data extractable with high confidence
  by a text/table-parsing approach alone.
- **1 of 3** (Format A) contained only two summary points, extractable
  with moderate confidence via targeted pattern matching, but far short
  of a full trajectory.
- **1 of 3** (Format C) contained zero directional content, because the
  well itself predates directional drilling — this is a **sampling
  artifact of KGS's own "horizontal wells" query classification**, not
  a document-extraction failure, and is exactly why "estimated coverage"
  below is presented as a range, not a single confident number.

**These statistics are not a reliable estimate of statewide
extraction-success rates.** Three documents is not a sample size a
production/no-production decision should be based on — it is a proof
that both a "good" and a "hard" case are real and current, which is
exactly what a research spike is for. See "measurable threshold" below
for what sample size would actually support a real recommendation.

## Recommended parsing approach

1. **Classify before extracting.** For each candidate document: (a)
   check whether it has an extractable PDF text layer at all (a cheap,
   deterministic check — e.g. via a PDF library's text-extraction call
   returning non-empty content) to route digital documents away from an
   OCR path; (b) within digital documents, classify by structural shape
   (a table with recognizable column headers like `Measured Depth`/
   `Azimuth`/`Inclination` vs. free-text prose) before attempting either
   a table parser or an NLP/regex extractor.
2. **Never fall back to guessing.** A document that doesn't cleanly
   match a known shape must be marked `unresolved`/`needs_review`, the
   same discipline this repo already applies everywhere else (KS-06's
   unresolved-intents report, KS-09's unresolved-lease-APIs report,
   KS-15's unresolved-documents report) — never sent through a
   best-effort parser that might silently fabricate a plausible-looking
   but wrong number.
3. **Table extraction (Format B) is the highest-confidence, lowest-effort
   win** — a real PDF-table-text extraction pass (no OCR) against
   documents that already pass the "has a real text layer" check and
   match a recognizable directional-survey column-header set. This is
   the format worth building first if a future task proceeds.
4. **Narrative-memo extraction (Format A) needs a controlled vocabulary,
   not free-form NLP.** This task's one real sample used "Landing Point,"
   "Heal" (a likely typo for "Heel"), and "TD"/"Toe" — a future extractor
   should maintain an explicit list of known phrasings per contractor/
   operator (since this content is contractor-authored, not
   KCC-standardized) and flag anything outside that list as unresolved
   rather than guessing at a new phrasing's meaning.
5. **OCR (Format C, and any future scanned-directional-survey case) is
   its own, separately-scoped problem** — different tooling, different
   confidence characteristics, and (per this task's real sample) real
   image-quality risk from aged source documents. Do not bundle its
   success/failure rate into the digital-format numbers above.
6. **Vector-plat extraction (the as-drilled plat page) is a distinct
   third technique** from both table-text and OCR — it requires reading
   the PDF's own vector drawing primitives (line/curve paths) and
   correlating them to the plat's stated scale and orientation (a north
   arrow and a scale bar were present in the one real example found).
   This is meaningfully harder engineering than either text extraction
   or OCR and was not attempted in this task beyond visual inspection.

## Coordinate-conversion approach

None of the three real documents stated a datum or projection for their
directional/offset data (the ACO-1 form itself has a `Datum: NAD27 /
NAD83 / WGS84` checkbox and a GPS lat/long field — both were **left
blank** in every real sample this task checked). Any future extraction
must therefore:
1. Treat a document's own N/S, E/W, or azimuth values as **relative
   offsets from an implicit reference point** (almost certainly the
   surface/kickoff location), never as absolute coordinates on their
   own.
2. Obtain the well's real surface location from the already-canonical
   `ks_well` record (KS-04's `display_geometry_4326`/`source_latitude`/
   `source_longitude`, already datum-flagged via
   `location_source`/`location_warning`) rather than from the completion
   document itself, since the document rarely states one reliably.
3. Resolve "azimuth (Grid)" against a real, well-documented Kansas grid
   convention (state plane zone by county, since Kansas has both North
   and South zones) before converting to true/geographic north — this
   task did not confirm which grid convention Kansas operators
   consistently use in practice, and a future task must verify this
   against a larger real sample before trusting any converted heading.
4. **Never compute an absolute terminus coordinate from a narrative
   memo's two summary points alone** (Format A) — with only landing and
   TD given, no real path shape between them is known; presenting that
   as a "trajectory" would be exactly the "guessed straight-line
   lateral" this task's acceptance criteria forbids.

## Provenance model (what a future geometry table must record)

Per the task doc's own requirement ("preserve document ID, page,
extraction method, datum, original values and confidence for every
vertex"), and grounded in what this task's real samples actually
distinguish:
- `document_source` (`KGS_DOCUMENTS`/KOLAR vs. the older
  `kgs.ku.edu/.../WebDocs/ACO/...` scan convention — two real, distinct
  hosting/provenance patterns found in this task, not a guess)
- `source_document_id` / `doc_id` (KOLAR's own numeric doc ID, real
  examples: `1194781`, `1250421`)
- `page_number` (the real Format-B sample's directional table spans
  pages 4-39 of a 40-page document; a vertex's page matters for
  re-verification)
- `extraction_method` (`pdf_table_text` / `pdf_prose_nlp` /
  `ocr_tesseract` / `vector_plat` — one per format found, plus
  `unresolved` for anything that didn't match a known shape)
- `original_value_text` (the literal string extracted, before any unit
  conversion or parsing — so a future review can always see exactly what
  the source said)
- `datum` (nullable — most real documents observed state none at all;
  never fabricate one)
- `reference_point` (what a relative offset is measured from, since
  Format B's EW/NS values are not absolute)
- Standard lineage columns this repo already uses everywhere else:
  `source_ingestion_run_id`, `first_observed_at`/`last_observed_at`.

## Confidence model

A three-tier confidence label, assigned per vertex (not per document —
a single document can mix confidence levels, as Format B's clean table
pages versus its garbled single-column pages demonstrate):
- **`high`** — extracted from a recognized structured table with a
  matching column-header set, from a document confirmed to have a real
  PDF text layer (no OCR involved).
- **`low`** — extracted from free-text prose via controlled-vocabulary
  matching (Format A), or from a table page that partially matched
  expected structure but required a fallback assumption (e.g. an
  unlabeled single-column page).
- **`unresolved`** — anything OCR'd (until OCR accuracy against a real
  Kansas sample is independently measured), anything not matching any
  known format, or anything where the document's own label contradicts
  its content (the "Well Map" mislabeling found in this task). An
  `unresolved` vertex must never be silently promoted to `low`/`high`
  by a downstream consumer.

## Estimated coverage

KGS's own live "Horizontal Wells" query returned **1,180 real records**
statewide (confirmed 2026-09-06). This is the honest starting number,
with two important caveats found directly in this task's own sample,
not assumed:
- It is a **query result, not a verified count of wells with real
  extractable trajectory data** — this task's own oldest sample (1928)
  is almost certainly a classification artifact, not a genuine
  horizontal/directional well, meaning the true population needing
  trajectory extraction is likely **smaller** than 1,180.
- Coverage of the KOLAR document-hosting pattern (the one that actually
  makes real extraction possible) appears **vintage-dependent** — one of
  this task's own three deep-sampled wells (in the same era as the two
  KOLAR-covered wells) had **no** digitized documents at all, so even
  among modern horizontal wells, document availability is not universal.

**No statewide percentage is reported here** — three deep-sampled wells
cannot responsibly produce one, and doing so would misrepresent the
actual state of knowledge. See "measurable threshold" for what would be
needed to produce a trustworthy number.

## Production / no-production recommendation

**No production.** Per the task's own framing, this is expected, not a
disappointing result: a research spike's job is to establish whether
proceeding is warranted, not to proceed. The reasons, concretely:
- Real trajectory data exists and is genuinely reachable for at least
  some wells (Format B is a real, strong positive finding) — this is
  not a "the data doesn't exist" conclusion.
- But format variability, label unreliability, missing datum/projection
  information, and a real vintage/coverage gap are all confirmed, not
  hypothetical — building a production importer today would mean
  guessing at exactly the kind of missing information (datum, reference
  points, unverified grid conventions) that would violate "no guessed
  straight-line laterals."
- The horizontal/directional **badge** already ships independently
  (KS-04/KS-14) and needs nothing from this task to keep working.

## A measurable threshold for proceeding

A future task should be authorized to build a real trajectory-geometry
importer only once **all** of the following are true, established
against a real, randomly-drawn sample (not a hand-picked one like this
task's, which was deliberately chosen to illustrate format diversity
rather than to be statistically representative):

1. **Sample size**: at least 100 real horizontal/directional wells,
   stratified across at least 5 operators, 5 counties, and both
   pre-2015 and post-2015 vintages (mirroring the real vintage split
   this task found matters).
2. **Extraction-success rate**: at least 70% of that sample's documents
   classify cleanly into a known format (Format A/B/plat/OCR) with no
   `unresolved` vertices, using the real, automated pipeline (not this
   task's manual multimodal reading) — measured and reported honestly
   even if it comes in lower.
3. **Coordinate-conversion validation**: the assumed grid/azimuth
   convention is independently confirmed against at least 10 real wells
   whose surface-to-terminus vector can be cross-checked against KGS's
   own point location and a known lease/section boundary (e.g. via the
   real vector plats this task found exist for at least some wells).
4. **Confidence distribution is disclosed**, not just an aggregate
   success rate — e.g. "70% high, 20% low, 10% unresolved" is a
   meaningfully different (and more honest) finding than "90% success"
   that quietly folds low-confidence guesses in with real ones.

Until those four conditions are met and documented, this task's
recommendation stands: index metadata and badges (already done),
research further if warranted, but do not build the geometry table or
any production trajectory data.
