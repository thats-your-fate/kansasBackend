# Fixture provenance — `format-b-directional-survey.csv`

Real, public KCC/KGS filing attachment, fetched directly 2026-09-05
during KS-16 research and transcribed verbatim (column headers
normalized to `snake_case`; values unchanged) from a genuinely digital
PDF table (not a scan — the source PDF's text layer was extracted
directly, confirming this format is real-text, not OCR-territory).

- **Well**: Wilson County Holdings LLC, "Wilson County Holdings TL012,"
  Wilson County, KS. API: 15-205-28317-01-00. KID: 1044939529. KCC
  operator license 34697. Directional contractor: REI Drilling, Inc.
  (KCC license 35154).
- **Document**: Well Completion Report (ACO-1) attachment, received by
  KGS 2015-07-09.
- **Real source URL** (public, no login, `kgsimages.blob.core.windows.net`
  carries no `robots.txt` and is not part of KCC/KGS's own disallowed
  paths): `https://kgsimages.blob.core.windows.net/kolar/filer_1/public/2015-04/solar.documents.released_pdf/doc_id=1250421.pdf`
  — a 40-page PDF; this fixture is the first ~40 rows of its directional
  survey table (pages 4-5 of 40).

## What this fixture does NOT capture (real, observed limitations)

- The real document contains **hundreds more rows** across at least 9
  named branches (`main`, `b4s`, `b4`, `b2s`, `b2`, `b1s`, `b1`, `b1e`,
  `b2e`) — a genuine multilateral completion. This fixture is truncated
  for size; a real parser must handle many more rows and branches than
  are reproduced here.
- Later pages of the real document (pages ~36-39) render as a **single
  numeric column per page**, with no repeated header and no visible
  column label — almost certainly an Excel-to-PDF export artifact where
  a wide table's columns wrapped across page boundaries. A naive
  per-page table extractor would misread these pages entirely; a real
  parser needs either full-document column-position tracking or a
  fallback to flag such pages as "unparsed, needs review" rather than
  guessing which column a lone number belongs to.
- The real document's **final page (page 40) is a vector CAD plan-view
  plat** (not a table) showing all branches plotted against section/lease
  boundaries, a north arrow, and a scale bar (`1"=200'`), produced by
  the same contractor (REI Drilling). This is the real "as-drilled plat"
  the task doc asks about — not reproduced here (not practical as a text
  fixture); see the main report's discussion of vector-plat extraction.
- **No datum or projection is stated anywhere in the 40-page document.**
  EW/NS offsets are relative to a per-branch reference point (presumably
  the surface/kickoff location), not absolute coordinates — converting
  these to real lat/long requires combining them with the well's own
  surface location (from the ACO-1 form itself or the KGS well master)
  and an assumed grid-north convention, which this document never states
  explicitly.
