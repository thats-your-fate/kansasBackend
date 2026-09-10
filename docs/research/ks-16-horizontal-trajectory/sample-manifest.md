# KS-16 sample manifest

Every row below is a real Kansas well, confirmed live 2026-09-05/06
against `https://chasm.kgs.ku.edu/ords/qualified.ogw5.HorizWells`
(KGS's own "Horizontal Wells" query — **1,180 real records** returned
statewide at the time of this research, the denominator used throughout
this task's coverage estimate) and, for the six wells below, its
per-well detail page (`qualified.well_page.DisplayWell?f_kid=<KID>`).
"Deep-sampled" means the well's actual document(s) were fetched and
inspected, not just its listing row.

| KID | API | Operator | County | Vintage | Deep-sampled | Document section present | Format observed |
|---|---|---|---|---|---|---|---|
| 1044661378 | 15-131-20219-0100 | Kinney Oil Company | Nemaha | 2013-2014 (spud-completion) | Yes | "Documents from the KCC" (KOLAR) | A — narrative memo, 2 points, no coordinates |
| 1044939529 | 15-205-28317-0100 | Wilson County Holdings LLC (Jako Productions) | Wilson | 2014-2015 | Yes | "Documents from the KCC" (KOLAR) | B — structured digital directional-survey table (9 branches) + vector as-drilled plat |
| 1046935948 | 15-035-01809-0100 | Roxana Petroleum Corp. | Cowley | 1925-1928 | Yes | "PDF files from the KGS" (pre-KOLAR scan) | C — scanned image, formation-tops log only, no directional content at all |
| 1028007285 | 15-103-20433-0100 | KLM Exploration Co., Inc. | Leavenworth | undated (permit/spud/completion all blank in the listing) | Listing only — well-detail page fetched, no "ACO-1 and Driller's Logs" section present at all | None | N/A — no digitized documents of any kind for this well |
| 1025775640 / 1025775641 / 1025775642 / 1025775643 | (same section, distinct KIDs) | (same operator group as above, Sec. 3 T10S R20E) | Leavenworth | listing only | No | Not checked individually | Listed for operator/county diversity only; not deep-sampled given this task's time budget |
| 1045002690 / 1045072134 / 1045072135 | — | Wilson County Holdings LLC (Jako Productions) | Wilson | listing only | No | Not checked individually | Same lease/pad cluster as the KID 1044939529 deep-sample; listed to show a single operator can file many wells from one facility ("Fredonia Facility," per the real plat's own title block) |

## Sampling method and its limits

Wells were selected by (a) taking the first real row from the default
sort of the live `HorizWells` query (KID 1044661378), (b) taking a row
from a different page with a visibly different operator/county (KID
1044939529 and its Leavenworth neighbor), and (c) re-sorting by spud
date to find the oldest real record returned (KID 1046935948) to
deliberately test the vintage axis. This is **not** a statistically
random sample — six wells (three deep-sampled) out of 1,180 is not
enough to produce reliable extraction-success statistics by itself; see
the main report's "measurable threshold for proceeding" section for how
many real documents a future task should sample before trusting any
success-rate number.

## Scanned versus digital, as actually observed

Of the three deep-sampled wells:
- 1 was **genuinely digital** with only free-text prose (Format A).
- 1 was **genuinely digital** with a real structured table plus a
  vector CAD plat (Format B) — the single best-case format found.
- 1 was a **genuine image scan** with no directional content at all,
  because it predates directional drilling in Kansas entirely (Format
  C) — and its presence in the "horizontal wells" result set is itself
  evidence that the source query's own classification is not a reliable
  filter for "wells that actually need trajectory extraction."

No wells in this small sample were confirmed as *scanned images of a
real directional survey* (as opposed to a scanned non-directional log) —
that combination (the hardest real case: OCR needed to recover genuine
trajectory numbers) was not observed in this sample and should be
treated as unconfirmed risk, not assumed absent, in any coverage
estimate — see the main report.
