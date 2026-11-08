# Kansas native ingestion verification — November 8, 2026

All implementation is on main. Native PostgreSQL, Rust/Python, AWS CLI and user
systemd units are used. No container runtime is involved.

## Published data

- Official current oil and gas archives reconciled by monthly row counts and
  numeric volume sums: **416,382 records, January 2025–June 2026**. The daily
  refresh successfully repeated publication without duplicate natural keys.
- Public export **96394106-f1e5-4b75-a573-94ac0234febc**: 3,859,874 records,
  including 477,274 wells, 416,382 production rows, 4,261 documents, 4,261 document
  contents, and 5,460 document/event evidence links. All documents and links
  resolved to their canonical wells/events on the shared host; zero failed rows.
- Manifest SHA-256:
  `4eb78c1791d7903c01e924973dc374289245d70cdcc7f1122f0639f9f93a533c`.
- Shared sync finished at 07:11:56 UTC: 782 batches, 300,567 inserted,
  3,559,307 unchanged, zero failed. Source acknowledgement and target confirmation
  both succeeded. Refreshed workspace summaries show 4,261 documents and 416,382
  production rows, with latest document activity November 6.
- Kansas production stays at lease level. Verified associations expose lease
  context to wells; volumes are never allocated or copied into individual wells.
  Shared API 15067202840000 has 18 linked lease months through June 2026.
- Document discovery is progressive, from January 2025. The initial shared
  package covers 1,413 documented wells, with latest KGS receipt November 6.
  The source worker continues adding files after that snapshot.

## Live checks

- January 2025 production page returns 23,216 lease/month/product records.
- Allan 1 (API 15163245390000) has its five-page drilling intent. KGS receipt
  August 6, 2026; registered November 7; effective date remains unverified.
- Public download and independent read using the shared host's R2 configuration
  both returned HTTP 200, 1,940,668 bytes, and identical SHA-256
  `e58e8989408e04da1b06a188b186e1b7b92193b7dbda4fb3f2f6a7469d52556e`.
- Rooks County permit and producing sections return documented wells first,
  preserving each section's lifecycle filter.
- Native frontend release `.next-native-071a0b5` is promoted. Browser proof:
  `kansasMarketing/reports/kansas-documents-live.png` (frontend repository).

## Background operation and recovery

- Persistent user timers: `future-wells-ks-wells.timer`,
  `future-wells-ks-production.timer`, `future-wells-ks-documents.timer`.
- Documents checkpoint after a six-hour budget, before the eight-hour service
  cap. Conflicting identities, incomplete PDFs and missing dates remain errors;
  they are not published under guessed identities or dates.
- ZIP snapshot and materialization stream bounded batches instead of retaining
  the statewide CSV in memory. The fresh official snapshot has 519,691 raw rows.
- An exclusive database source lock recovers abandoned running ZIP attempts.
  The confirmed OOM run was marked failed and replaced by completed snapshot
  `3ab53e3e-97fb-43e4-bd50-e319fba13b19`. Canonical publication is running.
- The ZIP omits COUNTY. A regression fix preserves existing county values.
  All 57,305 affected values from the first partial refresh were restored from
  checksum-verified pre-refresh export parts, matching exact internal well UUIDs;
  no unmatched records. Rechecks found zero missing counties in that scope.
- A successful unchanged inventory snapshot is not materialized again, so
  repeat daily checks cannot advance missing-snapshot tombstone counts.

## Tests and limits

- Native disposable PostgreSQL: 12 well materialization tests and four ZIP
  import tests passed, including abandoned-worker recovery and county retention.
- Python: ten document parser/budget tests, one unchanged-refresh test, four
  transactional publication/revision/identity/event tests passed.
- Earlier pipeline checks also passed: Rust unit/document/production/export
  suites, native builds, frontend typecheck and lint.
- Source PDFs with other/multiple well APIs remain flagged for review. Historical
  backfill coverage is not claimed complete while the scan is still draining.
- The current published package predates completion of the fresh inventory
  materialization. Scheduled exports/syncs carry later inventory and document
  updates.
