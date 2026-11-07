# Kansas native current-data ingestion

All jobs run on main as native user systemd services. PostgreSQL, Rust, Python,
Poppler and the official AWS CLI are native installations. Existing R2 credentials
come only from Kansas's private `.env`; neither credentials nor signed URLs enter
source metadata or logs.

## Production and inventory

`scripts/kansas_refresh.py production` fetches only the two current 2020-present
KGS partitions through `ks_ingest snapshot-production --current`. It publishes
monthly records from **2025-06-01** using the native materializer's `--since`
filter. Original immutable staged rows remain available. Baseline materialization
upserts and removes absent facts in one statement per source partition. The
worker refuses a missing month, a >10% count drop or test-sized source, then
independently reconciles monthly record counts and numeric volumes to staging.
Repeated runs do not create duplicate production records.

`scripts/kansas_refresh.py wells` stages the official full ZIP, requires a
completed statewide snapshot, materializes it through the existing native Rust
well importer, repairs lifecycle mappings and retains dated well history.

Production stays at lease/product/month grain. Exact KGS product-KID, unique
product-DOR and exact API associations provide linked-lease context; volumes
are never allocated across individual wells. The public `/api/ks/production`
resource and `/production` page expose the real reporting month and source
import date. The most recent source month may be partial.

## Public documents

`scripts/kansas_documents.py` verifies each KGS page's KID and API against a
non-tombstoned well in a completed statewide inventory. It reads the real public
PDF links and KGS receipt dates, ignores WDL/login and non-public paths, and
rejects inconsistent identities or ambiguous/future receipt dates. Missing
effective dates remain unknown. An effective date requires an explicit,
unambiguous PDF field; KGS receipt, first registration and effective date are
separate fields.

The worker bounds downloads at 50 MiB, validates the PDF and page count with
Poppler, checks explicit cover API conflicts, and stores content-addressed
objects under `kansas/documents/sha256/`. An upload is published only after
R2 HEAD size/hash validation and a complete checksum-verified R2 read-back.
Document metadata, immutable versions, source runs, typed events and evidence
links publish together in one database transaction. Revision and retry keys are
stable. A source PDF encountered under conflicting wells requires review.

Every imported file gets `DOCUMENT_ADDED`. Explicit intent, completion report
and plugging report labels also produce `INTENT_POSTED`, `COMPLETION_RECORDED`
and `PLUGGING_RECORDED` respectively. A plugging application is not a plugging
record, and an intent is not an assertion of permit approval. A completion or
plugging event without verified effective date has unknown `occurred_at`; its
known receipt date is `published_at`. Historical evidence is marked in payloads.

The scan table is resumable, records failures and retries them after a day.
Default daily budget is 2,000 wells, prioritizing wells with permit/completion/
plug dates since 2025. Successful scans are revisited after seven days, including
PDF revision checks. The remaining inventory is scanned progressively. This is
**not a claim of a complete statewide document backfill**: KGS's confirmed
public document discovery is per well, including old wells with new filings.
Review `data/kansas/documents/latest-run.json` and scan backlog before declaring
coverage complete. `--kid KID --recheck` uses the same ingestion path for a
targeted verification. Daily source changes need no manual document matching.

## Export and deployment

`state-export-v1 full --exclude-test-ingestion --production-since 2025-06-01`
includes public R2 documents, safe structured date/identity metadata, event
evidence links and scoped monthly production. Remove the old
`KS_EXPORT_PRODUCTION_YEAR=2026` limit from export service configuration. Set
`KS_EXPORT_PRODUCTION_SINCE=2025-06-01` instead. Full exports still include every
contract entity and stable source keys. Private PDF text is never exported.

After building/restarting the Kansas API (which applies migration 0032), install:

```bash
bash scripts/install-kansas-ingestion.sh
systemctl --user start --no-block future-wells-ks-production.service
systemctl --user start --no-block future-wells-ks-wells.service
systemctl --user start --no-block future-wells-ks-documents.service
```

Timers run daily with up to 15 minutes jitter: inventory 00:10 UTC, production
05:30 UTC, documents 06:45 UTC. These are 01:10, 06:30 and 07:45 in Berlin in
November; systemd's explicit UTC schedules remain stable over daylight saving.
Enable user lingering on the host so jobs continue without a login.

Failures leave the previous public version available and are visible through
`systemctl --user status`, the journal and private job reports. An ingestion
budget is resumable and is not equivalent to complete source coverage.

Rollback: stop/disable the three timers and services, deploy the previous
binary/site and retain source snapshots, document versions and R2 objects.
Migration 0032 is additive; do not drop it to roll back application code.
