# Resume checkpoint — November 7, 2026

Resumed November 8. This is the historical pause record; see
`kansas-ingestion-live-2026-11-08.md` for current completion and background status.

User paused interactive work until tomorrow, explicitly allowing ingestion jobs
and timers to continue in the background. Do not disable scheduled ingestion.

Completed:
- Utah backend main 2fb12e2 exports unambiguous single-report well/month facts.
  Multi-formation/construct groups remain withheld to prevent double-counting.
- Full Utah export 86c702c3-5f41-4e31-8056-f30ab153202a imported successfully into
  Future Wells Co: 209,782 production records, 14,068 wells, zero failed rows.
  Shared read models refreshed. San Juan Federal 18 API 4303731423 and CWU 20
  API 4304715065 both have latest production August 2026.
- Shared state_import_v1 rebuilt on live to include already-applied migrations.
  Shared API restarted and now loads R2_BUCKET_KS=futurewellskansas.
- Kansas backend main 2de541a; frontend main 071a0b5. Existing useful map/deploy
  edits included on main; original remote edits also preserved in git stash.
- Kansas API deployed ac10b6a runtime (later script/config fixes pulled).
  Migration 0032 applied, API healthy. Four initial PDFs imported and downloaded
  from R2 successfully; later collector added more. Allan 1 document ID
  6581caac-18b6-4cb0-b582-bdb2a44c0d5b, API15163245390000, five pages.
- Production native job successfully reconciled 298,948 records June 2025–June
  2026. Code/config has since been extended to January 2025; next native refresh
  will include the remaining 117,434 January–May 2025 source records.
- Three Kansas user timers installed, persistent, lingering enabled. User
  explicitly wants background jobs running. Docs uses a resumable scan table.
- Live web currently .next-native-0315124. Final web build
  .next-native-071a0b5 is staged, not yet promoted. Copy absent old static chunks
  before atomically swapping .next and restarting the user web unit.
- Screenshot: kansasMarketing/reports/kansas-documents-live.png (before final
  document action grouping polish).

Important fixes awaiting final deployment/verification:
- First statewide inventory job OOMed at 1.4GB because old Rust code buffered all
  well rows. Main 2de541a now streams ZIP batches and materialization source rows;
  kansas_common CLI uses TWO bounded connections for streaming reads and writes.
  Remote rebuild log: /home/yaro/state-sync/kansas-repair/native-build-streaming-20261107.log.
  Before retrying, verify build finished and restart Kansas API to load new
  January-2025 public production floor. A previous OOM run may remain status
  running and need an operational failed-state recovery before the same checksum
  can be ingested again. Confirm the old process is absent first.
- KGS older pages returned a public 'direct linking disabled' page. Sending the
  official KGS catalog Referer was verified to return the actual public well page.
  The collector now supplies that navigation reference and per-well Referer for
  PDFs; --retry-failed supports immediate recovery after this parser/source fix.
  The initial broad scan was stopped for the repair, then background work resumed.
- Do not silently treat a received date as effective. Completion/plugging events
  lacking verified effective date have NULL occurred_at and known published_at.
- Production stays lease-level in Kansas; do not fabricate per-well allocations.

Tests:
- 126 Rust unit tests passed.
- Existing document importer 3, document materializer 7, production materializer
  10, exporter 11 integration tests passed on a disposable native PostgreSQL.
- New June floor/source replacement integration test passed.
- New Python publication transaction/retry/revision/identity/date tests: 4 passed.
- Parser tests: 9 passed; frontend typecheck and lint passed; native builds passed.
- Streaming ZIP bounded-batch test passed. Full streaming inventory integration
  run was paused by terminating its temporary test process; remaining checks must
  be rerun. Log /tmp/ks-wells-streaming-tests.log. Separate test DB ks_wells_test.
- Disposable native PostgreSQL on postgressh, private socket
  /home/yaro/.cache/ks-native-test-20261107/socket, port55439, cluster pg sibling.
  SSH local forward session may need recreation. DBs postgres, ks_pipeline_v2,
  ks_wells_test. Never run fixture tests against the live Kansas DB.

Outstanding:
1. Finish streaming inventory tests, deploy built runtime, recover only the
   confirmed orphan OOM ingestion run and resume native inventory refresh.
2. Confirm January 2025-forward production total 416,382 monthly source records
   and public page/API coverage; repeat normal ingestion to verify idempotence.
3. Verify broader docs scan/retry succeeds with official navigation reference,
   validate typed events and well matching, keep progressive scan running.
4. Promote final frontend, verify production/docs pages and refresh screenshot.
5. Generate a new full Kansas export with --exclude-test-ingestion and production
   since January 2025; verify nonzero documents/contents/event links, upload/import
   into Future Wells Co through its existing Kansas sync service. Last shared
   Kansas package still predates this work. Do not claim it contains these PDFs.
6. Verify shared Kansas R2 download and Utah search production badges; confirm
   Utah package acknowledgement if pending. Record final live QA in ops docs.

Native services live on SSH futurewells. Shared workspace on futurewellsco.
No containers. No secrets in logs or committed files. Continue on main and
preserve unrelated edits, notably frontend .env.example duplicates (not staged).
