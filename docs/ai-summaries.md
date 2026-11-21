# AI well summaries

Superadmin-only. Ported from the New Mexico and Oklahoma implementations (`newMexicoBackend` / `oklahomaBackend` `docs/ai-summaries.md`). The Kansas API sends recently active wells' public facts to a disposable GPU box rented on Vast.ai, which writes short summaries. They are saved in PostgreSQL as **drafts**; only approved summaries are shown publicly.

- Admin page: `https://futurewellskansas.com/api/admin/ai-summaries/app` (sign in at `/login` first; `kansasMarketing` authenticates the superadmin session and proxies `/api/admin/ai-summaries/*` to `kansasBackend`). Linked from the Kansas admin sidebar as "AI Summaries".
- Code: `src/api/ai_summaries.rs`, page `src/api/ai_summaries_page.html`
- Schema: `migrations/ai_summaries/ai_summaries.sql` (`ks_ai_summary_runs`, `ks_ai_summary_run_items`, `ks_well_ai_summaries`, `ks_ai_summary_log`). Idempotent and applied at API start, outside the numbered sqlx migrations, so an older release can still start against the database.
- Box kit: `deploy/vllm.sh`. Default model: `Qwen/Qwen3-30B-A3B-Instruct-2507` on 80+ GB cards.

## What gets sent to the box

Only facts the public well page already shows from `ks_well`, `ks_event`, `ks_well_document`, and `ks_lease_production_month`:
- identity: well name, operator (`COALESCE(operator_name_kgs_reported_current, operator_name_kgs_original)`), county, field name, lease name, well type (`well_class_raw`), plain status (`lifecycle_status` / `status_raw`), permit/spud/completion/plug dates, total depth, producing formation, formation at total depth, directional/horizontal flag, initial test rates (`ip_oil`, `ip_gas`, `ip_water`);
- up to 40 dated activity events since `activity_since` (from `ks_event` excluding lease production events, public `ks_well_document` filings, and well dates);
- **lease-level production**: from `ks_well_lease_link` (`confidence = 1`) joined to `ks_lease_production_month` (`record_kind = 'monthly'`), explicitly labeled as shared lease-level KGS production (never individual-well output).

Not sent: API numbers, `kgs_kid`, `kcc_permit`, section/township/range, coordinates, raw hashes, source keys, or download URLs. The exact JSON sent is stored with each summary (`input_json`); its SHA-256 lets a later run skip unchanged wells.

## Which wells

A candidate is an active public `ks_well` (`NOT is_tombstoned`) with a non-empty well name and county, not spudded before `activity_since` (`spud_date IS NULL OR spud_date >= activity_since`), and with at least one dated activity/filing between `activity_since` and `CURRENT_DATE`. Candidates are ordered most recent activity first, up to the run's well limit.

## Public display

- **Approve all drafts of this run** (`POST /api/admin/ai-summaries/runs/:id/approve-drafts`) approves every draft of a finished run (skipping any whose `spud_date` is before `activity_since`).
- `attach_public_summaries` middleware adds `ai_summary` to well and activity records in public `/api/ks/*` JSON responses (`/api/ks/wells/:api`, `/api/ks/counties/:slug`, `/api/ks/operators/:slug`, `/api/ks/fields/:slug`, `/api/ks/browse`).
- Web (`kansasMarketing`): a Summary panel on the well page, an excerpt on well cards, and "Recently active wells" at the top of county, operator, and field pages.
