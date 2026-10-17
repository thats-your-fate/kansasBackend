use clap::{Parser, Subcommand};
use future_wells_kansas::materialize::kcc_intents::{
    materialize_intents_from_run, unresolved_intents_report,
};
use future_wells_kansas::materialize::kcc_operators::{
    active_all_reconciliation, materialize_operators_from_run,
};
use future_wells_kansas::materialize::kcc_transfers::{
    materialize_transfers_from_run, unmatched_transfers_report,
};
use future_wells_kansas::materialize::kgs_documents::{
    self, materialize_documents_from_run, unresolved_documents_report,
};
use future_wells_kansas::materialize::kgs_fields::{
    materialize_fields_from_run, well_count_reconciliation,
};
use future_wells_kansas::materialize::kgs_leases::{
    materialize_leases_from_run, unresolved_lease_apis_report,
};
use future_wells_kansas::materialize::kgs_production::{
    coverage_summary, materialize_production_from_run, reconciliation_report,
    ReconciliationGrouping,
};
use future_wells_kansas::materialize::kgs_wells::{
    materialize_wells_from_run, unknown_status_report,
};
use future_wells_kansas::monitoring;
use future_wells_kansas::parse::kcc_intents::{self, run_intent_import};
use future_wells_kansas::parse::kcc_operators::{self, run_operator_import};
use future_wells_kansas::parse::kcc_transfers::{self, run_transfer_import};
use future_wells_kansas::parse::kgs_documents::run_document_import;
use future_wells_kansas::parse::kgs_fields::{self, run_field_import};
use future_wells_kansas::parse::kgs_leases::run_lease_import;
use future_wells_kansas::parse::kgs_production::{self, run_all_production_files};
use future_wells_kansas::parse::kgs_wells::{
    run_arcgis_snapshot_import, run_zip_snapshot_import, ImportOptions, ImportStatus,
};
use future_wells_kansas::source::retry::RetryPolicy;
use future_wells_kansas::{config::Config, constants, db, source::arcgis, source::registry};
use uuid::Uuid;

#[derive(Parser)]
#[command(
    name = "ks_ingest",
    about = "Kansas KGS/KCC ingestion CLI",
    long_about = "Kansas KGS/KCC ingestion CLI. Every subcommand below (KS-03 through \
KS-10, see future_wells_kansas_backend_marketing_codex_tasks.md §11 for the \
authoritative task history) performs real work — snapshot/materialize/report \
commands per dataset (wells, operators, drilling intents, approved transfers, \
fields, leases, monthly production), plus status/sources/source-registry/ \
resolve-arcgis. KS-05 through KS-07 are unverified against their live \
service (kcc.ks.gov is unreachable from this development environment); \
KS-03/04/08/09/10 are verified against the real KGS service."
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Check configuration and database connectivity (implemented).
    Status,
    /// List the official source registry with its current configured URL (implemented; read-only).
    Sources,
    /// List the KS-02 ingestible-source registry (ks_source) with its data-use policy (implemented; read-only).
    SourceRegistry,
    /// Resolve the KGS wells ArcGIS item ID to its current service URL (implemented).
    ResolveArcgis,
    /// KS-03: run the KGS statewide well snapshot importer (ArcGIS preferred, ZIP fallback on
    /// resolve/metadata failure). Stages raw rows only — no normalization/materialization yet.
    SnapshotWells {
        /// Testing/ops escape hatch: caps how many object IDs are processed via the ArcGIS path.
        /// Omit for a real full run.
        #[arg(long)]
        limit_object_ids: Option<usize>,
        /// Fraction the extracted count may deviate from the layer's estimate before the run is
        /// rejected.
        #[arg(long, default_value_t = 0.02)]
        count_tolerance_ratio: f64,
    },
    /// KS-04: materialize a completed snapshot-wells run's staged rows into ks_well.
    MaterializeWells {
        /// The ingestion_run_id printed by `snapshot-wells` (must be a `completed` run).
        run_id: Uuid,
    },
    /// KS-04: list distinct (well_class, status, status2) combinations currently unmapped to a
    /// known lifecycle status, for human review — never silently treated as inactive.
    LifecycleQaReport,
    /// KS-05: discover and import the KCC active/all operator downloads.
    /// **Unverified against the live service** — kcc.ks.gov is unreachable
    /// from this environment; see docs/architecture/kansas-source-boundary.md.
    SnapshotOperators,
    /// KS-05: materialize a completed snapshot-operators run's staged rows into ks_operator.
    MaterializeOperators {
        /// The ingestion_run_id printed by `snapshot-operators` (must be a `completed` run).
        run_id: Uuid,
    },
    /// KS-05: print the active/all reconciliation report for a run.
    OperatorsReconciliationReport {
        /// The ingestion_run_id printed by `snapshot-operators`.
        run_id: Uuid,
    },
    /// KS-06: snapshot the KCC rolling 365-day drilling-intent window.
    /// **Unverified against the live service** — kcc.ks.gov is unreachable
    /// from this environment; see docs/ops/ks-06-kcc-intents-unverified.md.
    SnapshotIntents,
    /// KS-06: materialize a completed snapshot-intents run into permanent ks_drilling_intent rows,
    /// link each to a well, and emit INTENT_POSTED events.
    MaterializeIntents {
        /// The ingestion_run_id printed by `snapshot-intents` (must be a `completed` run).
        run_id: Uuid,
    },
    /// KS-06: run snapshot-intents then materialize-intents in one step, for cron/systemd use.
    /// Exits non-zero if the run is rejected, so a timer's Restart=on-failure retries later.
    IngestIntentsDaily,
    /// KS-06: list unresolved drilling intents (no well match at any tier) — the
    /// "unresolved-match dashboard" acceptance criterion, as a queryable report.
    IntentsUnresolvedReport {
        #[arg(long, default_value_t = 50)]
        limit: i64,
    },
    /// KS-07: snapshot the KCC rolling 90-day approved-transfer window.
    /// **Unverified against the live service** — kcc.ks.gov is unreachable
    /// from this environment; see docs/ops/ks-07-kcc-transfers-unverified.md.
    SnapshotTransfers,
    /// KS-07: materialize a completed snapshot-transfers run into immutable ks_operator_transfer
    /// rows, link each to zero or more wells, and emit OPERATOR_TRANSFER_APPROVED events.
    MaterializeTransfers {
        /// The ingestion_run_id printed by `snapshot-transfers` (must be a `completed` run).
        run_id: Uuid,
    },
    /// KS-07: run snapshot-transfers then materialize-transfers in one step, for cron/systemd use.
    IngestTransfersDaily,
    /// KS-07: list transfers with zero well links — "unmatched transfers remain searchable
    /// internally."
    TransfersUnmatchedReport {
        #[arg(long, default_value_t = 50)]
        limit: i64,
    },
    /// KS-08: import the KGS field GIS layer plus active/abandoned field-production files.
    /// Confirmed live against the real KGS field data 2026-09-05.
    SnapshotFields {
        /// Testing/ops escape hatch: caps how many object IDs are processed via the ArcGIS path.
        #[arg(long)]
        limit_object_ids: Option<usize>,
        #[arg(long, default_value_t = 0.02)]
        count_tolerance_ratio: f64,
    },
    /// KS-08: materialize a completed snapshot-fields run into ks_field.
    MaterializeFields { run_id: Uuid },
    /// KS-08: reconcile each field's reported max well counts against materialized ks_well rows.
    FieldWellCountReconciliationReport {
        #[arg(long, default_value_t = 50)]
        limit: i64,
    },
    /// KS-09: import the KGS all-leases archive into the lease dimension.
    /// Confirmed live against the real KGS lease data 2026-09-05.
    SnapshotLeases,
    /// KS-09: materialize a completed snapshot-leases run into ks_lease + ks_well_lease_link.
    MaterializeLeases { run_id: Uuid },
    /// KS-09: list production API numbers from the lease file that matched no ks_well.
    UnresolvedLeaseApisReport {
        #[arg(long, default_value_t = 50)]
        limit: i64,
    },
    /// KS-10: import all 8 configured KGS monthly lease-production decade archives
    /// (oil/gas x 2020-present/2010-2019/2000-2009/1990-1999). Confirmed live 2026-09-05.
    SnapshotProduction,
    /// KS-10: materialize a completed snapshot-production run into ks_lease_production_month.
    MaterializeProduction { run_id: Uuid },
    /// KS-10: print latest_oil_month/latest_gas_month/last_successful_import coverage metadata.
    ProductionCoverageReport,
    /// KS-10: reconcile summed monthly production against KGS's own reported yearly totals.
    ProductionReconciliationReport {
        #[arg(long, value_enum, default_value_t = ReconciliationGroupingArg::State)]
        group_by: ReconciliationGroupingArg,
        #[arg(long, default_value_t = 0.02)]
        tolerance_ratio: f64,
        #[arg(long, default_value_t = 50)]
        limit: i64,
    },
    /// KS-15: fetch and stage one well's document metadata evidence from KGS's real per-well
    /// "Qualified Well Database" detail page. Per-well by design — no bulk document-listing
    /// endpoint was confirmed live; see docs/ops/ks-15-kgs-documents-live-smoke.md.
    SnapshotDocumentsForWell {
        kid: String,
        #[arg(long)]
        api_number: Option<String>,
    },
    /// KS-15: materialize a completed snapshot-documents-for-well run into ks_well_document.
    MaterializeDocuments { run_id: Uuid },
    /// KS-15: list document metadata records with no resolved well relationship.
    DocumentsUnresolvedReport {
        #[arg(long, default_value_t = 50)]
        limit: i64,
    },
    /// KS-15: queue a specific public document for download/extraction (never a restricted,
    /// login-required, or otherwise non-public one — enforced by the database itself).
    EnqueueDocumentDownload { document_id: Uuid },
    /// KS-15: list documents currently queued for download.
    QueuedDocumentsReport {
        #[arg(long, default_value_t = 50)]
        limit: i64,
    },
    /// KS-17: print the full source-health/data-quality report (freshness, row-count and schema/
    /// content-type changes, rejected-run anomalies, well data quality, link confidence, production
    /// reconciliation, document-queue summary).
    HealthReport,
    /// KS-17: the two hard publication gates (restricted-year production rows; personal-contact-field
    /// leakage into an exported/indexed column). Exits non-zero if either finds a violation.
    PublicationGateCheck,
    /// KS-14: mark an ingestion run as a historical baseline/backfill before materializing it,
    /// so its event-emitting materializer (wells/production/intents/transfers) suppresses events
    /// for historical (pre-live-monitoring-cutoff) dates rather than flooding the event log.
    MarkRunBackfill { run_id: Uuid },
    /// KS-14: the "deliberate historical timeline job" escape hatch — also publish a
    /// backfill-marked run's events despite their historical dates. Meaningless unless the run is
    /// also marked with `mark-run-backfill`.
    MarkRunHistorical { run_id: Uuid },
    /// KS-12/KS-13: produce/inspect `state-export-v1` bundles for the shared Future Wells Co
    /// platform (`futureBackend`). Baseline/full exports only — see src/state_export/exporter.rs's
    /// module doc for why incremental is not yet supported.
    #[command(name = "state-export-v1", subcommand)]
    StateExportV1(StateExportV1Command),
}

#[derive(Subcommand)]
enum StateExportV1Command {
    /// Check that every table this exporter reads from exists.
    Preflight,
    /// Export the current full snapshot of every entity into a new state-export-v1 package.
    Full {
        #[arg(long, default_value = "future-wells-co")]
        consumer: String,
        #[arg(long)]
        output_dir: std::path::PathBuf,
        #[arg(long, default_value_t = 50_000)]
        max_records_per_part: u64,
        #[arg(long, default_value_t = 134_217_728)]
        max_uncompressed_bytes_per_part: u64,
        #[arg(long, default_value = "zstd")]
        compression: String,
        #[arg(long, default_value_t = 3)]
        compression_level: i32,
        /// Exclude fixture/test ingestion runs using session-local read views.
        #[arg(long)]
        exclude_test_ingestion: bool,
        #[arg(long)]
        json: bool,
    },
    /// Verify a package's manifest/part checksums without importing it.
    Verify {
        path: std::path::PathBuf,
        #[arg(long)]
        json: bool,
    },
    /// Mark a previously produced export as acknowledged (imported) by a consumer.
    Acknowledge {
        #[arg(long, default_value = "future-wells-co")]
        consumer: String,
        #[arg(long)]
        export_id: String,
        #[arg(long)]
        json: bool,
    },
    /// List recent export runs and their acknowledgement status for a consumer.
    Status {
        #[arg(long, default_value = "future-wells-co")]
        consumer: String,
        #[arg(long)]
        json: bool,
    },
    /// Remove abandoned `.partial` export directories older than the given age.
    CleanupPartials {
        #[arg(long)]
        output_dir: std::path::PathBuf,
        #[arg(long, default_value_t = 86_400)]
        older_than_seconds: u64,
        #[arg(long)]
        json: bool,
    },
}

#[derive(Clone, Copy, clap::ValueEnum)]
enum ReconciliationGroupingArg {
    State,
    County,
    Field,
}

impl From<ReconciliationGroupingArg> for ReconciliationGrouping {
    fn from(value: ReconciliationGroupingArg) -> Self {
        match value {
            ReconciliationGroupingArg::State => ReconciliationGrouping::State,
            ReconciliationGroupingArg::County => ReconciliationGrouping::County,
            ReconciliationGroupingArg::Field => ReconciliationGrouping::Field,
        }
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenvy::dotenv().ok();
    let cli = Cli::parse();
    let cfg = Config::from_env()?;

    match cli.command {
        Command::Status => {
            let pool = db::connect(&cfg.database_url).await?;
            sqlx::query("SELECT 1").execute(&pool).await?;
            println!("ok: database connection succeeded");
            println!(
                "state: {} ({})",
                constants::STATE_NAME,
                constants::STATE_KEY
            );
            println!("api port: {}", cfg.api_port);
        }
        Command::Sources => {
            for source in constants::SOURCES {
                let url = cfg.source_urls.get(source.id).unwrap_or("<unconfigured>");
                println!("{:<28} {:?}  {}", source.id, source.category, url);
            }
        }
        Command::SourceRegistry => {
            let pool = db::connect(&cfg.database_url).await?;
            let sources = registry::list_sources(&pool).await?;
            for source in sources {
                println!(
                    "{:<26} {:<4} public={:<5} min_year={:<6} contact_fields={:<5} docs={:<20} cadence={}",
                    source.code,
                    source.agency,
                    source.allowed_for_public_product,
                    source
                        .minimum_allowed_year
                        .map(|y| y.to_string())
                        .unwrap_or_else(|| "-".to_string()),
                    source.contains_restricted_contact_fields,
                    source.document_download_policy,
                    source.expected_cadence
                );
            }
        }
        Command::ResolveArcgis => {
            let item_id = &cfg.source_urls.kgs_wells_arcgis_item_id;
            match arcgis::resolve_service_url(item_id).await {
                Ok(url) => println!("resolved (item {item_id}): {url}"),
                Err(err) => {
                    eprintln!("warning: ArcGIS item resolution failed: {err}");
                    println!(
                        "fallback (pinned KGS_WELLS_ARCGIS_LAYER): {}",
                        cfg.source_urls.kgs_wells_arcgis_layer
                    );
                }
            }
        }
        Command::SnapshotWells {
            limit_object_ids,
            count_tolerance_ratio,
        } => {
            let pool = db::connect(&cfg.database_url).await?;
            let client = reqwest::Client::new();
            let retry_policy = RetryPolicy::default();
            let options = ImportOptions {
                count_tolerance_ratio,
                retry_policy,
                limit_object_ids,
            };

            let item_id = &cfg.source_urls.kgs_wells_arcgis_item_id;
            let arcgis_layer = match arcgis::resolve_service_url(item_id).await {
                Ok(service_root) => format!("{service_root}/0"),
                Err(err) => {
                    eprintln!("warning: ArcGIS item resolution failed ({err}); falling back to pinned layer URL");
                    cfg.source_urls.kgs_wells_arcgis_layer.clone()
                }
            };

            match run_arcgis_snapshot_import(&pool, &client, &arcgis_layer, options).await {
                Ok(outcome) => {
                    print_outcome("arcgis", &outcome);
                    if outcome.status == ImportStatus::Rejected {
                        std::process::exit(1);
                    }
                }
                Err(err) => {
                    eprintln!("ArcGIS import failed ({err}); falling back to KGS_WELLS_ZIP");
                    let dest = std::env::temp_dir().join("ks_wells_fallback.zip");
                    let outcome = run_zip_snapshot_import(
                        &pool,
                        &client,
                        &cfg.source_urls.kgs_wells_static_zip,
                        &dest,
                        retry_policy,
                    )
                    .await?;
                    print_outcome("zip", &outcome);
                    if outcome.status == ImportStatus::Rejected {
                        std::process::exit(1);
                    }
                }
            }
        }
        Command::MaterializeWells { run_id } => {
            let pool = db::connect(&cfg.database_url).await?;
            let outcome = materialize_wells_from_run(&pool, run_id).await?;
            println!("created: {}", outcome.created);
            println!("updated: {}", outcome.updated);
            println!("tombstoned: {}", outcome.tombstoned);
            println!("missing_incremented: {}", outcome.missing_incremented);
            println!("rejected_no_identity: {}", outcome.rejected_no_identity);
            println!("unknown_status_count: {}", outcome.unknown_status_count);
        }
        Command::LifecycleQaReport => {
            let pool = db::connect(&cfg.database_url).await?;
            let report = unknown_status_report(&pool).await?;
            if report.is_empty() {
                println!("no unmapped (well_class, status, status2) combinations found");
            }
            for entry in report {
                println!(
                    "{:>6}  well_class={:?} status={:?} status2={:?}",
                    entry.well_count, entry.well_class_raw, entry.status_raw, entry.status2_raw
                );
            }
        }
        Command::SnapshotOperators => {
            let pool = db::connect(&cfg.database_url).await?;
            let client = reqwest::Client::new();
            let outcome = run_operator_import(
                &pool,
                &client,
                &cfg.source_urls.kcc_operators,
                kcc_operators::ImportOptions::default(),
            )
            .await?;
            println!("run_id: {}", outcome.run_id);
            println!("status: {:?}", outcome.status);
            println!("active_count: {}", outcome.active_count);
            println!("all_count: {}", outcome.all_count);
            println!(
                "manifest: {}",
                serde_json::to_string_pretty(&outcome.manifest).unwrap_or_default()
            );
        }
        Command::MaterializeOperators { run_id } => {
            let pool = db::connect(&cfg.database_url).await?;
            let outcome = materialize_operators_from_run(&pool, run_id).await?;
            println!("created: {}", outcome.created);
            println!("updated: {}", outcome.updated);
            println!("rejected_no_identity: {}", outcome.rejected_no_identity);
        }
        Command::OperatorsReconciliationReport { run_id } => {
            let pool = db::connect(&cfg.database_url).await?;
            let report = active_all_reconciliation(&pool, run_id).await?;
            println!(
                "{}",
                serde_json::to_string_pretty(&report).unwrap_or_default()
            );
        }
        Command::SnapshotIntents => {
            let pool = db::connect(&cfg.database_url).await?;
            let client = reqwest::Client::new();
            let outcome = run_intent_import(
                &pool,
                &client,
                &cfg.source_urls.kcc_intents,
                kcc_intents::ImportOptions::default(),
            )
            .await?;
            println!("run_id: {}", outcome.run_id);
            println!("status: {:?}", outcome.status);
            println!("staged_count: {}", outcome.staged_count);
            println!("rejected_count: {}", outcome.rejected_count);
            println!(
                "manifest: {}",
                serde_json::to_string_pretty(&outcome.manifest).unwrap_or_default()
            );
            if outcome.status == kcc_intents::ImportStatus::Rejected {
                std::process::exit(1);
            }
        }
        Command::MaterializeIntents { run_id } => {
            let pool = db::connect(&cfg.database_url).await?;
            let outcome = materialize_intents_from_run(&pool, run_id).await?;
            println!("created: {}", outcome.created);
            println!("updated: {}", outcome.updated);
            println!("exact_matches: {}", outcome.exact_matches);
            println!("probabilistic_matches: {}", outcome.probabilistic_matches);
            println!("unresolved: {}", outcome.unresolved);
            println!("events_emitted: {}", outcome.events_emitted);
        }
        Command::IngestIntentsDaily => {
            let pool = db::connect(&cfg.database_url).await?;
            let client = reqwest::Client::new();
            let outcome = run_intent_import(
                &pool,
                &client,
                &cfg.source_urls.kcc_intents,
                kcc_intents::ImportOptions::default(),
            )
            .await?;
            println!("snapshot status: {:?}", outcome.status);
            if outcome.status == kcc_intents::ImportStatus::Rejected {
                eprintln!(
                    "snapshot-intents rejected: {}",
                    serde_json::to_string_pretty(&outcome.manifest).unwrap_or_default()
                );
                std::process::exit(1);
            }
            if outcome.status == kcc_intents::ImportStatus::Completed {
                let materialize_outcome =
                    materialize_intents_from_run(&pool, outcome.run_id).await?;
                println!("{materialize_outcome:?}");
            }
        }
        Command::IntentsUnresolvedReport { limit } => {
            let pool = db::connect(&cfg.database_url).await?;
            let report = unresolved_intents_report(&pool, limit).await?;
            if report.is_empty() {
                println!("no unresolved intents");
            }
            for entry in report {
                println!(
                    "{}  file={} county={:?} license={:?} name={:?} first_seen={}",
                    entry.id,
                    entry.source_filename,
                    entry.county,
                    entry.operator_license,
                    entry.reported_lease_or_well_name,
                    entry.first_observed_at
                );
            }
        }
        Command::SnapshotTransfers => {
            let pool = db::connect(&cfg.database_url).await?;
            let client = reqwest::Client::new();
            let outcome = run_transfer_import(
                &pool,
                &client,
                &cfg.source_urls.kcc_transfer_results,
                kcc_transfers::ImportOptions::default(),
            )
            .await?;
            println!("run_id: {}", outcome.run_id);
            println!("status: {:?}", outcome.status);
            println!("staged_count: {}", outcome.staged_count);
            println!(
                "manifest: {}",
                serde_json::to_string_pretty(&outcome.manifest).unwrap_or_default()
            );
            if outcome.status == kcc_transfers::ImportStatus::Rejected {
                std::process::exit(1);
            }
        }
        Command::MaterializeTransfers { run_id } => {
            let pool = db::connect(&cfg.database_url).await?;
            let outcome = materialize_transfers_from_run(&pool, run_id).await?;
            println!("created: {}", outcome.created);
            println!("updated: {}", outcome.updated);
            println!("well_links_created: {}", outcome.well_links_created);
            println!("unmatched: {}", outcome.unmatched);
            println!("events_emitted: {}", outcome.events_emitted);
        }
        Command::IngestTransfersDaily => {
            let pool = db::connect(&cfg.database_url).await?;
            let client = reqwest::Client::new();
            let outcome = run_transfer_import(
                &pool,
                &client,
                &cfg.source_urls.kcc_transfer_results,
                kcc_transfers::ImportOptions::default(),
            )
            .await?;
            println!("snapshot status: {:?}", outcome.status);
            if outcome.status == kcc_transfers::ImportStatus::Rejected {
                eprintln!(
                    "snapshot-transfers rejected: {}",
                    serde_json::to_string_pretty(&outcome.manifest).unwrap_or_default()
                );
                std::process::exit(1);
            }
            if outcome.status == kcc_transfers::ImportStatus::Completed {
                let materialize_outcome =
                    materialize_transfers_from_run(&pool, outcome.run_id).await?;
                println!("{materialize_outcome:?}");
            }
        }
        Command::TransfersUnmatchedReport { limit } => {
            let pool = db::connect(&cfg.database_url).await?;
            let report = unmatched_transfers_report(&pool, limit).await?;
            if report.is_empty() {
                println!("no unmatched transfers");
            }
            for entry in report {
                println!(
                    "{}  past_license={:?} new_license={:?} label={:?} county={:?} first_seen={}",
                    entry.id,
                    entry.past_operator_license,
                    entry.new_operator_license,
                    entry.lease_or_well_label,
                    entry.county,
                    entry.first_observed_at
                );
            }
        }
        Command::SnapshotLeases => {
            let pool = db::connect(&cfg.database_url).await?;
            let client = reqwest::Client::new();
            let dest = std::env::temp_dir().join("ks_all_leases.zip");
            let outcome = run_lease_import(
                &pool,
                &client,
                &cfg.source_urls.kgs_all_leases_zip,
                &dest,
                RetryPolicy::default(),
            )
            .await?;
            println!("run_id: {}", outcome.run_id);
            println!("status: {:?}", outcome.status);
            println!("staged_count: {}", outcome.staged_count);
            println!("rejected_count: {}", outcome.rejected_count);
            println!(
                "manifest: {}",
                serde_json::to_string_pretty(&outcome.manifest).unwrap_or_default()
            );
            if outcome.status == future_wells_kansas::parse::kgs_leases::ImportStatus::Rejected {
                std::process::exit(1);
            }
        }
        Command::MaterializeLeases { run_id } => {
            let pool = db::connect(&cfg.database_url).await?;
            let outcome = materialize_leases_from_run(&pool, run_id).await?;
            println!("leases_created: {}", outcome.leases_created);
            println!("leases_updated: {}", outcome.leases_updated);
            println!("links_created: {}", outcome.links_created);
            println!("unresolved_api_count: {}", outcome.unresolved_api_count);
        }
        Command::UnresolvedLeaseApisReport { limit } => {
            let pool = db::connect(&cfg.database_url).await?;
            let report = unresolved_lease_apis_report(&pool, limit).await?;
            if report.is_empty() {
                println!("no unresolved production APIs");
            }
            for entry in report {
                println!("{}  {}", entry.created_at, entry.raw_data);
            }
        }
        Command::SnapshotProduction => {
            let pool = db::connect(&cfg.database_url).await?;
            let client = reqwest::Client::new();
            let dest_dir = std::env::temp_dir();
            let options = kgs_production::ImportOptions {
                legal_review_override_enabled: cfg.pre_1990_legal_review_override_enabled,
                ..Default::default()
            };
            let results = run_all_production_files(
                &pool,
                &client,
                &cfg.source_urls.kgs_production_archive_base,
                &dest_dir,
                options,
            )
            .await;
            let mut any_failed = false;
            for (filename, result) in results {
                match result {
                    Ok(outcome) => {
                        println!(
                            "{filename:<32} run_id={} status={:?} staged={} rejected={} pre_1990_blocked={}",
                            outcome.run_id, outcome.status, outcome.staged_count, outcome.rejected_count, outcome.pre_1990_blocked_count
                        );
                        if outcome.status == kgs_production::ImportStatus::Rejected {
                            any_failed = true;
                        }
                    }
                    Err(err) => {
                        eprintln!("{filename:<32} FAILED: {err}");
                        any_failed = true;
                    }
                }
            }
            if any_failed {
                std::process::exit(1);
            }
        }
        Command::MaterializeProduction { run_id } => {
            let pool = db::connect(&cfg.database_url).await?;
            let outcome = materialize_production_from_run(&pool, run_id).await?;
            println!("created: {}", outcome.created);
            println!("updated: {}", outcome.updated);
            println!("skipped_unmapped_month: {}", outcome.skipped_unmapped_month);
            println!(
                "skipped_product_mismatch: {}",
                outcome.skipped_product_mismatch
            );
        }
        Command::ProductionCoverageReport => {
            let pool = db::connect(&cfg.database_url).await?;
            let summary = coverage_summary(&pool).await?;
            println!(
                "latest_oil: {:?}-{:?}",
                summary.latest_oil_year, summary.latest_oil_month
            );
            println!(
                "latest_gas: {:?}-{:?}",
                summary.latest_gas_year, summary.latest_gas_month
            );
            println!(
                "last_successful_import: {:?}",
                summary.last_successful_import
            );
        }
        Command::ProductionReconciliationReport {
            group_by,
            tolerance_ratio,
            limit,
        } => {
            let pool = db::connect(&cfg.database_url).await?;
            let report =
                reconciliation_report(&pool, group_by.into(), tolerance_ratio, limit).await?;
            for row in report {
                println!(
                    "{:<20} {:<4} {} monthly_sum={:.2} reported_yearly_total={:?} diff={:?} within_tolerance={:?}",
                    row.group_value.as_deref().unwrap_or("(none)"),
                    row.product,
                    row.year,
                    row.monthly_sum,
                    row.reported_yearly_total,
                    row.difference,
                    row.within_tolerance
                );
            }
        }
        Command::SnapshotFields {
            limit_object_ids,
            count_tolerance_ratio,
        } => {
            let pool = db::connect(&cfg.database_url).await?;
            let client = reqwest::Client::new();
            let active_file_urls =
                constants::field_active_file_urls(&cfg.source_urls.kgs_field_files_base);
            let options = kgs_fields::ImportOptions {
                retry_policy: RetryPolicy::default(),
                count_tolerance_ratio,
                limit_object_ids,
            };
            let outcome = run_field_import(
                &pool,
                &client,
                &cfg.source_urls.kgs_fields_arcgis_layer,
                &active_file_urls,
                &cfg.source_urls.kgs_field_abandoned_file,
                options,
            )
            .await?;
            println!("run_id: {}", outcome.run_id);
            println!("status: {:?}", outcome.status);
            println!("arcgis_count: {}", outcome.arcgis_count);
            println!("active_file_row_count: {}", outcome.active_file_row_count);
            println!(
                "abandoned_file_row_count: {}",
                outcome.abandoned_file_row_count
            );
            println!(
                "manifest: {}",
                serde_json::to_string_pretty(&outcome.manifest).unwrap_or_default()
            );
            if outcome.status == kgs_fields::ImportStatus::Rejected {
                std::process::exit(1);
            }
        }
        Command::MaterializeFields { run_id } => {
            let pool = db::connect(&cfg.database_url).await?;
            let outcome = materialize_fields_from_run(&pool, run_id).await?;
            println!("created: {}", outcome.created);
            println!("updated: {}", outcome.updated);
            println!("geometry_invalid_count: {}", outcome.geometry_invalid_count);
        }
        Command::FieldWellCountReconciliationReport { limit } => {
            let pool = db::connect(&cfg.database_url).await?;
            let report = well_count_reconciliation(&pool, limit).await?;
            for entry in report {
                println!(
                    "{:<40} reported_oil_wells={:?} reported_gas_wells={:?} materialized_well_count={}",
                    entry.official_field_name, entry.reported_max_oil_wells, entry.reported_max_gas_wells, entry.materialized_well_count
                );
            }
        }
        Command::SnapshotDocumentsForWell { kid, api_number } => {
            let pool = db::connect(&cfg.database_url).await?;
            let client = reqwest::Client::new();
            let outcome = run_document_import(
                &pool,
                &client,
                &cfg.source_urls.kgs_well_detail_page,
                &kid,
                api_number.as_deref(),
            )
            .await?;
            println!("run_id: {}", outcome.run_id);
            println!("status: {:?}", outcome.status);
            println!("staged_count: {}", outcome.staged_count);
        }
        Command::MaterializeDocuments { run_id } => {
            let pool = db::connect(&cfg.database_url).await?;
            let outcome = materialize_documents_from_run(&pool, run_id).await?;
            println!("created: {}", outcome.created);
            println!("updated: {}", outcome.updated);
            println!("events_emitted: {}", outcome.events_emitted);
            println!("unresolved: {}", outcome.unresolved);
        }
        Command::DocumentsUnresolvedReport { limit } => {
            let pool = db::connect(&cfg.database_url).await?;
            let report = unresolved_documents_report(&pool, limit).await?;
            for entry in report {
                println!(
                    "{:<38} {:<20} {:<12} kid={:?} api={:?}",
                    entry.id,
                    entry.document_source,
                    entry.source_document_id,
                    entry.kgs_kid,
                    entry.api_number_raw
                );
            }
        }
        Command::EnqueueDocumentDownload { document_id } => {
            let pool = db::connect(&cfg.database_url).await?;
            kgs_documents::enqueue_for_download(&pool, document_id).await?;
            println!("document {document_id} queued for download");
        }
        Command::QueuedDocumentsReport { limit } => {
            let pool = db::connect(&cfg.database_url).await?;
            let report = kgs_documents::next_queued_documents(&pool, limit).await?;
            for entry in report {
                println!(
                    "{:<38} {:<20} {:<12} {}",
                    entry.id, entry.document_source, entry.document_type, entry.source_url
                );
            }
        }
        Command::HealthReport => {
            let pool = db::connect(&cfg.database_url).await?;

            println!("=== source freshness ===");
            for f in monitoring::source_freshness_report(&pool).await? {
                println!(
                    "{:<26} cadence={:<10} last_success={:?} stale={} most_recent_status={:?} failed={}",
                    f.source_code,
                    f.expected_cadence,
                    f.last_successful_import,
                    f.is_stale(),
                    f.most_recent_run_status,
                    f.is_failed()
                );
            }

            println!("=== row-count change (latest vs previous completed run) ===");
            for r in monitoring::row_count_change_report(&pool).await? {
                println!(
                    "{:<26} current={:?} previous={:?} change_ratio={:?}",
                    r.source_code,
                    r.current_row_count,
                    r.previous_row_count,
                    r.change_ratio()
                );
            }

            println!("=== schema fingerprint changes ===");
            for s in monitoring::schema_fingerprint_change_report(&pool).await? {
                println!(
                    "{:<26} changed={:?} current={:?} previous={:?}",
                    s.source_code,
                    s.changed(),
                    s.current_fingerprint,
                    s.previous_fingerprint
                );
            }

            println!("=== content-type changes ===");
            for c in monitoring::content_type_change_report(&pool).await? {
                println!(
                    "{:<26} changed={:?} current={:?} previous={:?}",
                    c.source_code,
                    c.changed(),
                    c.current_content_type,
                    c.previous_content_type
                );
            }

            println!("=== rejected-run anomalies (last 30 days) ===");
            for r in monitoring::rejected_run_anomalies_report(&pool, 30).await? {
                println!(
                    "{:<26} rejected_run_count={}",
                    r.source_code, r.rejected_run_count
                );
            }

            println!("=== well data quality ===");
            let wq = monitoring::well_data_quality_report(&pool).await?;
            println!("{wq:#?}");

            println!("=== link confidence ===");
            for l in monitoring::link_confidence_report(&pool).await? {
                println!("{:<20} {:<20} count={}", l.category, l.method, l.count);
            }

            println!("=== production reconciliation (out of tolerance, statewide) ===");
            println!(
                "count={}",
                monitoring::production_reconciliation_out_of_tolerance_count(&pool, 0.02).await?
            );

            println!("=== document type distribution ===");
            for d in monitoring::document_type_distribution(&pool).await? {
                println!("{:<20} count={}", d.document_type, d.count);
            }
        }
        Command::PublicationGateCheck => {
            let pool = db::connect(&cfg.database_url).await?;
            let result = monitoring::publication_gate(&pool).await?;
            for v in &result.restricted_year_violations {
                eprintln!(
                    "RESTRICTED YEAR VIOLATION: production row {} (lease_kid={}, year={})",
                    v.id, v.lease_kid, v.year
                );
            }
            for h in &result.personal_field_leakage {
                eprintln!(
                    "PERSONAL FIELD LEAKAGE: {}.{} row {} — {:?}",
                    h.table_name, h.column_name, h.row_id, h.matched_excerpt
                );
            }
            if result.passes() {
                println!("publication gate: PASS");
            } else {
                println!(
                    "publication gate: FAIL ({} restricted-year violations, {} personal-field leaks)",
                    result.restricted_year_violations.len(),
                    result.personal_field_leakage.len()
                );
                std::process::exit(1);
            }
        }
        Command::MarkRunBackfill { run_id } => {
            let pool = db::connect(&cfg.database_url).await?;
            future_wells_kansas::source::ingestion_run::mark_as_backfill(&pool, run_id).await?;
            println!("run {run_id} marked as a baseline backfill");
        }
        Command::MarkRunHistorical { run_id } => {
            let pool = db::connect(&cfg.database_url).await?;
            future_wells_kansas::source::ingestion_run::mark_as_force_historical(&pool, run_id)
                .await?;
            println!("run {run_id} marked to force-publish its historical events");
        }
        Command::StateExportV1(command) => run_state_export_v1(&cfg, command).await?,
    }

    Ok(())
}

async fn run_state_export_v1(cfg: &Config, command: StateExportV1Command) -> anyhow::Result<()> {
    use future_wells_kansas::state_export::{
        self,
        contract::{ExportModeV1, SCHEMA_VERSION, SOURCE_CODE},
        package::Compression,
        ExportOptions,
    };

    match command {
        StateExportV1Command::Preflight => {
            let pool = db::connect(&cfg.database_url).await?;
            state_export::preflight(&pool).await?;
            println!("state-export-v1 preflight passed");
        }
        StateExportV1Command::Full {
            consumer,
            output_dir,
            max_records_per_part,
            max_uncompressed_bytes_per_part,
            compression,
            compression_level,
            exclude_test_ingestion,
            json,
        } => {
            let pool = db::connect(&cfg.database_url).await?;
            let pool = if exclude_test_ingestion {
                let options = (*pool.connect_options()).clone();
                pool.close().await;
                sqlx::postgres::PgPoolOptions::new()
                    .max_connections(1)
                    .after_connect(|connection, _| {
                        Box::pin(async move {
                            use sqlx::Executor;
                            connection
                                .execute(include_str!("../state_export/production_views.sql"))
                                .await?;
                            Ok(())
                        })
                    })
                    .connect_with(options)
                    .await?
            } else {
                pool
            };
            let compression = match compression.as_str() {
                "zstd" => Compression::Zstd,
                "none" => Compression::None,
                other => anyhow::bail!("--compression must be zstd or none, got {other}"),
            };
            let options = ExportOptions {
                consumer,
                output_dir,
                max_records_per_part,
                max_uncompressed_bytes_per_part,
                compression,
                compression_level,
            };
            let summary = state_export::export_state_v1(&pool, ExportModeV1::Full, options).await?;
            if json {
                let records_by_entity =
                    summary
                        .entities
                        .iter()
                        .fold(serde_json::Map::new(), |mut map, entity| {
                            map.insert(
                                entity.entity_type.clone(),
                                serde_json::json!(entity.records_written),
                            );
                            map
                        });
                println!(
                    "{}",
                    serde_json::to_string(&serde_json::json!({
                        "command_version": "state-sync-cli-v1",
                        "success": true,
                        "state_code": "KS",
                        "source_code": SOURCE_CODE,
                        "schema_version": SCHEMA_VERSION,
                        "export_id": summary.export_id,
                        "mode": summary.mode,
                        "snapshot_at": summary.snapshot_at,
                        "manifest_checksum": summary.manifest_checksum,
                        "package_path": summary.output_path,
                        "records_total": summary.total_records,
                        "compressed_bytes": summary.total_compressed_bytes,
                        "elapsed_ms": summary.elapsed.as_millis(),
                        "records_by_entity": records_by_entity,
                    }))?
                );
            } else {
                println!(
                    "state-export-v1 complete: export_id={} records={} compressed_bytes={} elapsed_ms={} output={} manifest_sha256={}",
                    summary.export_id,
                    summary.total_records,
                    summary.total_compressed_bytes,
                    summary.elapsed.as_millis(),
                    summary.output_path.display(),
                    summary.manifest_checksum
                );
                for entity in &summary.entities {
                    println!(
                        "  {} records={} parts={} compressed_bytes={}",
                        entity.entity_type,
                        entity.records_written,
                        entity.part_count,
                        entity.compressed_bytes
                    );
                }
            }
        }
        StateExportV1Command::Verify { path, json } => {
            let manifest = state_export::verify_package(&path)?;
            if json {
                println!(
                    "{}",
                    serde_json::to_string(&serde_json::json!({
                        "command_version": "state-sync-cli-v1",
                        "success": true,
                        "state_code": manifest.state_code,
                        "source_code": manifest.source_code,
                        "schema_version": manifest.schema_version,
                        "export_id": manifest.export_id,
                        "part_count": manifest.files.len(),
                        "manifest_checksum": manifest.files.iter().map(|f| f.sha256_checksum.clone()).collect::<Vec<_>>(),
                    }))?
                );
            } else {
                println!(
                    "state-export-v1 package verified: export_id={} files={}",
                    manifest.export_id,
                    manifest.files.len()
                );
            }
        }
        StateExportV1Command::Acknowledge {
            consumer,
            export_id,
            json,
        } => {
            let pool = db::connect(&cfg.database_url).await?;
            state_export::acknowledge_export(&pool, &consumer, &export_id).await?;
            if json {
                println!(
                    "{}",
                    serde_json::to_string(&serde_json::json!({
                        "command_version": "state-sync-cli-v1",
                        "success": true,
                        "consumer_code": consumer,
                        "export_id": export_id,
                        "acknowledgement_status": "acknowledged",
                    }))?
                );
            } else {
                println!("state-export-v1 acknowledged: consumer={consumer} export_id={export_id}");
            }
        }
        StateExportV1Command::Status { consumer, json } => {
            let pool = db::connect(&cfg.database_url).await?;
            let status = state_export::status(&pool, &consumer).await?;
            if json {
                println!(
                    "{}",
                    serde_json::to_string(&serde_json::json!({
                        "command_version": "state-sync-cli-v1",
                        "success": true,
                        "consumer_code": consumer,
                        "status_text": status,
                    }))?
                );
            } else {
                println!("{status}");
            }
        }
        StateExportV1Command::CleanupPartials {
            output_dir,
            older_than_seconds,
            json,
        } => {
            let removed = state_export::cleanup_partials(
                &output_dir,
                std::time::Duration::from_secs(older_than_seconds),
            )?;
            if json {
                println!(
                    "{}",
                    serde_json::to_string(&serde_json::json!({
                        "command_version": "state-sync-cli-v1",
                        "success": true,
                        "removed_count": removed.len(),
                        "removed_paths": removed,
                    }))?
                );
            } else {
                println!(
                    "state-export-v1 partial cleanup removed {} directories",
                    removed.len()
                );
            }
        }
    }
    Ok(())
}

fn print_outcome(
    source_format: &str,
    outcome: &future_wells_kansas::parse::kgs_wells::ImportOutcome,
) {
    println!("source: {source_format}");
    println!("run_id: {}", outcome.run_id);
    println!("status: {:?}", outcome.status);
    println!("included: {}", outcome.included_count);
    println!("excluded: {}", outcome.excluded_count);
    println!(
        "manifest: {}",
        serde_json::to_string_pretty(&outcome.manifest).unwrap_or_default()
    );
}
