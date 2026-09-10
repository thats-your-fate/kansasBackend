//! KS-12/KS-13: the `state-export-v1` exporter — how Kansas plugs into the
//! shared Future Wells Co map + entity-detail infrastructure that already
//! exists in `futureBackend`/`futureMarketing`, rather than building a
//! standalone Kansas map or detail UI (both tickets explicitly forbid
//! that). See `docs/ops/ks-12-13-shared-infra-handoff.md` for what this
//! repo builds, what already exists on the `futureBackend`/`futureMarketing`
//! side, and what real gaps this export surfaced in that shared platform.

pub mod contract;
pub mod exporter;
pub mod hashing;
pub mod keys;
pub mod package;

pub use exporter::{
    acknowledge_export, cleanup_partials, export_state_v1, preflight, status, verify_package,
    ExportOptions, ExportSummary,
};
