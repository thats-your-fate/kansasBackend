# Kansas state-sync source tools

These files run on the Kansas source host. They were previously installed
outside Git under `~/.local/lib/future-wells/` and are recorded here from the
reviewed copies in futureBackend commit 584a6f38a54413af8c2c66fea4cff85c7b70ac89
(byte-identical to the deployed files on 15 November 2026):

- `kansas_sync_source.py` - restricted SSH interface used by Future Wells Co
  (`preflight`, `export`, `describe UUID`, `archive UUID`, `ack UUID`) and the
  scheduled/manual export workers.
- `export_package_retention.py` - keeps one verified export package.
- `initialize_kansas_admin.py` - creates or rotates the Kansas admin account.
- `kansas_export_views.sql` - export-only views that exclude test ingestion.

The consumer side (`kansas_sync.py`) stays in futureBackend.

## Isolated runtime

With `KS_SYNC_RUNNER=broker` (set only in the fixed system units of the
isolated site) the helper never uses the deployment user's service manager:

- `manual-start` writes `manual-export` and the export workers write `export`
  into `KS_JOB_REQUEST_DIR`; root-owned path units start the fixed
  `future-wells-ks-export.service` / `future-wells-ks-manual-export.service`.
  An export already running is joined, as `systemctl start` did.
- Uploads use the private `KS_SYNC_SSH_CONFIG`; locks and retention use
  `KS_EXPORT_HOME`, which has no symlinked ancestors.

`kansas_sync_ssh.py` becomes the forced command of the sync key. It accepts the
same five commands and passes them to the fixed root helper
`/usr/local/libexec/seo_ks_isolation.py --sync`, which runs this helper as
fw-ks-runtime inside the sealed release. Without these variables the helper
behaves exactly as before.
