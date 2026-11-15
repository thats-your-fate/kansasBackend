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
