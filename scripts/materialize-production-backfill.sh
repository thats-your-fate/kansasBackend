#!/usr/bin/env bash
# Resume a deliberate historical import while snapshot-production stages its files.
# Uses one database connection; progress survives process/server restarts.
set -Eeuo pipefail
cd "$(dirname "$0")/.."
set -a
[[ ! -f .env ]] || source .env
set +a
export KS_DB_MAX_CONNECTIONS=1
export PGCONNECT_TIMEOUT=15
repair_progress_dir="${KS_BACKFILL_PROGRESS_DIR:?Set a private progress directory for this backfill}"
mkdir -p "$repair_progress_dir"
chmod 700 "$repair_progress_dir"
touch "$repair_progress_dir/materialized-runs.txt"
while true; do
  if ! repair_runs="$(psql "${DATABASE_URL:-${PG_CONNECTION:?}}" -X -At -v ON_ERROR_STOP=1 <<'SQL'
SELECT DISTINCT ON (r.source_url) r.id FROM ks_ingestion_run r JOIN ks_source s ON s.id=r.source_id
WHERE r.status='completed' AND s.code IN ('KGS_PRODUCTION_OIL','KGS_PRODUCTION_GAS')
 AND r.source_url LIKE 'https://www.kgs.ku.edu/PRS/Ora_Archive/%'
 AND r.parser_version='kgs_production_zip-v1'
ORDER BY r.source_url,r.started_at DESC;
SQL
)"; then
    sleep 20
    continue
  fi
  while IFS= read -r repair_run; do
    [[ -n "$repair_run" ]] || continue
    if grep -Fxq "$repair_run" "$repair_progress_dir/materialized-runs.txt"; then continue; fi
    printf '\nMaterializing production run %s\n' "$repair_run"
    ./target/release/ks_ingest mark-run-backfill "$repair_run"
    ./target/release/ks_ingest materialize-production "$repair_run"
    printf '%s\n' "$repair_run" >> "$repair_progress_dir/materialized-runs.txt"
  done <<< "$repair_runs"
  if [[ "$(wc -l < "$repair_progress_dir/materialized-runs.txt")" -eq 8 ]]; then
    echo 'All eight KGS archive runs materialized successfully.'
    break
  fi
  sleep 20
done
