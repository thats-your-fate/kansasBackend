#!/usr/bin/env bash
set -Eeuo pipefail

APP_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SERVICE_NAME="${SERVICE_NAME:-future-wells-ks-api.service}"
HEALTH_URL="${HEALTH_URL:-}"
ENV_FILE="${ENV_FILE:-/etc/future-wells/kansas-api.env}"

log() {
  printf '\n==> %s\n' "$*"
}

need_sudo() {
  if [ -t 0 ]; then
    sudo -v
  elif ! sudo -n true 2>/dev/null; then
    echo "sudo access is required to manage ${SERVICE_NAME}; run deploy.sh from an interactive shell." >&2
    exit 1
  fi
}

wait_for_http() {
  local url="$1"
  local attempt
  for attempt in $(seq 1 30); do
    if curl -fsS "$url" >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
  done
  echo "Timed out waiting for ${url}" >&2
  return 1
}

cd "$APP_DIR"
need_sudo

log "Pulling latest code"
git pull --ff-only

if [ -r "$ENV_FILE" ]; then
  set -a
  # shellcheck source=/dev/null
  source "$ENV_FILE"
  set +a
fi

KS_API_PORT="${KS_API_PORT:-4008}"
HEALTH_URL="${HEALTH_URL:-http://127.0.0.1:${KS_API_PORT}/health}"

log "Building Kansas backend"
cargo build --release --locked --bin future_wells_ks_api --bin ks_ingest

log "Installing systemd unit"
sudo install -m 0644 "$APP_DIR/deploy/systemd/${SERVICE_NAME}" "/etc/systemd/system/${SERVICE_NAME}"
sudo systemctl daemon-reload
sudo systemctl enable "$SERVICE_NAME" >/dev/null

log "Restarting ${SERVICE_NAME}"
sudo systemctl restart "$SERVICE_NAME"

if systemctl --user is-active --quiet "$SERVICE_NAME" 2>/dev/null; then
  log "Stopping legacy user service"
  systemctl --user disable --now "$SERVICE_NAME" >/dev/null 2>&1 || true
fi

log "Checking Kansas backend health"
wait_for_http "$HEALTH_URL"
sudo systemctl status "$SERVICE_NAME" --no-pager
