#!/usr/bin/env bash
set -Eeuo pipefail

APP_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SERVICE_NAME="${SERVICE_NAME:-future-wells-ks-api.service}"
HEALTH_URL="${HEALTH_URL:-http://127.0.0.1:4008/health}"
USER_SYSTEMD_DIR="${USER_SYSTEMD_DIR:-$HOME/.config/systemd/user}"

log() {
  printf '\n==> %s\n' "$*"
}

install_user_unit() {
  local unit="$1"
  local source="$APP_DIR/deploy/systemd/user/$unit"
  if [ ! -f "$source" ]; then
    return 0
  fi
  install -d -m 0755 "$USER_SYSTEMD_DIR"
  install -m 0644 "$source" "$USER_SYSTEMD_DIR/$unit"
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

log "Pulling latest code"
git pull --ff-only

log "Building Kansas backend"
cargo build --release --locked --bin future_wells_ks_api --bin ks_ingest

log "Installing user systemd units"
install_user_unit future-wells-ks-api.service

systemctl --user daemon-reload
systemctl --user enable "$SERVICE_NAME" >/dev/null

log "Restarting ${SERVICE_NAME}"
systemctl --user restart "$SERVICE_NAME"

log "Checking Kansas backend health"
wait_for_http "$HEALTH_URL"
systemctl --user status "$SERVICE_NAME" --no-pager
