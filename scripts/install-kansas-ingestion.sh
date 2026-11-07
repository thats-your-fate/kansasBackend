#!/usr/bin/env bash
# Native per-user runtimes and persistent systemd timers; no root required.
set -euo pipefail
cd "$(dirname "$0")/.."
bash scripts/install-r2-runtime.sh
install -d -m 0755 "$HOME/.config/systemd/user"
for dataset in documents production wells; do
  for kind in service timer; do
    install -m 0644 "deploy/systemd/user/future-wells-ks-${dataset}.${kind}" "$HOME/.config/systemd/user/"
  done
done
systemctl --user daemon-reload
systemctl --user enable --now future-wells-ks-documents.timer future-wells-ks-production.timer future-wells-ks-wells.timer
systemctl --user list-timers 'future-wells-ks-*' --no-pager
