#!/usr/bin/env bash
set -euo pipefail
cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.."
test "$(hostname -s)" = futurewells
test "$(git branch --show-current)" = main
test "$(git rev-parse HEAD)" = "$(git rev-parse origin/main)"
git diff --quiet
git diff --cached --quiet
export CARGO_BUILD_JOBS=1
"$HOME/.cargo/bin/cargo" build --locked --release --bin future_wells_ks_api
systemctl --user restart future-wells-ks-api.service
curl --fail --silent --retry 10 --retry-connrefused --retry-delay 1 --max-time 45 --output /dev/null 'http://127.0.0.1:4008/api/ks/production?api=15175001600001'
