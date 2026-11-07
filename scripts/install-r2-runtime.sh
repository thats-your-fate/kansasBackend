#!/usr/bin/env bash
# Official AWS CLI v2, installed for this checkout without system/root changes.
set -euo pipefail
cd "$(dirname "$0")/.."
root="$PWD/.tools"
case "$(uname -m)" in x86_64) arch=x86_64;; aarch64) arch=aarch64;; *) echo 'Unsupported AWS CLI architecture' >&2; exit 1;; esac
if [[ -x "$root/bin/aws" ]]; then "$root/bin/aws" --version; exit; fi
mkdir -p "$root"
staging=$(mktemp -d "$root/aws-install.XXXXXX")
trap 'rm -rf "$staging"' EXIT
curl --fail --silent --show-error --connect-timeout 10 --max-time 120 "https://awscli.amazonaws.com/awscli-exe-linux-${arch}.zip" -o "$staging/aws.zip"
python3 -m zipfile -e "$staging/aws.zip" "$staging"
# Python zip extraction does not preserve executable bits.
chmod -R u+X "$staging/aws"
chmod u+x "$staging/aws/install" "$staging/aws/dist/aws" "$staging/aws/dist/aws_completer"
"$staging/aws/install" --install-dir "$root/aws-cli" --bin-dir "$root/bin"
"$root/bin/aws" --version
