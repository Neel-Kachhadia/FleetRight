#!/usr/bin/env bash
set -euo pipefail
repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PYTHONPATH="$repo_root/python${PYTHONPATH:+:$PYTHONPATH}" \
  python3 -m fleetright.experiment "$@"
