#!/usr/bin/env bash
# Build the C++ components, create a virtualenv with pinned-minimum dependencies, run the core
# tests. Set FLEETRIGHT_SKIP_VENV=1 to use the current Python environment as-is (offline hosts).
# PostgreSQL integration tests run only when FLEETRIGHT_TEST_DATABASE_URL is exported (see README).
set -euo pipefail
repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
python_bin="${PYTHON:-python3}"
python_major="$(${python_bin} -c 'import sys; print(sys.version_info.major)')"
python_minor="$(${python_bin} -c 'import sys; print(sys.version_info.minor)')"
if (( python_major != 3 || python_minor < 11 )); then
  echo "Python 3.11+ required; found $(${python_bin} --version)" >&2
  exit 1
fi
for tool in cmake protoc; do
  command -v "$tool" >/dev/null || { echo "$tool is required" >&2; exit 1; }
done
if [[ "${FLEETRIGHT_SKIP_VENV:-0}" != "1" ]]; then
  venv="${FLEETRIGHT_VENV:-$repo_root/.venv}"
  [[ -x "$venv/bin/python" ]] || "$python_bin" -m venv "$venv"
  "$venv/bin/python" -m pip install --quiet --requirement "$repo_root/requirements-dev.txt"
  python_bin="$venv/bin/python"
  export PATH="$venv/bin:$PATH"
fi
cmake -S "$repo_root" -B "$repo_root/build" -DCMAKE_BUILD_TYPE=Release
cmake --build "$repo_root/build" --parallel
ctest --test-dir "$repo_root/build" --output-on-failure
PYTHONPATH="$repo_root/python" "$python_bin" -m pytest -q "$repo_root/python/tests"
ruff check "$repo_root/python" "$repo_root/workloads" "$repo_root/scripts"
