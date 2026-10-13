#!/usr/bin/env bash
# Start (or reuse) a disposable PostgreSQL for the integration tests. It binds 127.0.0.1 on an
# ephemeral port; the generated credentials live only in a 0600 env file (never printed).
#   scripts/start_test_db.sh && source "${FLEETRIGHT_STATE_DIR:-$HOME/.cache/fleetright}/test-db.env"
set -euo pipefail
state_dir="${FLEETRIGHT_STATE_DIR:-$HOME/.cache/fleetright}"
name="${FLEETRIGHT_TEST_DB_CONTAINER:-fleetright-isolated-test-db}"
env_file="$state_dir/test-db.env"
mkdir -p "$state_dir"
if [[ -z "$(docker ps -q --filter "name=^${name}$")" ]]; then
  docker rm -f "$name" >/dev/null 2>&1 || true
  password="$(head -c 24 /dev/urandom | base64 | tr -dc 'A-Za-z0-9')"
  docker run -d --name "$name" -e POSTGRES_USER=fleetright -e POSTGRES_DB=fleetright_test \
    -e "POSTGRES_PASSWORD=$password" -p 127.0.0.1::5432 postgres:16 >/dev/null
  port="$(docker port "$name" 5432/tcp | head -1 | sed 's/.*://')"
  (umask 077; : >"$env_file")
  printf 'export FLEETRIGHT_TEST_DATABASE_URL=postgresql://fleetright:%s@127.0.0.1:%s/fleetright_test\n' \
    "$password" "$port" >"$env_file"
  chmod 600 "$env_file"
fi
[[ -f "$env_file" ]] || { echo "container $name is running but $env_file is missing; remove the container and rerun" >&2; exit 1; }
until docker exec "$name" pg_isready -U fleetright -d fleetright_test >/dev/null 2>&1; do sleep 1; done
echo "ready; run: source $env_file"
