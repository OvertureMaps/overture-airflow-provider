#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
# run.sh — host-side convenience wrapper for the e2e stack (Linux/macOS/WSL).
# ─────────────────────────────────────────────────────────────────────────────
# Usage:
#   ./run.sh                 # build + run the e2e suite (default)
#   ./run.sh all             # run the FULL unit suite under real Airflow
#   ./run.sh standalone      # bring up Airflow UI at http://localhost:8080
#   AIRFLOW_VERSION=3.0.3 PYTHON_VERSION=3.12 ./run.sh
#
# This is just sugar over `docker compose`; CI calls `docker compose run` directly.
# ─────────────────────────────────────────────────────────────────────────────
set -euo pipefail
cd "$(dirname "$0")"

case "${1:-e2e}" in
  e2e)
    exec docker compose run --rm --build e2e
    ;;
  all)
    # Full unit + e2e suite inside the real-Airflow image (local CI parity).
    # Routed through run-e2e.sh so db migrate + reserialize always run first.
    exec docker compose run --rm --build e2e \
      bash -c "PYTEST_TARGETS=tests bash tests/e2e/run-e2e.sh"
    ;;
  standalone)
    exec docker compose --profile manual up --build standalone
    ;;
  *)
    echo "usage: $0 [e2e|all|standalone]" >&2
    exit 2
    ;;
esac
