#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
# run-e2e.sh — the in-container e2e entrypoint (parity: local == CI).
# ─────────────────────────────────────────────────────────────────────────────
# Initialises a throwaway Airflow metadata DB, reserializes the example DAGs
# through the REAL scheduler parser (the most faithful import check), then runs
# the marked e2e pytest suite which asserts against the parsed state.
#
# Any DAG import error makes `airflow dags list-import-errors` non-empty and
# fails the pytest assertions, so a broken compat shim / accidental eager import
# / bad provider registration is caught here exactly as it would be in a deploy.
# ─────────────────────────────────────────────────────────────────────────────
set -euo pipefail

# Opt in to collecting tests/e2e (the root conftest excludes it otherwise).
export RUN_E2E=1

echo "::group::[e2e] airflow db migrate"
airflow db migrate >/dev/null 2>&1
echo "metadata DB ready"
echo "::endgroup::"

echo "::group::[e2e] airflow dags reserialize (real scheduler parse)"
# Reserialize parses every file in the DAGs folder. It exits 0 even when some
# files fail to import, so the e2e suite inspects `list-import-errors` to gate.
airflow dags reserialize || true
echo "::endgroup::"

echo "::group::[e2e] pytest ${PYTEST_TARGETS:-tests/e2e}"
exec python -m pytest ${PYTEST_TARGETS:-tests/e2e} -v "$@"
