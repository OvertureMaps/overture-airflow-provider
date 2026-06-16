# E2E tests

Credential-free end-to-end tests that install the provider into a **real Apache
Airflow** (via Docker) and prove it integrates: the package installs under the
target Airflow's official constraints, the all-platforms example DAG parses
cleanly through the actual scheduler, the provider and its operator extra-links
register, and the spark-agnostic task group expands to the expected task graph.

Nothing here talks to Glue / Databricks / Wherobots — no cloud credentials are
needed. Live-platform tests remain a separate follow-up.

## Running

```bash
cd tests/e2e
docker compose run --rm --build e2e
# or the wrappers:
./run.sh                 # Linux/macOS/WSL
.\run.ps1                # Windows PowerShell
```

Pick the Airflow/Python version with build args (defaults: 2.11.0 / 3.12):

```bash
AIRFLOW_VERSION=3.0.3 PYTHON_VERSION=3.12 docker compose run --rm --build e2e
```

## What runs where

| Piece | Role |
| --- | --- |
| `Dockerfile` | Extends `apache/airflow:${AIRFLOW_VERSION}-python${PYTHON_VERSION}`, pip-installs the provider (core only) + pytest under Airflow's official constraints. |
| `docker-compose.yaml` | `e2e` one-shot service and an opt-in `standalone` service (Airflow UI) under the `manual` profile. |
| `run-e2e.sh` | In-container entrypoint: `airflow db migrate` -> `airflow dags reserialize` (real parse) -> `pytest`. |
| `test_dag_e2e.py` | The assertions: no import errors, DAG + provider + extra-links registered, task-group expansion + dependency edges, lazy-import purity, setup-task execution, and the setup_info XCom round-trip. |
| `run.sh` / `run.ps1` | Host sugar over `docker compose` (`e2e` \| `all` \| `standalone`). |

Platform SDKs (`databricks-sdk`, `wherobots`) are deliberately **not** installed,
so the example DAG parsing with all three platforms present is a hard gate on the
lazy-import architecture (no eager Airflow/SDK imports at DAG-parse time).

## Run the full unit suite under real Airflow

Windows/macOS devs can't easily install Airflow locally. The Docker image can
also run the whole `tests/` suite:

```bash
./run.sh all        # or:  .\run.ps1 all
```

## Explore the UI

```bash
docker compose --profile manual up --build standalone
# http://localhost:8080  (airflow standalone prints the admin password)
```

## Collection gating

`tests/e2e` is excluded from the default `pytest` run (the root `conftest.py`
sets `collect_ignore` unless `RUN_E2E=1`), so the host/unit legs never try to
run these without a real Airflow CLI. The container sets `RUN_E2E=1`.
