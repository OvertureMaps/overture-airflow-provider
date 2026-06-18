# ─────────────────────────────────────────────────────────────────────────────
# E2E image: the provider installed into a REAL Apache Airflow.
# ─────────────────────────────────────────────────────────────────────────────
# Extends the official Airflow image and installs this provider (no platform
# extras) plus the test runner. Installing under Airflow's official constraints
# pins apache-airflow-providers-amazon to a version compatible with the exact
# Airflow release, so pip never silently upgrades/downgrades Airflow itself.
#
# Build matrix is driven by AIRFLOW_VERSION / PYTHON_VERSION build args so CI
# can exercise the same image across the Airflow 2.x and 3.x compat branches.
#
# Platform SDKs (databricks-sdk, wherobots) are intentionally NOT installed:
# the lazy-import architecture means the all-platforms example DAG must parse
# with only core installed. Leaving them out makes that a hard, enforced gate.
# ─────────────────────────────────────────────────────────────────────────────
ARG AIRFLOW_VERSION=2.11.0
ARG PYTHON_VERSION=3.12

FROM apache/airflow:${AIRFLOW_VERSION}-python${PYTHON_VERSION}

# Re-declare so the args are in scope for RUN below.
ARG AIRFLOW_VERSION
ARG PYTHON_VERSION
ARG CONSTRAINTS_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

# Copy the provider source needed to build + test the wheel.
COPY --chown=airflow:0 pyproject.toml README.md LICENSE /opt/overture/
COPY --chown=airflow:0 src /opt/overture/src
COPY --chown=airflow:0 tests /opt/overture/tests
COPY --chown=airflow:0 examples /opt/overture/examples

# Install the provider (core only) + test runner, constrained to this Airflow.
RUN pip install --no-cache-dir --constraint "${CONSTRAINTS_URL}" \
        "/opt/overture" "pytest>=8.0" "pytest-mock>=3.12"

WORKDIR /opt/overture
