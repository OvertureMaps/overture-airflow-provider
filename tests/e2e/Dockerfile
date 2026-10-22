# ─────────────────────────────────────────────────────────────────────────────
# E2E image: the provider installed into a REAL Apache Airflow.
# ─────────────────────────────────────────────────────────────────────────────
# Extends the official Airflow image and installs this provider (no platform
# extras) plus the test runner. Installing under Airflow's official constraints
# pins apache-airflow-providers-amazon to a version compatible with the exact
# Airflow release, so pip never silently upgrades/downgrades Airflow itself.
#
# Build matrix is driven by AIRFLOW_VERSION / PYTHON_VERSION build args so CI
# can exercise the same image across supported Airflow 3.x releases.
#
# Platform SDKs (databricks-sdk, wherobots) are intentionally NOT installed:
# the lazy-import architecture means the all-platforms example DAG must parse
# with only core installed. Leaving them out makes that a hard, enforced gate.
# ─────────────────────────────────────────────────────────────────────────────
ARG AIRFLOW_VERSION=3.3.1
ARG PYTHON_VERSION=3.12

FROM apache/airflow:${AIRFLOW_VERSION}-python${PYTHON_VERSION}

# Re-declare so the args are in scope for RUN below.
ARG AIRFLOW_VERSION
ARG PYTHON_VERSION
ARG CONSTRAINTS_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

# emSign ECC Root CA - G3 remediation (GHSA-6q48-6c7w-9gw2): the base image's pip
# vendors a certifi that still trusts the compromised root. Hash-pinned so the
# download is verified even over a pre-fix TLS stack.
ARG PIP_WHEEL_URL="https://files.pythonhosted.org/packages/3f/06/3e4420302363fbb71be3c7f35ce042f54bfca330e3e66b3a2e3c4cca579f/pip-26.2.2-py3-none-any.whl#sha256=fd7f4dfa8190e014013b9f08f3e973f4e4c26ffd179a204895533a7254813a70"
RUN pip install --no-cache-dir "${PIP_WHEEL_URL}"

# Copy the provider source needed to build + test the wheel.
COPY --chown=airflow:0 pyproject.toml README.md LICENSE /opt/overture/
COPY --chown=airflow:0 src /opt/overture/src
COPY --chown=airflow:0 tests /opt/overture/tests
COPY --chown=airflow:0 examples /opt/overture/examples

# Install the provider (core only) + test runner, constrained to this Airflow.
# The constraints file pins an older certifi, so raise it to the fixed floor afterwards.
RUN pip install --no-cache-dir --constraint "${CONSTRAINTS_URL}" \
        "/opt/overture" "pytest>=8.0" "pytest-mock>=3.12" \
    && pip install --no-cache-dir "certifi>=2026.10.19"

WORKDIR /opt/overture
