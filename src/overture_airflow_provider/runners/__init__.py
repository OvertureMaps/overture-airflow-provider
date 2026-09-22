"""Bundled job runner scripts for Glue, Databricks, and Wherobots.

Each runner is a self-contained entry point uploaded to the cluster at job
submission time. Runners are **Overture-free**: they depend only on the Spark
runtime, the Python standard library, and dynamically-imported user job classes.

``SCALA_RUNNER_SOURCE`` is embedded here as a string so it is available as
package data regardless of build-backend non-Python file inclusion policy.

It is a deliberate comment-only no-op stub: AWS Glue Scala jobs run a
precompiled JAR selected via the ``--class`` job parameter (passed through
``--extra-jars``), so this ``scriptLocation`` file is never executed. Glue still
*compiles* it before the job starts, so it only needs to compile cleanly. See
the stub comment below for the full rationale.

``DATABRICKS_INIT_SCRIPT_SOURCE`` is likewise embedded as a string, for the
same package-data-inclusion reason. It is the Databricks cluster init script
referenced by ``DatabricksConfig.cluster_init_script_name`` (see
``config.py`` / ``_databricks.py``): it downloads the Sedona and GeoTools JARs
matching the cluster's ``SEDONA_VERSION``/``SPARK_VERSION``/``SCALA_VERSION``
env vars into ``/databricks/jars``. It is Overture-free — no Overture-specific
paths, buckets, or job logic — so it is bundled the same way as the job
runners rather than left to be hand-deployed by each caller.
"""

SCALA_RUNNER_SOURCE: str = """\
// Placeholder Scala script for AWS Glue Spark jobs - intentionally a no-op.
//
// AWS Glue requires a scriptLocation for every Scala job and COMPILES it before
// the job runs, even when the real entry point is overridden via the `--class`
// job parameter pointing at a class inside `--extra-jars`. Our Scala jobs ship
// their logic in a precompiled JAR (passed via --extra-jars) and select the main
// class with `--class`, so this file only needs to compile cleanly - it is never
// executed.
//
// Keep this trivial: do NOT add imports or logic. Anything that fails to compile
// on the Glue runtime's Scala version (e.g. Glue 5.0 = Scala 2.12) fails the whole
// job before the JAR's `--class` main ever runs.
//
// AWS Glue job parameters (--class, --scriptLocation, --job-language, --extra-jars):
// https://docs.aws.amazon.com/glue/latest/dg/aws-glue-programming-etl-glue-arguments.html
"""

DATABRICKS_INIT_SCRIPT_SOURCE: str = """\
#!/bin/bash

# Cluster init script: makes the Sedona and GeoTools JARs available at
# /databricks/jars for every node of a new Databricks cluster. Referenced via
# ``DatabricksConfig.cluster_init_script_name`` and deployed to the
# workspace path built from ``workspace_scripts_path_template`` (see
# ``overture_airflow_provider._databricks``, which sets SEDONA_VERSION,
# GEOTOOLS_VERSION, SPARK_VERSION, and SCALA_VERSION as cluster env vars).

# Directories
CACHE_DIR="/dbfs/databricks/shared/sedona/${SEDONA_VERSION}" # check if needed jars are available in this shared location from a previous job
LOCAL_TMP_DIR="/tmp/sedona_cache/${SEDONA_VERSION}"          # if cache doesn't have the jar, first download to /tmp because large downloads to /dbfs sometimes fail
DEST_DIR="/databricks/jars"                                  # copy cached jars here for new clusters here, each cluster has their own /databricks/jars

# Create directories if they do not exist
mkdir -p "$CACHE_DIR"
mkdir -p "$LOCAL_TMP_DIR"
mkdir -p "$DEST_DIR"

# Ensure environment variables are set
if [[ -z "$SEDONA_VERSION" || -z "$GEOTOOLS_VERSION" || -z "$SPARK_VERSION" || -z "$SCALA_VERSION" ]]; then
  echo "One or more required environment variables (SEDONA_VERSION, GEOTOOLS_VERSION, SPARK_VERSION, SCALA_VERSION) are not set."
  exit 1
fi

# Function to download a file if it does not exist
download_if_not_exists() {
  local jar_filename="$1"
  local url="$2"
  local cache_path="${CACHE_DIR}/${jar_filename}"
  local tmp_path="${LOCAL_TMP_DIR}/${jar_filename}"
  local dest_path="${DEST_DIR}/${jar_filename}"

  # Check if the file exists in the cache
  if [[ ! -f "$cache_path" ]]; then
    echo "Downloading $jar_filename to local temp directory"
    curl -f -L -C - -o "$tmp_path" "$url"
    if [[ $? -ne 0 ]]; then
      echo "Failed to download $jar_filename"
      rm -f "$tmp_path"
      exit 1
    fi

    # Try to populate the shared DBFS cache. Multiple nodes run this script in
    # parallel and may race to write the same path; treat a failed mv as benign
    # since another node likely wrote the file concurrently.
    echo "Moving completed JAR to DBFS cache directory"
    mv "$tmp_path" "$cache_path" 2>/dev/null \\
      || echo "DBFS cache write skipped (likely written concurrently by another node)"
  else
    echo "Found cached JAR: $cache_path"
  fi

  # Copy to per-node /databricks/jars — prefer the shared DBFS cache, but fall
  # back to the local temp file if the cache write above failed.
  if [[ -f "$cache_path" ]]; then
    cp "$cache_path" "$dest_path"
  elif [[ -f "$tmp_path" ]]; then
    echo "Using local temp copy for $jar_filename (DBFS cache unavailable)"
    cp "$tmp_path" "$dest_path"
  else
    echo "ERROR: $jar_filename not available in cache or local temp"
    exit 1
  fi

  if [[ $? -ne 0 ]]; then
    echo "Failed to copy $jar_filename to $DEST_DIR"
    exit 1
  fi
  echo "Copied $jar_filename to $DEST_DIR"
}

# Download Geotools Wrapper JAR
JAR_FILENAME="geotools-wrapper-${SEDONA_VERSION}-${GEOTOOLS_VERSION}.jar"
JAR_URL="https://repo1.maven.org/maven2/org/datasyslab/geotools-wrapper/${SEDONA_VERSION}-${GEOTOOLS_VERSION}/${JAR_FILENAME}"
download_if_not_exists "$JAR_FILENAME" "$JAR_URL"

# Download Sedona Spark Shaded JAR
JAR_FILENAME="sedona-spark-shaded-${SPARK_VERSION}_${SCALA_VERSION}-${SEDONA_VERSION}.jar"
JAR_URL="https://repo1.maven.org/maven2/org/apache/sedona/sedona-spark-shaded-${SPARK_VERSION}_${SCALA_VERSION}/${SEDONA_VERSION}/${JAR_FILENAME}"
download_if_not_exists "$JAR_FILENAME" "$JAR_URL"

echo "All JARs copied and downloaded successfully to $DEST_DIR"
"""
