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
