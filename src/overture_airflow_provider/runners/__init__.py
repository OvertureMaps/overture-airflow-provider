"""Bundled job runner scripts for Glue, Databricks, and Wherobots.

Each runner is a self-contained entry point uploaded to the cluster at job
submission time. Runners are **Overture-free**: they depend only on the Spark
runtime, the Python standard library, and dynamically-imported user job classes.

``SCALA_RUNNER_SOURCE`` is embedded here as a string so it is available as
package data regardless of build-backend non-Python file inclusion policy.
"""

SCALA_RUNNER_SOURCE: str = """\
import com.amazonaws.services.glue.GlueContext
import com.amazonaws.services.glue.util.GlueArgParser
import org.apache.spark.SparkContext
import scala.jdk.CollectionConverters._

/**
 * Thin AWS Glue entry point for Scala jobs.
 *
 * Expected Glue job args (set via --default_arguments):
 *   --class_name       Fully-qualified class name to instantiate.
 *   --params           JSON-encoded parameter string forwarded to run().
 *   --extra_spark_conf JSON-encoded map of additional SparkConf key/value pairs.
 *
 * The target class must expose a zero-argument constructor and one of:
 *   - run(spark: SparkSession, params: String): Unit  (preferred)
 *   - run(params: String): Unit                       (legacy)
 */
object JobRunnerGlue {
  def main(args: Array[String]): Unit = {
    val resolvedArgs = GlueArgParser
      .getResolvedOptions(args, Array("class_name", "params", "extra_spark_conf"))
      .asScala

    val className = resolvedArgs.getOrElse("class_name", "")
    val params    = resolvedArgs.getOrElse("params", "{}")

    val sc      = new SparkContext()
    val glueCtx = new GlueContext(sc)
    val spark   = glueCtx.getSparkSession

    val clazz    = Class.forName(className)
    val instance = clazz.getDeclaredConstructor().newInstance()

    // Prefer run(SparkSession, String); fall back to run(String).
    try {
      val m = clazz.getMethod(
        "run",
        classOf[org.apache.spark.sql.SparkSession],
        classOf[String],
      )
      m.invoke(instance, spark, params)
    } catch {
      case _: NoSuchMethodException =>
        val m = clazz.getMethod("run", classOf[String])
        m.invoke(instance, params)
    }
  }
}
"""
