# Reference

## Provider requirements

|                | Minimum | Also tested |
| -------------- | ------- | ----------- |
| Python         | 3.11    | 3.12, 3.13  |
| Apache Airflow | 3.3     |             |

## Spark platform matrix

Pass one of these names as `spark_impl_name`:

| `spark_impl_name`  | Platform                    | Spark | Scala | Python runtime |
| ------------------ | --------------------------- | ----- | ----- | -------------- |
| `GLUE_v4`          | AWS Glue 4.0                | 3.3.0 | 2.12  | 3.10           |
| `GLUE_v5`          | AWS Glue 5.0                | 3.5.2 | 2.12  | 3.11           |
| `DATABRICKS_v14`   | Databricks Runtime 14.3 LTS | 3.5.0 | 2.12  | 3.10.12        |
| `DATABRICKS_v15`   | Databricks Runtime 15.4 LTS | 3.5.0 | 2.12  | 3.11.0         |
| `WHEROBOTS_v1_5_0` | Wherobots Cloud 1.5.0       | 3.5.0 | 2.12  | 3.11           |

> `SYNAPSE_v3_3_1` and `SYNAPSE_v3_4_1` are defined but not yet active (Azure Synapse support reserved).

## Apache Sedona

Sedona JARs are resolved from Maven Central at runtime. Tested pairings and the minimum Spark version required:

| Sedona | geotools-wrapper | Min Spark               |
| ------ | ---------------- | ----------------------- |
| 1.5.3  | 28.2             | 3.3                     |
| 1.6.1  | 28.2             | 3.3                     |
| 1.7.0  | 28.5             | 3.3                     |
| 1.7.1  | 28.5             | 3.3                     |
| 1.7.2  | 28.5             | 3.3                     |
| 1.8.1  | 33.1             | 3.4 (Spark 3.3 dropped) |
| 1.9.0  | 33.5             | 3.4 (Spark 3.3 dropped) |
