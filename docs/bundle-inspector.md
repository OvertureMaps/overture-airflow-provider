# Bundle Inspector plugin

Installing this package registers `bundle_inspector`, an Airflow plugin (not related to `spark_agnostic_task_group`) for browsing Overture bundles in S3 by pipeline stage, theme, schema version, and run ID. It is served as an Airflow 3 FastAPI app. No plugins-folder drop-in needed, no separate install step: it ships in this package and shows up under **Browse -> Bundle Inspector** once the provider is installed.

Configure it via the `[bundle_inspector]` section in `airflow.cfg`:

| Option | Required | Description |
|--------|----------|-------------|
| `s3_bucket` | yes | S3 bucket containing bundle data |
| `environment` | no (default `dev`) | Environment name (`dev`, `staging`, `prod`) |
| `athena_output_bucket` | no | S3 bucket for Athena query results; falls back to `overture-bundle-inspector-athena-output-<environment>` |
| `user` | no | Namespace prefix under the bucket, read only when `environment` is `dev` |

Each option is also settable via its `AIRFLOW__BUNDLE_INSPECTOR__<OPTION>` environment variable (e.g. `AIRFLOW__BUNDLE_INSPECTOR__S3_BUCKET`), Airflow's standard override convention for provider config, so no metadata DB round-trip or Variable setup is needed to get the plugin running.

GeoParquet preview (`component-data`, `parquet-stats`) needs `pyarrow` and `shapely`: `pip install "airflow-provider-overture[bundle-inspector]"`. Without them those two endpoints return a 500 with the import error; the rest of the UI (tree browsing, PMTiles, CSV, Athena queries) works without either.

The UI loads its JS libraries from `cdn.jsdelivr.net` and its map basemap from [OpenFreeMap](https://openfreemap.org) (`tiles.openfreemap.org`, Positron style, no API key), so users' browsers need access to both hosts. If the basemap can't be reached, maps fall back to a blank background and still draw their data.
