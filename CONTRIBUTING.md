# Contributing

Thanks for your interest in `overture-airflow-provider`!

## Dev setup

```bash
git clone https://github.com/OvertureMaps/overture-airflow-provider.git
cd overture-airflow-provider
uv sync --all-extras --group dev
```

## Common commands

```bash
uv run pytest -v                 # run the test suite
uv run ruff check .              # lint (includes Airflow AIR* rules)
uv run ruff format --check .     # format check
uv run ruff format .             # apply formatting
```

## PR title format

```
[TYPE] Short description
[TYPE](scope) Short description
[BREAKING][TYPE] Short description
```

Valid `TYPE` values: `BUG`, `FEATURE`, `ENHANCEMENT`, `DOCS`, `REFACTOR`,
`TEST`, `CHORE`, `PERFORMANCE`, `SECURITY`, `INVESTIGATION`.

Use `[WIP]` as a prefix on repos without draft PR support.

## Scope guidelines

- This provider stays **unopinionated**. Do not bake in defaults that only make
  sense for one organization (bucket names, role names, catalog names, pool
  names, theme names).
- New platform support is welcome but lands as a separate phase — open an
  issue first to discuss the SDK surface.
- Live-platform E2E tests are tracked separately and require CI credentials.
  PRs adding them go into `tests/e2e/`.

## Reporting bugs

Open an issue with:
- a minimal DAG reproducing the problem
- the `spark_impl_name` you targeted
- the full Airflow task log (redact credentials)
