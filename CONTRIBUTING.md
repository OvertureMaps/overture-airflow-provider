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

## Publishing

Package published to [PyPI](https://pypi.org/project/overture-airflow-provider/) via the
[`publish-pypi.yml`](.github/workflows/publish-pypi.yml) workflow using
[OIDC trusted publishing](https://docs.pypi.org/trusted-publishers/) — no API token needed.
This repo, workflow, and GitHub environment must be pre-configured in PyPI and Test PyPI.

### Releasing a new version

1. Update `version` in `pyproject.toml` (`project.version`).
2. Commit and merge to `main`.
3. Create a GitHub Release (tag + title + notes).
4. The `publish-pypi.yml` workflow triggers automatically and publishes to PyPI
   in the [`pypi` GitHub environment](https://github.com/OvertureMaps/overture-airflow-provider/deployments).

### Dry-run / Test PyPI

Trigger the workflow manually via `workflow_dispatch` to publish to
[Test PyPI](https://test.pypi.org/project/overture-airflow-provider/) instead of production.
Useful for verifying the build and publish pipeline end-to-end. Uses `skip-existing: true`
so version conflicts don't fail the run.

### Environments

| GitHub environment | Target index    | Trigger                       |
|--------------------|-----------------|-------------------------------|
| `pypi`             | PyPI (prod)     | GitHub Release published      |
| `test-pypi`        | Test PyPI       | Manual `workflow_dispatch`    |

Both environments use OIDC trusted publisher entries on their respective indexes —
no secrets or API tokens stored in the repository.

## Reporting bugs

Open an issue with:
- a minimal DAG reproducing the problem
- the `spark_impl_name` you targeted
- the full Airflow task log (redact credentials)
