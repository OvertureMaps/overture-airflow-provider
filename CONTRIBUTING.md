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
node --test                      # run bundle_inspector's static JS tests (no deps, Node's built-in runner)
```

## PR title format

Titles follow [Conventional Commits](https://www.conventionalcommits.org/). Squash merge
makes the title the commit message on `main`, and it drives the release version.

```
type: short description
type(scope): short description
type!: short description
```

Valid `type` values: `feat`, `fix`, `perf`, `docs`, `refactor`, `test`, `build`, `ci`, `chore`.
Add `!` for a breaking change. Open a draft PR for work in progress.

## Scope guidelines

- This provider stays **unopinionated**. Do not bake in defaults that only make
  sense for one organization (bucket names, role names, catalog names, pool
  names, theme names).
- New platform support is welcome but lands as a separate phase — open an
  issue first to discuss the SDK surface.
- Live-platform E2E tests are tracked separately and require CI credentials.
  PRs adding them go into `tests/e2e/`.

## Publishing

Package published to [PyPI](https://pypi.org/project/airflow-provider-overture/) via the
[`publish-pypi.yml`](.github/workflows/publish-pypi.yml) workflow using
[OIDC trusted publishing](https://docs.pypi.org/trusted-publishers/) — no API token needed.
This repo, workflow, and GitHub environment must be pre-configured in PyPI and Test PyPI.

### Releasing a new version

Releases are automated by [python-semantic-release](https://python-semantic-release.readthedocs.io/)
in [`release.yml`](.github/workflows/release.yml). Do not edit `project.version` by hand; it is
a placeholder, and `publish-pypi.yml` stamps the release tag's version into the build.

1. Merge to `main` using a [Conventional Commits](https://www.conventionalcommits.org/) title
   (`feat:` → minor, `fix:`/`perf:` → patch, `!` or a `BREAKING CHANGE:` footer → major;
   `docs:`, `chore:`, `test:` etc. do not release). With squash merge, the PR title becomes the
   commit message, so make it Conventional-Commits formatted at merge time.
2. `release.yml` creates the `vX.Y.Z` tag and GitHub Release with generated notes (nothing is
   committed back to `main`).
3. Publishing the release triggers `publish-pypi.yml`, which publishes to PyPI
   in the [`pypi` GitHub environment](https://github.com/OvertureMaps/overture-airflow-provider/deployments).
   If publishing fails, re-run that workflow run.

The release is created as the `overture-releaser` GitHub App (`contents:write` only): the job
assumes the `gha-releaser-secrets-reader` AWS role via OIDC and reads the app's PEM from Secrets
Manager. `GITHUB_TOKEN` can't be used because its releases don't fire `release: published`.
Release notes live on the GitHub Release; `CHANGELOG.md` is frozen at 0.17.1.

### Dry-run / Test PyPI

Trigger the workflow manually via `workflow_dispatch` to publish to
[Test PyPI](https://test.pypi.org/project/airflow-provider-overture/) instead of production.
Useful for verifying the build and publish pipeline end-to-end. Each run stamps a unique
`0.0.0.devN` version, so every upload is new. Uses `skip-existing: true` so a re-run doesn't fail.

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
