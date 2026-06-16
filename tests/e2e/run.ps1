<#
.SYNOPSIS
  Host-side convenience wrapper for the e2e stack (Windows / PowerShell).

.DESCRIPTION
  Sugar over `docker compose`; CI calls `docker compose run` directly.

.EXAMPLE
  .\run.ps1               # build + run the e2e suite (default)
  .\run.ps1 all           # run the FULL unit suite under real Airflow
  .\run.ps1 standalone    # bring up Airflow UI at http://localhost:8080
  $env:AIRFLOW_VERSION="3.0.3"; .\run.ps1
#>
[CmdletBinding()]
param(
    [ValidateSet("e2e", "all", "standalone")]
    [string]$Target = "e2e"
)

$ErrorActionPreference = "Stop"
Push-Location $PSScriptRoot
try {
    switch ($Target) {
        "e2e" { docker compose run --rm --build e2e }
        "all" { docker compose run --rm --build e2e bash -c "PYTEST_TARGETS=tests bash tests/e2e/run-e2e.sh" }
        "standalone" { docker compose --profile manual up --build standalone }
    }
    exit $LASTEXITCODE
}
finally {
    Pop-Location
}
