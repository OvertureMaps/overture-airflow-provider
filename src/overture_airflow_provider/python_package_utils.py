"""Private package registry (CodeArtifact) client, HTTP/PyPI downloaders, S3/DBFS uploaders."""

import logging
import os
from collections.abc import Callable
from datetime import UTC, datetime
from enum import Enum
from typing import Any
from urllib.parse import urlparse

import boto3
import requests
from boto3.s3.transfer import S3Transfer
from packaging.version import InvalidVersion, Version


class PackageVersionStrategy(Enum):
    LATEST_IN_BRANCH = "LATEST_IN_BRANCH"  # assumes poetry-dynamic-versioning in use
    LATEST_STABLE = "LATEST_STABLE"
    CUSTOM = "CUSTOM"


class CodeArtifactPyPiClient:
    """Lightweight client for AWS CodeArtifact (pip + auth token URL)."""

    def __init__(
        self,
        domain_owner: str,
        repository: str,
        domain: str,
        region_name: str,
    ):
        self.domain_owner = domain_owner
        self.repository = repository
        self.domain = domain
        self.region_name = region_name
        self.auth_token = None
        self.auth_token_expiration = None
        self.session = boto3.session.Session(region_name=self.region_name)
        self.codeartifact = self.session.client("codeartifact")

    def resolve_package_version(
        self,
        package_name: str,
        strategy: PackageVersionStrategy,
        custom_version: str | None = None,
        branch: str | None = None,
    ) -> str:
        """Resolve a package version per the requested strategy.

        Args:
            package_name: Package to resolve.
            strategy: Version-resolution strategy.
                - ``LATEST_IN_BRANCH``: latest version in ``branch`` (relies on
                  poetry-dynamic-versioning local-version encoding).
                - ``LATEST_STABLE``: latest non-prerelease, non-dev version.
                - ``CUSTOM``: returns ``custom_version`` verbatim.
            custom_version: Required when strategy is ``CUSTOM``.
            branch: Required when strategy is ``LATEST_IN_BRANCH``.
        """
        if strategy == PackageVersionStrategy.LATEST_IN_BRANCH:
            return self.get_latest_package_version_in_branch(package_name, branch)
        if strategy == PackageVersionStrategy.LATEST_STABLE:
            return self.get_latest_stable_package_version(package_name)
        if strategy == PackageVersionStrategy.CUSTOM:
            if not custom_version:
                raise ValueError(
                    "custom_version must be specified when using VersionStrategy.CUSTOM!"
                )
            return custom_version
        raise ValueError(f"Unknown version resolution strategy: {strategy}")

    def get_latest_stable_package_version(self, package_name: str) -> str:
        return str(
            self.get_latest_package_using_filter(
                package_name, lambda v: not v.is_prerelease and not v.is_devrelease
            )
        )

    def get_latest_package_version_in_branch(self, package_name: str, branch_name: str) -> str:
        """Latest version in ``branch_name``.

        Assumes the package uses poetry-dynamic-versioning and the branch is
        encoded in the local version (e.g. ``0.0.1.dev0+mybranch.2996.20250622165115``).
        """

        def matches_branch(version: Version) -> bool:
            if version.local:
                return version.local.split(".")[0] == branch_name.replace("-", "")
            return False

        return str(self.get_latest_package_using_filter(package_name, matches_branch))

    def get_latest_package_version(self, package_name: str) -> str:
        """Latest available version (may be a prerelease from any branch)."""
        return str(self.get_package_versions(package_name)[0])

    def get_latest_package_using_filter(
        self, package_name: str, cond: Callable[[Version], bool]
    ) -> Version | None:
        versions = self.get_package_versions(package_name)
        for v in versions:
            if cond(v):
                return v
        return None

    def get_package_versions(self, package_name: str) -> list[Version]:
        """List all valid ``Version`` objects for ``package_name`` in this repo."""
        client = boto3.client("codeartifact", region_name=self.region_name)
        response = client.list_package_versions(
            domain=self.domain,
            repository=self.repository,
            format="pypi",
            package=package_name,
            sortBy="PUBLISHED_TIME",  # descending order
        )
        raw_versions = response.get("versions", [])
        if not raw_versions:
            raise Exception(f"No versions found for {package_name}")

        versions: list[Version] = []
        for v in raw_versions:
            try:
                versions.append(Version(v["version"]))
            except InvalidVersion:
                continue

        if not versions:
            raise Exception(f"No valid versions found for {package_name}")

        return versions

    def get_auth_token(self) -> str:
        if (
            not self.auth_token
            or not self.auth_token_expiration
            or (self.auth_token_expiration.replace(tzinfo=UTC) - datetime.now(UTC)).total_seconds()
            < 3600
        ):
            response = self.codeartifact.get_authorization_token(
                domain=self.domain, domainOwner=self.domain_owner
            )
            self.auth_token = response["authorizationToken"]
            self.auth_token_expiration = response["expiration"]

        return self.auth_token

    def get_url(self) -> str:
        return (
            f"https://aws:{self.get_auth_token()}@{self.domain}-{self.domain_owner}"
            f".d.codeartifact.{self.region_name}.amazonaws.com/pypi/{self.repository}/simple/"
        )


class HttpDownloader:
    def __init__(self, output_dir: str):
        self.output_dir = output_dir
        os.makedirs(output_dir, exist_ok=True)

    def download_urls(self, urls: list[str]) -> list[str]:
        return [self.download_url(url) for url in urls if url.strip() != ""]

    def download_url(self, url: str) -> str:
        response = requests.get(url, stream=True, timeout=300)
        response.raise_for_status()
        save_path = os.path.join(self.output_dir, os.path.basename(urlparse(url).path))
        with open(save_path, "wb") as fh:
            for chunk in response.iter_content(chunk_size=2**18):
                if chunk:
                    fh.write(chunk)
        return save_path


class PyPiDownloader:
    def __init__(self, pypi_client: CodeArtifactPyPiClient, output_dir: str):
        self.client = pypi_client
        self.output_dir = output_dir
        os.makedirs(output_dir, exist_ok=True)

    def download_packages(self, packages: list[str], python_version: str) -> None:
        if not packages:
            return

        # Single pip call: ensures pip resolves a consistent dependency tree
        # (avoids duplicate/conflicting versions when transitive deps overlap
        # with explicitly requested packages).
        pip_command = [
            "download",
            "--python-version",
            python_version,
            "--only-binary",
            ":all:",
            # Required: many packages are published as PEP 440 pre-releases.
            "--pre",
            # We don't care if the package is incompatible with the local Airflow
            # python version; we only want to download it.
            "--ignore-requires-python",
            "--index-url",
            self.client.get_url(),
            "--dest",
            self.output_dir,
            *packages,
        ]

        try:
            import sh

            sh.pip(*pip_command)
            logging.info("Successfully downloaded %s and dependencies.", packages)
        except sh.ErrorReturnCode as exc:
            logging.error(
                "Error downloading pypi packages %s: %s",
                packages,
                exc.stderr.decode(),
            )
            raise


class S3Uploader:
    def __init__(self, bucket_name: str):
        self.s3_client = boto3.client("s3")
        self.s3_transfer = S3Transfer(self.s3_client)
        self.bucket_name = bucket_name

    def upload_directory(self, directory_path: str, s3_prefix: str = "") -> list[str]:
        """Upload directory contents to S3. Returns list of resulting ``s3://`` URLs."""
        result: list[str] = []
        for root, _, files in os.walk(directory_path):
            for file in files:
                local_path = os.path.join(root, file)
                relative_path = os.path.relpath(local_path, directory_path)
                s3_path = os.path.join(s3_prefix, relative_path).replace("\\", "/").lstrip("/")
                try:
                    self.s3_transfer.upload_file(local_path, self.bucket_name, s3_path)
                    uri = f"s3://{self.bucket_name}/{s3_path}"
                    logging.info("Uploaded %s to %s", local_path, uri)
                    result.append(uri)
                except Exception as exc:
                    logging.error(
                        "Failed to upload %s to s3://%s/%s: %s",
                        local_path,
                        self.bucket_name,
                        s3_path,
                        exc,
                    )
                    raise
        return result


class DBFSUploader:
    def __init__(self):
        from databricks.sdk import WorkspaceClient  # local: optional SDK

        self.client = WorkspaceClient()

    def upload_directory(self, local_directory: str, dbfs_directory: str) -> list[str]:
        """Upload directory contents to DBFS. Returns list of DBFS paths."""
        result: list[str] = []
        for root, _, files in os.walk(local_directory):
            for file in files:
                local_path = os.path.join(root, file)
                relative_path = os.path.relpath(local_path, local_directory)
                dbfs_path = os.path.join(dbfs_directory, relative_path).replace("\\", "/")
                with open(local_path, "rb") as fh:
                    try:
                        self.client.dbfs.upload(dbfs_path, fh, overwrite=True)
                        logging.info("Uploaded %s to %s", local_path, dbfs_path)
                        result.append(dbfs_path)
                    except Exception as exc:
                        logging.error(
                            "Failed to upload %s to %s: %s",
                            local_path,
                            dbfs_path,
                            exc,
                        )
                        raise
        return result


# Reserved for future use: keeps mypy/IDE happy about ``Dict``/``Any`` imports
# that may be re-introduced by downstream extension modules.
_RESERVED: dict[str, Any] = {}
