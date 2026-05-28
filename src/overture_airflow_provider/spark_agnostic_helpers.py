"""Shared helpers for Spark-agnostic operator/TaskGroup.

Common logic for downloading, caching, and managing Python packages and JAR
files from a private package registry (CodeArtifact) and Maven repositories.
"""

import datetime
import os
import shutil
import tempfile
import zipfile
from urllib.parse import urlparse

import boto3

from overture_airflow_provider.python_package_utils import (
    HttpDownloader,
    PyPiDownloader,
    S3Uploader,
)


class SparkAgnosticHelper:
    """Shared logic for Spark-agnostic operations (package + JAR caching)."""

    def __init__(
        self,
        job_name: str,
        run_identifier: str,
        s3_bucket: str,
        s3_root: str = "spark-agnostic-operator",
        force_pip_packages: list[str] | None = None,
    ):
        """Initialize the helper.

        Args:
            job_name: Spark job name.
            run_identifier: Unique identifier for this run.
            s3_bucket: S3 bucket for storing assets.
            s3_root: Key prefix root for all assets written by this operator.
            force_pip_packages: Substrings of package names forced through pip
                on the cluster (instead of being uploaded as wheels).
        """
        self.job_name = job_name
        self.run_identifier = run_identifier
        self.s3_bucket = s3_bucket
        self.s3_root = s3_root
        self.s3_prefix = f"{self.s3_root}/{run_identifier}"
        self.force_pip_packages = list(force_pip_packages or [])
        self._s3_client = None

    @property
    def s3_client(self):
        """Lazily construct the S3 client so tests/imports don't need AWS creds."""
        if self._s3_client is None:
            self._s3_client = boto3.client("s3")
        return self._s3_client

    @s3_client.setter
    def s3_client(self, value):
        self._s3_client = value

    @staticmethod
    def generate_run_identifier(job_name: str) -> str:
        timestamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
        return f"{job_name}/{timestamp}"

    @staticmethod
    def _has_native_dependencies(wheel_filename: str) -> bool:
        """Detect native deps via PEP 427 filename tagging.

        Pure-Python wheels always include ``-none-any`` in the platform tag
        (e.g. ``pkg-1.0-py3-none-any.whl``). Anything else is platform-specific.
        """
        if "-none-any.whl" in wheel_filename.lower():
            return False
        return True

    @staticmethod
    def _parse_wheel_filename(wheel_filename: str) -> tuple[str | None, str | None]:
        """Extract ``(package_name, version)`` from a PEP 427 wheel filename.

        Filename format: ``{dist}-{version}(-{build})?-{py}-{abi}-{platform}.whl``
        Returns ``(None, None)`` on parse failure.
        """
        name = wheel_filename[:-4] if wheel_filename.endswith(".whl") else wheel_filename
        parts = name.split("-")
        if len(parts) >= 2:
            # Wheel filenames use underscores; pip uses hyphens.
            package_name = parts[0].replace("_", "-")
            version = parts[1]
            return package_name, version
        return None, None

    def _force_pip_install(self, package: str) -> bool:
        for force_pkg in self.force_pip_packages:
            if force_pkg in package:
                return True
        return False

    def download_and_cache_python_packages(
        self,
        py_pi_client,
        packages: list[str],
        python_version: str,
        job_runner_wheel_prefix: str | None = None,
    ) -> tuple[str, str | None, str, list[str]]:
        """Download Python packages and cache them in S3.

        Native deps are detected from wheel filenames and excluded from S3
        uploads. Only **explicitly requested** packages (in ``packages``) are
        tracked and returned for install via ``--additional-python-modules``
        on Glue. Transitive native deps that exist in the target environment
        (e.g. numpy/shapely) are not included.

        Returns:
            Tuple of:
              - comma-separated S3 paths of cached pure-Python wheels,
              - job-runner wheel local path (or None),
              - temp folder path (caller cleans up),
              - list of explicitly requested native package specs
                (e.g. ``['numba==0.59.0']``).
        """
        # Force-install via pip for caller-configured complex packages.
        excluded_native_packages = [pkg for pkg in packages if self._force_pip_install(pkg)]
        for pkg in excluded_native_packages:
            print(f"Force install required for {pkg} -> will install via pip: {pkg}")
            packages.remove(pkg)

        # Build set of explicitly-requested package names (normalized).
        # Handles formats: "pkg==1.0.0", "pkg>=1.0", "pkg", "pkg[extras]".
        requested_package_names = set()
        for pkg in packages:
            name = (
                pkg.split("==")[0]
                .split(">=")[0]
                .split("<=")[0]
                .split("<")[0]
                .split(">")[0]
                .split("[")[0]
                .strip()
            )
            requested_package_names.add(name.lower().replace("_", "-"))

        # Download all packages into a temp folder.
        tmp_folder_pypi = tempfile.mkdtemp()
        py_pi_downloader = PyPiDownloader(py_pi_client, tmp_folder_pypi)
        py_pi_downloader.download_packages(packages, python_version=python_version)

        # Shared S3 cache location for Python wheels from the private registry.
        s3_pypi_cache_prefix = "python_wheels/codeartifact_cache"

        cached_wheels: list[str] = []
        wheels_to_upload: list[str] = []
        job_runner_whl: str | None = None

        for file in os.listdir(tmp_folder_pypi):
            if not file.endswith(".whl"):
                continue

            wheel_path = os.path.join(tmp_folder_pypi, file)

            if job_runner_wheel_prefix and file.startswith(job_runner_wheel_prefix):
                job_runner_whl = wheel_path

            if self._has_native_dependencies(file):
                pkg_name, version = self._parse_wheel_filename(file)
                if pkg_name:
                    if pkg_name.lower() in requested_package_names:
                        if pkg_name not in excluded_native_packages:
                            excluded_native_packages.append(f"{pkg_name}=={version}")
                            print(
                                f"Native package (explicitly requested): {file} -> "
                                f"will install via pip: {pkg_name}=={version}"
                            )
                    else:
                        print(f"Native package (transitive dep, skipping): {file}")
                else:
                    print(f"Native package (could not parse name): {file}")
                os.remove(wheel_path)
                continue

            cached_s3_key = f"{s3_pypi_cache_prefix}/{file}"
            cached_s3_path = f"s3://{self.s3_bucket}/{cached_s3_key}"

            try:
                self.s3_client.head_object(Bucket=self.s3_bucket, Key=cached_s3_key)
                print(f"Found cached Python wheel in S3: {cached_s3_path}")
                cached_wheels.append(cached_s3_path)
                # Keep the job runner wheel locally for script extraction.
                if not (job_runner_wheel_prefix and file.startswith(job_runner_wheel_prefix)):
                    os.remove(wheel_path)
            except self.s3_client.exceptions.ClientError as exc:
                if exc.response["Error"]["Code"] == "404":
                    print(f"Python wheel not in cache, will upload: {file}")
                    wheels_to_upload.append(file)
                else:
                    raise

        uploaded_wheels: list[str] = []
        for wheel_file in wheels_to_upload:
            wheel_path = os.path.join(tmp_folder_pypi, wheel_file)
            s3_key = f"{s3_pypi_cache_prefix}/{wheel_file}"
            self.s3_client.upload_file(wheel_path, self.s3_bucket, s3_key)
            uri = f"s3://{self.s3_bucket}/{s3_key}"
            print(f"Uploaded Python wheel to shared cache: {uri}")
            uploaded_wheels.append(uri)

        py_files = ",".join(cached_wheels + uploaded_wheels)
        print(f"Using Python wheels from cache and uploads: {py_files}")

        if excluded_native_packages:
            print(f"Native packages to install via pip: {excluded_native_packages}")

        return py_files, job_runner_whl, tmp_folder_pypi, excluded_native_packages

    def extract_job_runner_scripts(
        self,
        job_runner_whl: str,
        script_names: list[str],
    ) -> dict[str, str]:
        """Extract job-runner scripts from a wheel and upload to S3."""
        if not job_runner_whl or not os.path.exists(job_runner_whl):
            print("No job runner wheel found for script extraction")
            return {}

        script_dir = tempfile.mkdtemp()
        script_locations: dict[str, str] = {}

        extracted_files: list[str] = []
        with zipfile.ZipFile(job_runner_whl, "r") as wheel:
            for file_info in wheel.filelist:
                for script_name in script_names:
                    if file_info.filename.endswith(script_name):
                        wheel.extract(file_info, script_dir)
                        extracted_files.append(file_info.filename)

        print(f"Extracted job runner files: {extracted_files}")

        if extracted_files:
            uploaded_scripts = S3Uploader(self.s3_bucket).upload_directory(
                script_dir, s3_prefix=self.s3_prefix
            )
            for uploaded_path in uploaded_scripts:
                for script_name in script_names:
                    if uploaded_path.endswith(script_name):
                        script_locations[script_name] = uploaded_path
                        print(f"Job runner uploaded to: {uploaded_path}")

        shutil.rmtree(script_dir)

        return script_locations

    def download_and_cache_jars(
        self,
        jar_urls: list[str],
        pre_provisioned_jars: list[str] | None = None,
    ) -> str:
        """Download JARs and cache registry-sourced JARs in shared S3 cache.

        Args:
            jar_urls: JAR URLs to download (HTTP/HTTPS).
            pre_provisioned_jars: Pre-existing ``s3://`` JAR paths.

        Returns:
            Comma-separated string of resulting S3 JAR paths.
        """
        if pre_provisioned_jars is None:
            pre_provisioned_jars = []

        s3_jar_cache_prefix = "scala_jars/codeartifact_cache"

        jars_to_download: list[str] = []
        cached_jars: list[str] = []

        for jar_url in jar_urls:
            if not jar_url or jar_url.strip() == "":
                continue

            jar_filename = os.path.basename(urlparse(jar_url).path)

            if "codeartifact" in jar_url:
                cached_s3_key = f"{s3_jar_cache_prefix}/{jar_filename}"
                cached_s3_path = f"s3://{self.s3_bucket}/{cached_s3_key}"

                try:
                    self.s3_client.head_object(Bucket=self.s3_bucket, Key=cached_s3_key)
                    print(f"Found cached JAR in S3: {cached_s3_path}")
                    cached_jars.append(cached_s3_path)
                except self.s3_client.exceptions.ClientError as exc:
                    if exc.response["Error"]["Code"] == "404":
                        print(f"JAR not in cache, will download: {jar_filename}")
                        jars_to_download.append(jar_url)
                    else:
                        raise
            else:
                # Non-registry URLs always download.
                jars_to_download.append(jar_url)

        tmp_folder_jar = tempfile.mkdtemp()
        uploaded_jars: list[str] = []

        try:
            if jars_to_download:
                http_downloader = HttpDownloader(tmp_folder_jar)
                http_downloader.download_urls(jars_to_download)

            for local_jar in os.listdir(tmp_folder_jar):
                local_jar_path = os.path.join(tmp_folder_jar, local_jar)
                if not os.path.isfile(local_jar_path):
                    continue

                # Decide cache vs job-specific destination based on origin URL.
                jar_from_codeartifact = False
                for download_url in jars_to_download:
                    if local_jar in download_url and "codeartifact" in download_url:
                        jar_from_codeartifact = True
                        break

                if jar_from_codeartifact:
                    s3_key = f"{s3_jar_cache_prefix}/{local_jar}"
                    self.s3_client.upload_file(local_jar_path, self.s3_bucket, s3_key)
                    uri = f"s3://{self.s3_bucket}/{s3_key}"
                    print(f"Uploaded registry JAR to shared cache: {uri}")
                    uploaded_jars.append(uri)
                else:
                    s3_key = f"{self.s3_prefix}/jars/{local_jar}"
                    self.s3_client.upload_file(local_jar_path, self.s3_bucket, s3_key)
                    uri = f"s3://{self.s3_bucket}/{s3_key}"
                    print(f"Uploaded JAR to job-specific location: {uri}")
                    uploaded_jars.append(uri)

        finally:
            shutil.rmtree(tmp_folder_jar)

        return ",".join(cached_jars + uploaded_jars + pre_provisioned_jars)

    def get_codeartifact_maven_repo(
        self,
        domain: str,
        domain_owner: str,
        region: str,
        repository_path: str,
    ) -> str:
        """Return a CodeArtifact Maven repo URL with an embedded auth token.

        Returns an empty string when ``domain``, ``domain_owner``, or
        ``repository_path`` is empty (no Maven mirror configured).
        """
        if not domain or not domain_owner or not repository_path:
            print("CodeArtifact Maven repository not configured; skipping")
            return ""

        codeartifact_client = boto3.client("codeartifact", region_name=region)
        token_response = codeartifact_client.get_authorization_token(
            domain=domain, domainOwner=domain_owner
        )
        token = token_response["authorizationToken"]

        url = (
            f"https://aws:{token}@{domain}-{domain_owner}"
            f".d.codeartifact.{region}.amazonaws.com/{repository_path.lstrip('/')}"
        )
        print(f"CodeArtifact Maven repository available: {url}")
        return url
