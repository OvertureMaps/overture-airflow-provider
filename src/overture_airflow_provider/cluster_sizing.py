"""Cluster size enums and instance-type resolution for each Spark platform.

Pure-Python except for ``WherobotsClusterSize``, which lazily imports the
``wherobots.db`` SDK only when used.
"""

from enum import IntEnum, auto


class ClusterSize(IntEnum):
    XS = auto()
    S = auto()
    M = auto()
    L = auto()
    XL = auto()
    XXL = auto()

    @classmethod
    def from_str(cls, name: str) -> "ClusterSize":
        normalized = name.upper()
        if normalized in cls.__members__:
            return cls[normalized]
        raise ValueError(f"{name} is not a valid ClusterSize")


# AWS Glue ------------------------------------------------------------------

# Worker types: https://docs.aws.amazon.com/glue/latest/dg/aws-glue-api-jobs-job.html
# FLEX execution class is only supported on G.1X and G.2X worker types.
glue_instance_types = {"G.1X": 4, "G.2X": 8, "G.4X": 16, "G.8X": 32}


class AwsGlueClusterSize:
    FLEX_COMPATIBLE_WORKER_TYPES = {"G.1X", "G.2X"}

    mapping = {
        ClusterSize.XS: ("G.1X", 2),  # 8 cpu
        ClusterSize.S: ("G.2X", 5),  # 40 cpu
        ClusterSize.M: ("G.2X", 20),  # 160 cpu
        ClusterSize.L: ("G.4X", 40),  # 640 cpu
        ClusterSize.XL: ("G.8X", 64),  # 2048 cpu
    }

    @classmethod
    def from_desired_cores(cls, desired_cores: int, desired_workers: int | None = None) -> dict:
        worker_type, number_of_workers = InstanceCalculator.calculate_instances(
            min_instance_count=2,
            desired_cores=desired_cores,
            instance_types=glue_instance_types,
            desired_workers=desired_workers,
        )
        return {"WorkerType": worker_type, "NumberOfWorkers": number_of_workers}

    @classmethod
    def resolve_execution_class(cls, execution_class: str, worker_type: str) -> str:
        if worker_type in cls.FLEX_COMPATIBLE_WORKER_TYPES:
            return "FLEX"
        return "STANDARD"

    @classmethod
    def from_cluster_size(cls, cluster_size: ClusterSize) -> dict:
        worker_type, number_of_workers = cls.mapping[cluster_size]
        return {"WorkerType": worker_type, "NumberOfWorkers": number_of_workers}


# Databricks ---------------------------------------------------------------

azure_databricks_instance_types = {
    "Standard_E4a_v4": 4,
    "Standard_E8a_v4": 8,
    "Standard_E16a_v4": 16,
    "Standard_E20a_v4": 20,
    "Standard_E32a_v4": 32,
    "Standard_E48a_v4": 48,
    "Standard_E64a_v4": 64,
    "Standard_E96a_v4": 96,
}

# EC2 instance types carrying local NVMe storage (the AWS analogue of the
# Azure "a"-series above), sized to roughly the same core ladder. Core counts
# per size: https://aws.amazon.com/ec2/instance-types/m5/
aws_databricks_instance_types = {
    "m5d.xlarge": 4,
    "m5d.2xlarge": 8,
    "m5d.4xlarge": 16,
    "m5d.8xlarge": 32,
    "m5d.12xlarge": 48,
    "m5d.16xlarge": 64,
    "m5d.24xlarge": 96,
}

_DEFAULT_CLOUD = "azure"


class DatabricksClusterSize:
    # Per-cloud (driver, worker, number_of_workers) shortcuts for from_cluster_size.
    mapping = {
        "azure": {
            ClusterSize.XS: ("Standard_E4a_v4", "Standard_E4a_v4", 2),
            ClusterSize.S: ("Standard_E4a_v4", "Standard_E8a_v4", 5),
            ClusterSize.M: ("Standard_E4a_v4", "Standard_E8a_v4", 20),
            ClusterSize.L: ("Standard_E4a_v4", "Standard_E16a_v4", 40),
            ClusterSize.XL: ("Standard_E4a_v4", "Standard_E32a_v4", 64),
        },
        "aws": {
            ClusterSize.XS: ("m5d.xlarge", "m5d.xlarge", 2),
            ClusterSize.S: ("m5d.xlarge", "m5d.2xlarge", 5),
            ClusterSize.M: ("m5d.xlarge", "m5d.2xlarge", 20),
            ClusterSize.L: ("m5d.xlarge", "m5d.4xlarge", 40),
            ClusterSize.XL: ("m5d.xlarge", "m5d.8xlarge", 64),
        },
    }

    _INSTANCE_TYPES = {
        "azure": azure_databricks_instance_types,
        "aws": aws_databricks_instance_types,
    }
    _DEFAULT_DRIVER = {"azure": "Standard_E4a_v4", "aws": "m5d.xlarge"}

    @classmethod
    def _cloud_attributes(
        cls,
        cloud: str,
        aws_instance_profile_arn: str = "",
        availability: str = "",
        spot_bid_price_percent: int | None = None,
        spot_bid_max_price: int | float | None = None,
    ) -> dict:
        """Return the single cloud-specific attributes key the Clusters API accepts.

        Databricks rejects a cluster spec carrying ``azure_attributes`` against
        an AWS workspace (and vice versa), so exactly one of these keys must be
        present, matching the workspace's actual cloud.

        ``availability``/``spot_bid_price_percent``/``spot_bid_max_price``
        override the provider's spot-with-fallback defaults; leave them unset
        to keep today's hardcoded behavior (e.g. set ``availability`` to
        ``"ON_DEMAND"`` to opt a workspace out of spot entirely).
        """
        if cloud == "aws":
            aws_attributes = {
                "first_on_demand": 1,
                "availability": availability or "SPOT_WITH_FALLBACK",
                "zone_id": "auto",
                "spot_bid_price_percent": (
                    100 if spot_bid_price_percent is None else spot_bid_price_percent
                ),
            }
            if aws_instance_profile_arn:
                aws_attributes["instance_profile_arn"] = aws_instance_profile_arn
            return {"aws_attributes": aws_attributes}
        if cloud == "gcp":
            return {
                "gcp_attributes": {
                    "availability": availability or "PREEMPTIBLE_WITH_FALLBACK_GCP",
                }
            }
        return {
            "azure_attributes": {
                "first_on_demand": 1,
                "availability": availability or "SPOT_WITH_FALLBACK_AZURE",
                "spot_bid_max_price": (-1 if spot_bid_max_price is None else spot_bid_max_price),
            }
        }

    @classmethod
    def as_json(
        cls,
        driver_node_type,
        worker_node_type,
        number_of_workers,
        cloud: str = _DEFAULT_CLOUD,
        aws_instance_profile_arn: str = "",
        availability: str = "",
        spot_bid_price_percent: int | None = None,
        spot_bid_max_price: int | float | None = None,
    ) -> dict:
        return {
            "node_type_id": worker_node_type,
            "driver_node_type_id": driver_node_type,
            "autoscale": {
                "min_workers": number_of_workers,
                "max_workers": number_of_workers,
            },
            **cls._cloud_attributes(
                cloud,
                aws_instance_profile_arn,
                availability=availability,
                spot_bid_price_percent=spot_bid_price_percent,
                spot_bid_max_price=spot_bid_max_price,
            ),
        }

    @classmethod
    def from_desired_cores(
        cls,
        desired_cores: int,
        desired_workers: int | None = None,
        *,
        instance_types: dict | None = None,
        driver_node_type: str | None = None,
        cloud: str = _DEFAULT_CLOUD,
        aws_instance_profile_arn: str = "",
        availability: str = "",
        spot_bid_price_percent: int | None = None,
        spot_bid_max_price: int | float | None = None,
    ) -> dict:
        driver_node_type = driver_node_type or cls._DEFAULT_DRIVER.get(
            cloud, cls._DEFAULT_DRIVER[_DEFAULT_CLOUD]
        )
        worker_node_type, number_of_workers = InstanceCalculator.calculate_instances(
            min_instance_count=1,
            desired_cores=desired_cores,
            instance_types=(
                instance_types
                or cls._INSTANCE_TYPES.get(cloud, cls._INSTANCE_TYPES[_DEFAULT_CLOUD])
            ),
            desired_workers=desired_workers,
        )
        return cls.as_json(
            driver_node_type,
            worker_node_type,
            number_of_workers,
            cloud=cloud,
            aws_instance_profile_arn=aws_instance_profile_arn,
            availability=availability,
            spot_bid_price_percent=spot_bid_price_percent,
            spot_bid_max_price=spot_bid_max_price,
        )

    @classmethod
    def from_cluster_size(
        cls,
        cluster_size: ClusterSize,
        cloud: str = _DEFAULT_CLOUD,
        aws_instance_profile_arn: str = "",
        availability: str = "",
        spot_bid_price_percent: int | None = None,
        spot_bid_max_price: int | float | None = None,
    ) -> dict:
        cloud_mapping = cls.mapping.get(cloud, cls.mapping[_DEFAULT_CLOUD])
        driver_node_type, worker_node_type, number_of_workers = cloud_mapping[cluster_size]
        return cls.as_json(
            driver_node_type,
            worker_node_type,
            number_of_workers,
            cloud=cloud,
            aws_instance_profile_arn=aws_instance_profile_arn,
            availability=availability,
            spot_bid_price_percent=spot_bid_price_percent,
            spot_bid_max_price=spot_bid_max_price,
        )


# Wherobots ----------------------------------------------------------------


class WherobotsClusterSize:
    @classmethod
    def _get_runtime_mapping(cls) -> dict:
        from wherobots.db import Runtime  # local import: optional SDK

        return {
            ClusterSize.XS: Runtime.TINY,
            ClusterSize.S: Runtime.SMALL,
            ClusterSize.M: Runtime.MEDIUM,
            ClusterSize.L: Runtime.LARGE,
            ClusterSize.XL: Runtime.X_LARGE,
            ClusterSize.XXL: Runtime.XX_LARGE,
        }

    @classmethod
    def from_desired_cores(cls, desired_cores: int):
        mapping = cls._get_runtime_mapping()

        if 0 <= desired_cores <= 100:
            return mapping[ClusterSize.S]
        if 101 <= desired_cores <= 500:
            return mapping[ClusterSize.M]
        if 501 <= desired_cores <= 1000:
            return mapping[ClusterSize.L]
        if 1001 <= desired_cores <= 2000:
            return mapping[ClusterSize.XL]
        if desired_cores > 2001:
            return mapping[ClusterSize.XXL]
        return None

    @classmethod
    def from_cluster_size(cls, cluster_size: str):
        mapping = cls._get_runtime_mapping()
        return mapping[ClusterSize.from_str(cluster_size)]


# Shared helper -------------------------------------------------------------


class InstanceCalculator:
    @classmethod
    def calculate_instances(
        cls,
        min_instance_count: int,
        desired_cores: int,
        instance_types: dict,
        desired_workers: int | None = None,
    ):
        """Pick (instance_type, count) meeting the desired core (and worker) constraints.

        With ``desired_workers``: pick the worker type whose required count
        lands closest to ``desired_workers`` while still meeting ``desired_cores``.

        Without it: pick the configuration with the fewest workers that still
        meets ``desired_cores``.

        ``min_instance_count`` is a floor enforced by the platform (e.g. 2 for
        AWS Glue) and is independent of ``desired_workers``.
        """
        if desired_cores <= 0:
            raise ValueError("Desired cores must be a positive integer")
        if not instance_types:
            raise ValueError("Instance types must not be empty")
        for instance_type, cores_per_instance in instance_types.items():
            if not isinstance(cores_per_instance, int) or cores_per_instance <= 0:
                raise ValueError(
                    f"Instance type {instance_type!r} must map to a positive integer "
                    f"core count, got {cores_per_instance!r}"
                )

        best_instance_type = None
        min_instances = float("inf")
        best_diff = float("inf")

        if desired_workers is not None:
            for instance_type, cores_per_instance in instance_types.items():
                required = max(
                    min_instance_count,
                    (desired_cores + cores_per_instance - 1) // cores_per_instance,
                )
                diff = abs(required - desired_workers)
                if required * cores_per_instance >= desired_cores and diff < best_diff:
                    best_diff = diff
                    min_instances = required
                    best_instance_type = instance_type
            if best_instance_type is not None:
                return best_instance_type, min_instances

        for instance_type, cores_per_instance in instance_types.items():
            required = max(
                min_instance_count,
                (desired_cores + cores_per_instance - 1) // cores_per_instance,
            )
            if required < min_instances and required * cores_per_instance >= desired_cores:
                min_instances = required
                best_instance_type = instance_type

        if best_instance_type is None:
            raise ValueError("Unable to find a suitable instance type for the desired cores")

        return best_instance_type, min_instances
