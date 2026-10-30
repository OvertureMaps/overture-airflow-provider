"""Tests for cluster_sizing module."""

import pytest

from overture_airflow_provider.cluster_sizing import (
    AwsGlueClusterSize,
    ClusterSize,
    DatabricksClusterSize,
    InstanceCalculator,
    WherobotsClusterSize,
)


class TestAwsGlueClusterSizeResolveExecutionClass:
    """Tests for AwsGlueClusterSize.resolve_execution_class.

    Truth table:
        STANDARD + G.1X → STANDARD  (honored: caller opted out of Flex)
        STANDARD + G.2X → STANDARD  (honored: caller opted out of Flex)
        STANDARD + G.4X → STANDARD
        STANDARD + G.8X → STANDARD
        FLEX     + G.1X → FLEX
        FLEX     + G.2X → FLEX
        FLEX     + G.4X → STANDARD  (downgrade: prevents Glue API rejection)
        FLEX     + G.8X → STANDARD  (downgrade: prevents Glue API rejection)
    """

    @pytest.mark.parametrize(
        "execution_class, worker_type, expected",
        [
            # STANDARD is always honored
            ("STANDARD", "G.1X", "STANDARD"),
            ("STANDARD", "G.2X", "STANDARD"),
            ("STANDARD", "G.4X", "STANDARD"),
            ("STANDARD", "G.8X", "STANDARD"),
            # FLEX on compatible types → keep FLEX
            ("FLEX", "G.1X", "FLEX"),
            ("FLEX", "G.2X", "FLEX"),
            # FLEX on incompatible types → downgrade to STANDARD
            ("FLEX", "G.4X", "STANDARD"),
            ("FLEX", "G.8X", "STANDARD"),
        ],
    )
    def test_resolve_execution_class(self, execution_class: str, worker_type: str, expected: str):
        result = AwsGlueClusterSize.resolve_execution_class(execution_class, worker_type)
        assert result == expected


class TestDatabricksClusterSizeCustomInstanceTypes:
    """Generic custom-node-type sizing (the GPU support mechanism).

    Callers enable GPU (or any custom SKU) by supplying their own
    ``instance_types`` catalog instead of toggling a flag.
    """

    GPU_TYPES = {"Standard_NC8as_T4_v3": 8, "Standard_NC16as_T4_v3": 16}

    def test_default_uses_builtin_cpu_catalog(self):
        result = DatabricksClusterSize.from_desired_cores(40)
        assert result["node_type_id"] in {
            "Standard_E4a_v4",
            "Standard_E8a_v4",
            "Standard_E16a_v4",
            "Standard_E20a_v4",
            "Standard_E32a_v4",
            "Standard_E48a_v4",
            "Standard_E64a_v4",
            "Standard_E96a_v4",
        }
        assert result["driver_node_type_id"] == "Standard_E4a_v4"

    def test_custom_catalog_selects_gpu_worker_type(self):
        result = DatabricksClusterSize.from_desired_cores(32, instance_types=self.GPU_TYPES)
        assert result["node_type_id"] in self.GPU_TYPES

    def test_custom_driver_node_type_applied(self):
        result = DatabricksClusterSize.from_desired_cores(
            32,
            instance_types=self.GPU_TYPES,
            driver_node_type="Standard_NC8as_T4_v3",
        )
        assert result["driver_node_type_id"] == "Standard_NC8as_T4_v3"

    def test_desired_workers_pins_gpu_worker_count(self):
        result = DatabricksClusterSize.from_desired_cores(
            32, desired_workers=4, instance_types={"Standard_NC8as_T4_v3": 8}
        )
        assert result["autoscale"]["min_workers"] == 4
        assert result["autoscale"]["max_workers"] == 4


class TestInstanceCalculatorValidation:
    def test_rejects_zero_core_instance_type(self):
        with pytest.raises(ValueError, match="positive integer"):
            InstanceCalculator.calculate_instances(
                min_instance_count=1,
                desired_cores=16,
                instance_types={"BadSKU": 0},
            )

    def test_rejects_non_int_core_count(self):
        with pytest.raises(ValueError, match="positive integer"):
            InstanceCalculator.calculate_instances(
                min_instance_count=1,
                desired_cores=16,
                instance_types={"BadSKU": "8"},
            )

    def test_rejects_non_positive_desired_cores(self):
        with pytest.raises(ValueError, match="positive"):
            InstanceCalculator.calculate_instances(1, 0, {"G.1X": 4})

    def test_rejects_empty_instance_types(self):
        with pytest.raises(ValueError, match="empty"):
            InstanceCalculator.calculate_instances(1, 16, {})

    def test_desired_workers_path(self):
        # desired_workers triggers the proximity branch (lines 206-218)
        instance_type, count = InstanceCalculator.calculate_instances(
            min_instance_count=2,
            desired_cores=16,
            instance_types={"G.1X": 4, "G.2X": 8},
            desired_workers=4,
        )
        assert count >= 2
        assert instance_type in {"G.1X", "G.2X"}


# ─── ClusterSize.from_str ─────────────────────────────────────────────────────


class TestClusterSizeFromStr:
    def test_valid_lowercase(self):
        from overture_airflow_provider.cluster_sizing import ClusterSize

        assert ClusterSize.from_str("xs") == ClusterSize.XS
        assert ClusterSize.from_str("xl") == ClusterSize.XL

    def test_invalid_raises(self):
        from overture_airflow_provider.cluster_sizing import ClusterSize

        with pytest.raises(ValueError, match="not a valid ClusterSize"):
            ClusterSize.from_str("HUGE")


# ─── from_cluster_size shortcuts ─────────────────────────────────────────────


def test_glue_from_cluster_size():
    from overture_airflow_provider.cluster_sizing import AwsGlueClusterSize, ClusterSize

    result = AwsGlueClusterSize.from_cluster_size(ClusterSize.XS)
    assert result == {"WorkerType": "G.1X", "NumberOfWorkers": 2}


def test_databricks_from_cluster_size():
    from overture_airflow_provider.cluster_sizing import ClusterSize, DatabricksClusterSize

    result = DatabricksClusterSize.from_cluster_size(ClusterSize.S)
    assert result["node_type_id"] == "Standard_E8a_v4"
    assert result["autoscale"]["min_workers"] == 5


# ─── cloud-specific attributes (#78) ──────────────────────────────────────────


class TestDatabricksClusterSizeCloudAttributes:
    """Databricks rejects a cluster spec carrying the wrong cloud's attributes
    key (e.g. `azure_attributes` on an AWS workspace), so exactly one of
    `aws_attributes`/`azure_attributes`/`gcp_attributes` must be present and it
    must match the target workspace's cloud.
    """

    def test_default_cloud_is_azure(self):
        result = DatabricksClusterSize.from_desired_cores(40)
        assert "azure_attributes" in result
        assert "aws_attributes" not in result
        assert "gcp_attributes" not in result

    def test_aws_cloud_uses_aws_attributes_and_ec2_catalog(self):
        result = DatabricksClusterSize.from_desired_cores(40, cloud="aws")
        assert "aws_attributes" in result
        assert "azure_attributes" not in result
        assert result["node_type_id"] in {
            "m5d.xlarge",
            "m5d.2xlarge",
            "m5d.4xlarge",
            "m5d.8xlarge",
            "m5d.12xlarge",
            "m5d.16xlarge",
            "m5d.24xlarge",
        }
        assert result["driver_node_type_id"] == "m5d.xlarge"

    def test_gcp_cloud_uses_gcp_attributes(self):
        result = DatabricksClusterSize.from_desired_cores(
            40, instance_types={"n1-standard-8": 8}, cloud="gcp"
        )
        assert "gcp_attributes" in result
        assert "aws_attributes" not in result
        assert "azure_attributes" not in result

    def test_explicit_instance_types_win_over_cloud_default_catalog(self):
        # A caller-supplied catalog (e.g. pinned GPU SKUs) still applies
        # regardless of which cloud's attributes key gets set.
        result = DatabricksClusterSize.from_desired_cores(
            32, instance_types={"my.custom.sku": 8}, cloud="aws"
        )
        assert result["node_type_id"] == "my.custom.sku"
        assert "aws_attributes" in result

    def test_from_cluster_size_aws(self):
        result = DatabricksClusterSize.from_cluster_size(ClusterSize.S, cloud="aws")
        assert result["node_type_id"] == "m5d.2xlarge"
        assert result["driver_node_type_id"] == "m5d.xlarge"
        assert "aws_attributes" in result

    def test_aws_instance_profile_arn_omitted_by_default(self):
        # #90: no instance_profile_arn override means no key at all, matching
        # prior behavior for callers who don't set one.
        result = DatabricksClusterSize.from_desired_cores(40, cloud="aws")
        assert "instance_profile_arn" not in result["aws_attributes"]

    def test_aws_instance_profile_arn_set_from_desired_cores(self):
        result = DatabricksClusterSize.from_desired_cores(
            40,
            cloud="aws",
            aws_instance_profile_arn="arn:aws:iam::123456789012:instance-profile/spark-logs",
        )
        assert (
            result["aws_attributes"]["instance_profile_arn"]
            == "arn:aws:iam::123456789012:instance-profile/spark-logs"
        )

    def test_aws_instance_profile_arn_set_from_cluster_size(self):
        result = DatabricksClusterSize.from_cluster_size(
            ClusterSize.S,
            cloud="aws",
            aws_instance_profile_arn="arn:aws:iam::123456789012:instance-profile/spark-logs",
        )
        assert (
            result["aws_attributes"]["instance_profile_arn"]
            == "arn:aws:iam::123456789012:instance-profile/spark-logs"
        )

    def test_aws_instance_profile_arn_ignored_on_non_aws_clouds(self):
        result = DatabricksClusterSize.from_desired_cores(
            40,
            cloud="azure",
            aws_instance_profile_arn="arn:aws:iam::123456789012:instance-profile/spark-logs",
        )
        assert "aws_attributes" not in result

    def test_default_spot_availability_and_bid_price_unchanged(self):
        # Existing behavior when no overrides are passed.
        aws_result = DatabricksClusterSize.from_desired_cores(40, cloud="aws")
        assert aws_result["aws_attributes"]["availability"] == "SPOT_WITH_FALLBACK"
        assert aws_result["aws_attributes"]["spot_bid_price_percent"] == 100

        azure_result = DatabricksClusterSize.from_desired_cores(40, cloud="azure")
        assert azure_result["azure_attributes"]["availability"] == "SPOT_WITH_FALLBACK_AZURE"
        assert azure_result["azure_attributes"]["spot_bid_max_price"] == -1

    def test_availability_override_applies_per_cloud(self):
        aws_result = DatabricksClusterSize.from_desired_cores(
            40, cloud="aws", availability="ON_DEMAND"
        )
        assert aws_result["aws_attributes"]["availability"] == "ON_DEMAND"

        azure_result = DatabricksClusterSize.from_desired_cores(
            40, cloud="azure", availability="ON_DEMAND"
        )
        assert azure_result["azure_attributes"]["availability"] == "ON_DEMAND"

    def test_aws_spot_bid_price_percent_override(self):
        result = DatabricksClusterSize.from_cluster_size(
            ClusterSize.S, cloud="aws", spot_bid_price_percent=50
        )
        assert result["aws_attributes"]["spot_bid_price_percent"] == 50

    def test_azure_spot_bid_max_price_override(self):
        result = DatabricksClusterSize.from_cluster_size(
            ClusterSize.S, cloud="azure", spot_bid_max_price=25
        )
        assert result["azure_attributes"]["spot_bid_max_price"] == 25


def test_wherobots_from_desired_cores_all_branches():
    # Hit every range branch + the None fallback
    assert WherobotsClusterSize.from_desired_cores(50) is not None  # S
    assert WherobotsClusterSize.from_desired_cores(200) is not None  # M
    assert WherobotsClusterSize.from_desired_cores(600) is not None  # L
    assert WherobotsClusterSize.from_desired_cores(1500) is not None  # XL
    assert WherobotsClusterSize.from_desired_cores(3000) is not None  # XXL
    assert WherobotsClusterSize.from_desired_cores(-1) is None  # None path


class TestWherobotsClusterSizeFromClusterSize:
    @pytest.mark.parametrize(
        "name, expected",
        [
            ("XS", "TINY"),
            ("s", "SMALL"),
            ("M", "MEDIUM"),
            ("L", "LARGE"),
            ("xl", "X_LARGE"),
            ("XXL", "XX_LARGE"),
        ],
    )
    def test_cluster_size_names_map_to_general_purpose_runtimes(
        self, fake_wherobots_runtime, name, expected
    ):
        assert WherobotsClusterSize.from_cluster_size(name) is fake_wherobots_runtime[expected]

    def test_rejects_native_runtime_name(self, fake_wherobots_runtime):
        with pytest.raises(ValueError, match="not a valid ClusterSize"):
            WherobotsClusterSize.from_cluster_size("large-himem")


class TestWherobotsClusterSizeFromRuntimeName:
    @pytest.mark.parametrize(
        "name", ["large-himem", "LARGE-HIMEM", " large-himem ", "LARGE_HIMEM", "large_himem"]
    )
    def test_value_or_member_name(self, fake_wherobots_runtime, name):
        assert WherobotsClusterSize.from_runtime_name(name) is fake_wherobots_runtime.LARGE_HIMEM

    def test_value_with_leading_digit(self, fake_wherobots_runtime):
        result = WherobotsClusterSize.from_runtime_name("2x-large-himem")
        assert result is fake_wherobots_runtime.XX_LARGE_HIMEM

    def test_rejects_cluster_size_name(self, fake_wherobots_runtime):
        with pytest.raises(ValueError, match="not a valid Wherobots runtime"):
            WherobotsClusterSize.from_runtime_name("XL")

    def test_unknown_name_lists_valid_choices(self, fake_wherobots_runtime):
        with pytest.raises(ValueError, match="not a valid Wherobots runtime") as exc:
            WherobotsClusterSize.from_runtime_name("huge")
        assert "large-himem" in str(exc.value)


def test_installed_wherobots_sdk_resolves_large_himem():
    runtime_cls = getattr(pytest.importorskip("wherobots.db"), "Runtime", None)
    if not isinstance(runtime_cls, type):
        pytest.skip("wherobots SDK is not installed (stubbed)")
    assert WherobotsClusterSize.from_runtime_name("large-himem") is runtime_cls.LARGE_HIMEM
