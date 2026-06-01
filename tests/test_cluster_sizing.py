"""Tests for cluster_sizing module."""

import pytest

from overture_airflow_provider.cluster_sizing import (
    AwsGlueClusterSize,
    DatabricksClusterSize,
    InstanceCalculator,
)


class TestAwsGlueClusterSizeResolveExecutionClass:
    """Tests for AwsGlueClusterSize.resolve_execution_class.

    Truth table:
        STANDARD + G.1X → FLEX   (upgrade: G.1X supports FLEX, ~35% cheaper)
        STANDARD + G.2X → FLEX   (upgrade: G.2X supports FLEX, ~35% cheaper)
        STANDARD + G.4X → STANDARD  (G.4X incompatible with FLEX)
        STANDARD + G.8X → STANDARD  (G.8X incompatible with FLEX)
        FLEX     + G.1X → FLEX   (preserve: already optimal)
        FLEX     + G.2X → FLEX   (preserve: already optimal)
        FLEX     + G.4X → STANDARD  (downgrade: prevents Glue API rejection)
        FLEX     + G.8X → STANDARD  (downgrade: prevents Glue API rejection)
    """

    @pytest.mark.parametrize(
        "execution_class, worker_type, expected",
        [
            # STANDARD on compatible types → upgrade to FLEX
            ("STANDARD", "G.1X", "FLEX"),
            ("STANDARD", "G.2X", "FLEX"),
            # STANDARD on incompatible types → keep STANDARD
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
