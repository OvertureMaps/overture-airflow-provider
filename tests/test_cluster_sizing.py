"""Tests for cluster_sizing module."""

import pytest

from overture_airflow_provider.cluster_sizing import AwsGlueClusterSize


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
