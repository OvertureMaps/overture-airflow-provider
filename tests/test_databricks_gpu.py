"""Tests for Databricks GPU discovery and override-merge guards."""

import types
from contextlib import contextmanager

import pytest

import overture_airflow_provider._databricks as dbx
from overture_airflow_provider._databricks import (
    _resolve_databricks_node_config,
    discover_gpu_cluster_options,
)


def _node(node_type_id, num_cores, num_gpus, is_deprecated=False):
    return types.SimpleNamespace(
        node_type_id=node_type_id,
        num_cores=num_cores,
        num_gpus=num_gpus,
        is_deprecated=is_deprecated,
    )


class _FakeClusters:
    def __init__(self, nodes=None, spark_version="15.4.x-gpu-ml-scala2.12"):
        self._nodes = nodes or []
        self._spark_version = spark_version
        self.select_kwargs = None
        self.list_calls = 0
        self.select_calls = 0

    def list_node_types(self):
        self.list_calls += 1
        return types.SimpleNamespace(node_types=self._nodes)

    def select_spark_version(self, **kwargs):
        self.select_calls += 1
        self.select_kwargs = kwargs
        return self._spark_version


def _patch_workspace(monkeypatch, clusters):
    @contextmanager
    def _fake_workspace_client(self):
        yield types.SimpleNamespace(clusters=clusters)

    monkeypatch.setattr(
        "overture_airflow_provider.hooks.DatabricksSdkHook.get_workspace_client",
        _fake_workspace_client,
    )

    class _Conn:
        host = "https://example.cloud.databricks.com"
        login = None
        password = "token"
        extra_dejson = {}

    monkeypatch.setattr(
        "overture_airflow_provider._airflow_compat.BaseHook.get_connection",
        staticmethod(lambda conn_id: _Conn()),
    )


class TestDiscoverGpuClusterOptions:
    def test_filters_to_non_deprecated_gpu_nodes(self, monkeypatch):
        clusters = _FakeClusters(
            [
                _node("Standard_NC8as_T4_v3", 8.0, 1),
                _node("Standard_NC16as_T4_v3", 16.0, 2),
                _node("Standard_E8a_v4", 8.0, 0),  # CPU node
                _node("Standard_E4a_v4", 4.0, 0),  # smaller CPU node
                _node("Old_GPU", 8.0, 1, is_deprecated=True),  # deprecated -> excluded
            ]
        )
        _patch_workspace(monkeypatch, clusters)

        result = discover_gpu_cluster_options("databricks_default")

        assert result["worker_instance_types"] == {
            "Standard_NC8as_T4_v3": 8,
            "Standard_NC16as_T4_v3": 16,
        }
        # driver defaults to the cheapest CPU node (driver needs no GPU)
        assert result["driver_node_type"] == "Standard_E4a_v4"
        assert result["spark_version"] == "15.4.x-gpu-ml-scala2.12"
        assert clusters.select_kwargs == {
            "long_term_support": True,
            "ml": True,
            "gpu": True,
        }

    def test_driver_falls_back_to_gpu_when_no_cpu_node(self, monkeypatch):
        clusters = _FakeClusters(
            [
                _node("Standard_NC8as_T4_v3", 8.0, 1),
                _node("Standard_NC16as_T4_v3", 16.0, 2),
            ]
        )
        _patch_workspace(monkeypatch, clusters)

        result = discover_gpu_cluster_options("databricks_default", need_runtime=False)

        # no CPU node available -> driver falls back to the smallest GPU node
        assert result["driver_node_type"] == "Standard_NC8as_T4_v3"

    def test_need_nodes_only_skips_runtime_call(self, monkeypatch):
        clusters = _FakeClusters([_node("Standard_NC8as_T4_v3", 8.0, 1)])
        _patch_workspace(monkeypatch, clusters)

        result = discover_gpu_cluster_options("databricks_default", need_runtime=False)

        assert clusters.list_calls == 1
        assert clusters.select_calls == 0
        assert result["spark_version"] is None

    def test_need_runtime_only_skips_node_call(self, monkeypatch):
        clusters = _FakeClusters()
        _patch_workspace(monkeypatch, clusters)

        result = discover_gpu_cluster_options("databricks_default", need_nodes=False)

        assert clusters.list_calls == 0
        assert clusters.select_calls == 1
        assert result["worker_instance_types"] is None
        assert result["spark_version"] == "15.4.x-gpu-ml-scala2.12"

    def test_raises_when_no_gpu_nodes(self, monkeypatch):
        clusters = _FakeClusters([_node("Standard_E8a_v4", 8.0, 0)])
        _patch_workspace(monkeypatch, clusters)

        with pytest.raises(ValueError, match="no\\s+GPU-capable node types"):
            discover_gpu_cluster_options("databricks_default")


class TestResolveDatabricksNodeConfig:
    _DISCOVERED = {
        "worker_instance_types": {"Standard_NC8as_T4_v3": 8},
        "driver_node_type": "Standard_NC8as_T4_v3",
        "spark_version": "15.4.x-gpu-ml-scala2.12",
    }

    def _spy(self, monkeypatch):
        """Patch discovery with a spy capturing call count and need_* flags."""
        calls = []

        def _fake(conn_id, *, need_nodes=True, need_runtime=True):
            calls.append({"need_nodes": need_nodes, "need_runtime": need_runtime})
            out = {"worker_instance_types": None, "driver_node_type": None, "spark_version": None}
            if need_nodes:
                out["worker_instance_types"] = self._DISCOVERED["worker_instance_types"]
                out["driver_node_type"] = self._DISCOVERED["driver_node_type"]
            if need_runtime:
                out["spark_version"] = self._DISCOVERED["spark_version"]
            return out

        monkeypatch.setattr(dbx, "discover_gpu_cluster_options", _fake)
        return calls

    def test_gpu_false_makes_no_calls(self, monkeypatch):
        calls = self._spy(monkeypatch)
        result = _resolve_databricks_node_config({"databricks_gpu": False})
        assert calls == []
        assert result == {
            "worker_instance_types": None,
            "driver_node_type": None,
            "spark_version": None,
        }

    def test_gpu_true_fills_all_from_discovery(self, monkeypatch):
        calls = self._spy(monkeypatch)
        result = _resolve_databricks_node_config({"databricks_gpu": True, "databricks_conf": {}})
        assert calls == [{"need_nodes": True, "need_runtime": True}]
        assert result == self._DISCOVERED

    def test_pinned_runtime_skips_runtime_lookup(self, monkeypatch):
        calls = self._spy(monkeypatch)
        result = _resolve_databricks_node_config(
            {
                "databricks_gpu": True,
                "databricks_conf": {},
                "databricks_spark_version": "15.4.x-gpu-ml-scala2.12",
            }
        )
        assert calls == [{"need_nodes": True, "need_runtime": False}]
        assert result["spark_version"] == "15.4.x-gpu-ml-scala2.12"

    def test_pinned_nodes_and_driver_skip_node_lookup(self, monkeypatch):
        calls = self._spy(monkeypatch)
        result = _resolve_databricks_node_config(
            {
                "databricks_gpu": True,
                "databricks_conf": {},
                "databricks_worker_instance_types": {"My_GPU": 8},
                "databricks_driver_node_type": "My_GPU",
            }
        )
        assert calls == [{"need_nodes": False, "need_runtime": True}]
        assert result["worker_instance_types"] == {"My_GPU": 8}
        assert result["driver_node_type"] == "My_GPU"
        assert result["spark_version"] == self._DISCOVERED["spark_version"]

    def test_pinned_workers_only_still_needs_node_lookup_for_driver(self, monkeypatch):
        calls = self._spy(monkeypatch)
        result = _resolve_databricks_node_config(
            {
                "databricks_gpu": True,
                "databricks_conf": {},
                "databricks_worker_instance_types": {"My_GPU": 8},
            }
        )
        # driver missing -> node lookup still required; explicit workers win
        assert calls == [{"need_nodes": True, "need_runtime": True}]
        assert result["worker_instance_types"] == {"My_GPU": 8}
        assert result["driver_node_type"] == "Standard_NC8as_T4_v3"

    def test_all_explicit_makes_no_calls(self, monkeypatch):
        calls = self._spy(monkeypatch)
        result = _resolve_databricks_node_config(
            {
                "databricks_gpu": True,
                "databricks_conf": {},
                "databricks_worker_instance_types": {"My_GPU": 8},
                "databricks_driver_node_type": "My_GPU",
                "databricks_spark_version": "15.4.x-gpu-ml-scala2.12",
            }
        )
        assert calls == []
        assert result == {
            "worker_instance_types": {"My_GPU": 8},
            "driver_node_type": "My_GPU",
            "spark_version": "15.4.x-gpu-ml-scala2.12",
        }

    def test_warns_on_non_gpu_explicit_runtime(self, monkeypatch, capsys):
        self._spy(monkeypatch)
        _resolve_databricks_node_config(
            {
                "databricks_gpu": True,
                "databricks_conf": {},
                "databricks_worker_instance_types": {"My_GPU": 8},
                "databricks_driver_node_type": "My_GPU",
                "databricks_spark_version": "15.4.x-scala2.12",  # not GPU
            }
        )
        assert "does not look GPU-enabled" in capsys.readouterr().out
