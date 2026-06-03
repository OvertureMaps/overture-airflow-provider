"""Tests for the shared coerce_config_dict helper.

It is the single parser for every JSON-string-or-dict config op_kwarg (the four
IcebergConfig variants and extra_spark_conf), so it must accept both a JSON
object string and an already-parsed dict (the render_template_as_native_obj=True
case) while rejecting non-object payloads with a field-named error.
"""

import pytest

from overture_airflow_provider.config import coerce_config_dict


def test_parses_json_object_string():
    assert coerce_config_dict('{"a": "b", "n": 3000}') == {"a": "b", "n": 3000}


def test_accepts_dict_as_is():
    value = {"a": "b", "n": 3000}
    assert coerce_config_dict(value) is value


def test_empty_and_falsy_become_empty_dict():
    assert coerce_config_dict("{}") == {}
    assert coerce_config_dict("") == {}
    assert coerce_config_dict(None) == {}


def test_invalid_json_uses_field_name():
    with pytest.raises(ValueError, match=r"Invalid JSON in extra_spark_conf"):
        coerce_config_dict('{"bad"', field_name="extra_spark_conf")


def test_non_object_json_uses_field_name():
    with pytest.raises(
        ValueError,
        match=r"IcebergConfig\.spark_config must decode to a JSON object, got list",
    ):
        coerce_config_dict("[]", field_name="IcebergConfig.spark_config")


def test_native_rendered_empty_list_is_rejected_not_swallowed():
    """A native-rendered empty JSON array is falsy but is NOT a "no config"
    placeholder; it must raise rather than be silently treated as {}."""
    with pytest.raises(
        ValueError,
        match=r"IcebergConfig\.spark_config must decode to a JSON object, got list",
    ):
        coerce_config_dict([], field_name="IcebergConfig.spark_config")


def test_native_rendered_nonempty_list_is_rejected():
    with pytest.raises(ValueError, match=r"must decode to a JSON object, got list"):
        coerce_config_dict([{"a": 1}], field_name="IcebergConfig.spark_config")


def test_native_rendered_scalar_is_rejected():
    with pytest.raises(ValueError, match=r"must decode to a JSON object, got int"):
        coerce_config_dict(0, field_name="extra_spark_conf")
