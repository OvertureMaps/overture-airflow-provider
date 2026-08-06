"""Airflow 2 (Flask/FAB) adapter for the bundle inspector plugin.

Only imported when Flask and ``airflow.www.auth`` are available (Airflow 2's
webserver). Route handlers are thin wrappers over :mod:`_endpoints`; the
actual S3/Airflow-Variable logic lives there so it has no Flask dependency.
"""

from airflow.www.auth import has_access_dag
from flask import Blueprint, Response, jsonify, render_template, request

from . import _endpoints as ep

bundle_inspector_bp = Blueprint(
    "bundle_inspector",
    __name__,
    template_folder="templates",
    static_folder="static",
    static_url_path="/static",
    url_prefix="/bundle-inspector",
)


@bundle_inspector_bp.route("/")
@bundle_inspector_bp.route("/<path:subpath>")
@has_access_dag("GET")
def index(subpath=None):
    """Render the Bundle Inspector view."""
    return render_template("bundle_inspector/index.html")


bundle_inspector_api_bp = Blueprint(
    "bundle_inspector_api",
    __name__,
    url_prefix="/api/bundle-inspector",
)


def _handle(fn, *args, **kwargs):
    """Call an ``_endpoints`` function and translate ``ApiError`` to a JSON response."""
    try:
        return jsonify(fn(*args, **kwargs))
    except ep.ApiError as e:
        return jsonify({"error": e.message}), e.status


def _s3_object_response(result: ep.S3ObjectResponse) -> Response:
    if result.body is None:
        return Response(status=result.status, headers=result.headers)
    return Response(result.body, status=result.status, headers=result.headers)


@bundle_inspector_api_bp.route("/config", methods=["GET"])
@has_access_dag("GET")
def config():
    return _handle(ep.config, request.args.get("user"))


@bundle_inspector_api_bp.route("/list", methods=["GET"])
@has_access_dag("GET")
def list_children():
    return _handle(ep.list_children, request.args.get("prefix", ""), request.args.get("user"))


@bundle_inspector_api_bp.route("/users", methods=["GET"])
@has_access_dag("GET")
def users():
    return _handle(ep.list_users)


@bundle_inspector_api_bp.route("/theme-types", methods=["GET"])
@has_access_dag("GET")
def theme_types():
    return _handle(ep.theme_types, request.args.get("prefix", ""), request.args.get("user"))


@bundle_inspector_api_bp.route("/count", methods=["GET"])
@has_access_dag("GET")
def count():
    return _handle(ep.count, request.args.get("prefix", ""), request.args.get("user"))


@bundle_inspector_api_bp.route("/tree", methods=["GET"])
@has_access_dag("GET")
def tree():
    return _handle(ep.tree, request.args.get("prefix", ""), request.args.get("user"))


@bundle_inspector_api_bp.route("/component-data", methods=["GET"])
@has_access_dag("GET")
def component_data():
    return _handle(ep.component_data, request.args.get("prefix", ""), request.args.get("user"))


@bundle_inspector_api_bp.route("/parquet-stats", methods=["GET"])
@has_access_dag("GET")
def parquet_stats_endpoint():
    return _handle(ep.parquet_stats, request.args.get("prefix", ""), request.args.get("user"))


@bundle_inspector_api_bp.route("/presign", methods=["GET"])
@has_access_dag("GET")
def presign():
    return _handle(
        ep.presign,
        request.args.get("prefix", ""),
        request.args.get("suffix", ".pmtiles"),
        request.args.get("user"),
    )


@bundle_inspector_api_bp.route("/resolve", methods=["GET"])
@has_access_dag("GET")
def resolve():
    return _handle(
        ep.resolve,
        request.args.get("prefix", ""),
        request.args.get("suffix", ".pmtiles"),
        request.args.get("user"),
    )


@bundle_inspector_api_bp.route("/s3proxy", methods=["GET", "HEAD"])
@has_access_dag("GET")
def s3proxy():
    try:
        result = ep.s3proxy(
            request.args.get("key", ""),
            request.args.get("user"),
            request.headers.get("Range"),
            request.method == "HEAD",
        )
    except ep.ApiError as e:
        return jsonify({"error": e.message}), e.status
    return _s3_object_response(result)


@bundle_inspector_api_bp.route("/athena-query", methods=["GET"])
@has_access_dag("GET")
def athena_query_endpoint():
    return _handle(ep.athena_query, request.args.get("sql", ""), request.args.get("user"))


@bundle_inspector_api_bp.route("/athena-status", methods=["GET"])
@has_access_dag("GET")
def athena_status_endpoint():
    return _handle(ep.athena_query_status, request.args.get("query_id", ""))


@bundle_inspector_api_bp.route("/athena-csv", methods=["GET", "HEAD"])
@has_access_dag("GET")
def athena_csv_proxy():
    try:
        result = ep.athena_csv_proxy(
            request.args.get("query_id", ""),
            request.headers.get("Range"),
            request.method == "HEAD",
        )
    except ep.ApiError as e:
        return jsonify({"error": e.message}), e.status
    return _s3_object_response(result)
