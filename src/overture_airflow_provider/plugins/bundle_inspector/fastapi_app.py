"""Airflow 3 (FastAPI) adapter for the bundle inspector plugin.

Only imported when FastAPI and Airflow's FastAPI auth dependency are
available (Airflow 3's API server). Route handlers are thin wrappers over
:mod:`_endpoints`; the actual S3/Airflow-Variable logic lives there so it
has no FastAPI dependency and stays shared with the Airflow 2 (Flask)
adapter in ``flask_app.py``.

Endpoints require an authenticated Airflow user (any user who can reach the
API server's session, via ``airflow.api_fastapi.core_api.security.GetUserDep``)
rather than a per-DAG authorization check: bundle browsing isn't scoped to a
single DAG, so there is no ``dag_id`` to authorize against the way Airflow 2's
``has_access_dag`` did.
"""

import pathlib

from airflow.api_fastapi.core_api.security import GetUserDep
from fastapi import FastAPI, Request, Response
from fastapi.responses import HTMLResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from . import _endpoints as ep

_PLUGIN_ROOT = pathlib.Path(__file__).parent
_templates = Jinja2Templates(directory=str(_PLUGIN_ROOT / "templates" / "bundle_inspector"))


def _error_response(e: ep.ApiError):
    from fastapi.responses import JSONResponse

    return JSONResponse({"error": e.message}, status_code=e.status)


def _s3_object_response(result: ep.S3ObjectResponse):
    if result.body is None:
        return Response(status_code=result.status, headers=result.headers)
    return StreamingResponse(result.body, status_code=result.status, headers=result.headers)


def build_ui_app() -> FastAPI:
    """Serve the Bundle Inspector index page and its static assets."""
    app = FastAPI(openapi_url=None)
    app.mount(
        "/static/bundle_inspector",
        StaticFiles(directory=str(_PLUGIN_ROOT / "static" / "bundle_inspector")),
        name="bundle_inspector_static",
    )

    @app.get("/", response_class=HTMLResponse)
    @app.get("/{subpath:path}", response_class=HTMLResponse)
    def index(request: Request, subpath: str | None = None, user: GetUserDep = None):
        return _templates.TemplateResponse(request, "index.html")

    return app


def build_api_app() -> FastAPI:
    """Expose the same JSON/streaming endpoints as the Airflow 2 Flask blueprint."""
    app = FastAPI(openapi_url=None)

    @app.get("/config")
    def config(user: GetUserDep, request: Request):
        return ep.config(request.query_params.get("user"))

    @app.get("/list")
    def list_children(user: GetUserDep, request: Request):
        try:
            return ep.list_children(
                request.query_params.get("prefix", ""), request.query_params.get("user")
            )
        except ep.ApiError as e:
            return _error_response(e)

    @app.get("/users")
    def users(user: GetUserDep):
        return ep.list_users()

    @app.get("/theme-types")
    def theme_types(user: GetUserDep, request: Request):
        return ep.theme_types(
            request.query_params.get("prefix", ""), request.query_params.get("user")
        )

    @app.get("/count")
    def count(user: GetUserDep, request: Request):
        return ep.count(request.query_params.get("prefix", ""), request.query_params.get("user"))

    @app.get("/tree")
    def tree(user: GetUserDep, request: Request):
        return ep.tree(request.query_params.get("prefix", ""), request.query_params.get("user"))

    @app.get("/component-data")
    def component_data(user: GetUserDep, request: Request):
        try:
            return ep.component_data(
                request.query_params.get("prefix", ""), request.query_params.get("user")
            )
        except ep.ApiError as e:
            return _error_response(e)

    @app.get("/parquet-stats")
    def parquet_stats_endpoint(user: GetUserDep, request: Request):
        try:
            return ep.parquet_stats(
                request.query_params.get("prefix", ""), request.query_params.get("user")
            )
        except ep.ApiError as e:
            return _error_response(e)

    @app.get("/presign")
    def presign(user: GetUserDep, request: Request):
        try:
            return ep.presign(
                request.query_params.get("prefix", ""),
                request.query_params.get("suffix", ".pmtiles"),
                request.query_params.get("user"),
            )
        except ep.ApiError as e:
            return _error_response(e)

    @app.get("/resolve")
    def resolve(user: GetUserDep, request: Request):
        try:
            return ep.resolve(
                request.query_params.get("prefix", ""),
                request.query_params.get("suffix", ".pmtiles"),
                request.query_params.get("user"),
            )
        except ep.ApiError as e:
            return _error_response(e)

    @app.api_route("/s3proxy", methods=["GET", "HEAD"])
    def s3proxy(user: GetUserDep, request: Request):
        try:
            result = ep.s3proxy(
                request.query_params.get("key", ""),
                request.query_params.get("user"),
                request.headers.get("Range"),
                request.method == "HEAD",
            )
        except ep.ApiError as e:
            return _error_response(e)
        return _s3_object_response(result)

    @app.get("/athena-query")
    def athena_query_endpoint(user: GetUserDep, request: Request):
        try:
            return ep.athena_query(
                request.query_params.get("sql", ""), request.query_params.get("user")
            )
        except ep.ApiError as e:
            return _error_response(e)

    @app.get("/athena-status")
    def athena_status_endpoint(user: GetUserDep, request: Request):
        try:
            return ep.athena_query_status(request.query_params.get("query_id", ""))
        except ep.ApiError as e:
            return _error_response(e)

    @app.api_route("/athena-csv", methods=["GET", "HEAD"])
    def athena_csv_proxy(user: GetUserDep, request: Request):
        try:
            result = ep.athena_csv_proxy(
                request.query_params.get("query_id", ""),
                request.headers.get("Range"),
                request.method == "HEAD",
            )
        except ep.ApiError as e:
            return _error_response(e)
        return _s3_object_response(result)

    return app
