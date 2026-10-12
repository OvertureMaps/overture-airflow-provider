"""Bundle Inspector plugin for Apache Airflow.

Provides a UI for browsing Overture Maps bundles in S3, organized by
pipeline stage, theme, schema version, and run ID. Registered via
``get_provider_info()``'s ``plugins`` entry (see ``provider_info.py``), so it
loads automatically wherever this package is installed -- no separate
plugins-folder drop-in required.

Built on Airflow 3 FastAPI apps + External Views.
"""

import logging

from airflow.plugins_manager import AirflowPlugin

logger = logging.getLogger(__name__)

_fastapi_apps: list = []
_external_views: list = []

try:
    from .fastapi_app import build_api_app, build_ui_app

    _fastapi_apps = [
        {
            "app": build_ui_app(),
            "url_prefix": "/bundle-inspector",
            "name": "Bundle Inspector UI",
        },
        {
            "app": build_api_app(),
            "url_prefix": "/api/bundle-inspector",
            "name": "Bundle Inspector API",
        },
    ]
    _external_views = [
        {
            "name": "Bundle Inspector",
            "href": "/bundle-inspector/",
            "destination": "nav",
            "category": "Browse",
            "url_route": "bundle-inspector",
        }
    ]
except Exception:
    logger.warning(
        "Bundle Inspector plugin disabled: could not build its FastAPI apps "
        "(FastAPI or Airflow's FastAPI auth dependency unavailable).",
        exc_info=True,
    )


class BundleInspectorPlugin(AirflowPlugin):
    """Airflow plugin for bundle inspection."""

    name = "bundle_inspector"
    fastapi_apps = _fastapi_apps
    external_views = _external_views
