"""Bundle Inspector plugin for Apache Airflow.

Provides a UI for browsing Overture Maps bundles in S3, organized by
pipeline stage, theme, schema version, and run ID. Registered via
``get_provider_info()``'s ``plugins`` entry (see ``provider_info.py``), so it
loads automatically wherever this package is installed -- no separate
plugins-folder drop-in required.

Supports both Airflow 2 (Flask Blueprints + FAB menu items) and Airflow 3
(FastAPI apps + External Views), selected at import time via
``overture_airflow_provider._airflow_compat.AIRFLOW_MAJOR``. Each framework's
adapter module (``flask_app.py`` / ``fastapi_app.py``) is only imported on
the major version that ships that framework, so neither becomes a hard
dependency of the other.
"""

import logging

from airflow.plugins_manager import AirflowPlugin

from overture_airflow_provider._airflow_compat import AIRFLOW_MAJOR

logger = logging.getLogger(__name__)

_flask_blueprints: list = []
_appbuilder_menu_items: list = []
_fastapi_apps: list = []
_external_views: list = []

if AIRFLOW_MAJOR == 2:
    try:
        from .flask_app import bundle_inspector_api_bp, bundle_inspector_bp

        _flask_blueprints = [bundle_inspector_bp, bundle_inspector_api_bp]
        _appbuilder_menu_items = [
            {
                "name": "Bundle Inspector",
                "href": "/bundle-inspector/",
                "category": "Browse",
            }
        ]
    except ImportError:
        logger.warning(
            "Bundle Inspector plugin disabled: Flask is not installed. "
            "Install the Airflow 2 webserver extras to enable it.",
        )
else:
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
    flask_blueprints = _flask_blueprints
    appbuilder_views: list = []
    appbuilder_menu_items = _appbuilder_menu_items
    fastapi_apps = _fastapi_apps
    external_views = _external_views
