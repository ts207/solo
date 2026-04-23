from __future__ import annotations

from pathlib import Path

from project import PROJECT_ROOT

WIDGET_URI = "ui://edge/operator-dashboard.v1.html"
WIDGET_MIME_TYPE = "text/html;profile=mcp-app"


def widget_path() -> Path:
    return PROJECT_ROOT / "apps" / "chatgpt" / "ui" / "operator_dashboard.html"


def load_widget_html() -> str:
    return widget_path().read_text(encoding="utf-8")


def build_widget_resource() -> dict[str, object]:
    return {
        "uri": WIDGET_URI,
        "mimeType": WIDGET_MIME_TYPE,
        "text": load_widget_html(),
        "_meta": {
            "ui": {
                "prefersBorder": True,
                "csp": {
                    "connect_domains": [],
                    "resource_domains": [],
                },
            }
        },
    }
