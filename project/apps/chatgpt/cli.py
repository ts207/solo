from __future__ import annotations

import argparse
import json
import sys

from project.apps.chatgpt.backlog import IMPLEMENTATION_BACKLOG
from project.apps.chatgpt.resources import build_widget_resource
from project.apps.chatgpt.server import build_server_blueprint, serve_streamable_http
from project.apps.chatgpt.tool_catalog import get_profile_metadata, get_tool_catalog


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="edge-chatgpt-app",
        description="Scaffolding and inspection helpers for the Edge ChatGPT app surface.",
    )
    subparsers = parser.add_subparsers(dest="command")

    subparsers.add_parser("backlog", help="Print the current implementation backlog as JSON.")
    blueprint_parser = subparsers.add_parser("blueprint", help="Print the current server blueprint as JSON.")
    blueprint_parser.add_argument("--profile", choices=["operator", "repo"], default="operator")
    blueprint_parser.add_argument("--repo-root", default=None)
    subparsers.add_parser("widget", help="Print the widget resource payload as JSON.")
    tools_parser = subparsers.add_parser("tools", help="Print a summary table of all registered tools.")
    tools_parser.add_argument("--profile", choices=["operator", "repo"], default="operator")
    subparsers.add_parser("status", help="Print a lightweight status summary of the project data.")

    serve_parser = subparsers.add_parser("serve", help="Attempt to start the live MCP server scaffold.")
    serve_parser.add_argument("--profile", choices=["operator", "repo"], default="operator")
    serve_parser.add_argument("--repo-root", default=None)
    serve_parser.add_argument("--host", default="127.0.0.1")
    serve_parser.add_argument("--port", type=int, default=8000)
    serve_parser.add_argument("--path", default="/mcp")

    subparsers.required = True
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)

    if args.command == "backlog":
        print(json.dumps(list(IMPLEMENTATION_BACKLOG), indent=2, sort_keys=True))
        return 0

    if args.command == "blueprint":
        print(json.dumps(build_server_blueprint(profile=args.profile, repo_root=args.repo_root), indent=2, sort_keys=True))
        return 0

    if args.command == "widget":
        print(json.dumps(build_widget_resource(), indent=2, sort_keys=True))
        return 0

    if args.command == "tools":
        metadata = get_profile_metadata(args.profile)
        tool_catalog = get_tool_catalog(args.profile)
        print(f"Profile: {metadata['profile']} ({metadata['app_name']})")
        print(f"{'NAME':<34} | {'CATEGORY':<8} | {'READ':<4} | {'SYNC':<4} | {'TITLE'}")
        print("-" * 100)
        for tool in tool_catalog:
            props = tool.hints
            print(f"{tool.name:<34} | {tool.category:<8} | {'R' if props.read_only else 'W':<4} | {'Sync' if not props.open_world else 'Open':<4} | {tool.title}")
        return 0

    if args.command == "status":
        from project.apps.chatgpt.handlers import (
            _project_program_ids,
            _recent_run_summaries,
            _resolve_data_root,
        )
        data_root = _resolve_data_root(None)
        recent_runs = _recent_run_summaries(data_root, limit=100)
        program_ids = _project_program_ids(data_root, recent_runs)
        print(f"Data root: {data_root}")
        print(f"Programs found: {len(program_ids)}")
        print(f"Runs found: {len(recent_runs)} (recent sample)")
        return 0

    if args.command == "serve":
        serve_streamable_http(host=args.host, port=args.port, path=args.path, profile=args.profile, repo_root=args.repo_root)
        return 0

    parser.print_help()
    return 1


if __name__ == "__main__":
    sys.exit(main())
