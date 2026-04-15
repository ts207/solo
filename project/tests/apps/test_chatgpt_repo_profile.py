import asyncio

from mcp.types import DEFAULT_NEGOTIATED_VERSION
from starlette.testclient import TestClient

from project.apps.chatgpt.server import build_asgi_app, build_mcp_server, build_server_blueprint
from project.apps.chatgpt.tool_catalog import get_profile_metadata, get_tool_catalog, get_tool_definition


def test_repo_profile_blueprint_is_read_only_and_has_no_widget() -> None:
    blueprint = build_server_blueprint(profile="repo", repo_root="/tmp/repo")

    assert blueprint["app"]["name"] == "Edge Repository"
    assert blueprint["app"]["profile"] == "repo"
    assert blueprint["app"]["repo_root"] == "/tmp/repo"
    assert blueprint["resources"] == []
    assert all(tool["name"].startswith("repo_") for tool in blueprint["tools"])


def test_repo_profile_catalog_and_metadata_are_isolated() -> None:
    metadata = get_profile_metadata("repo")
    tools = get_tool_catalog("repo")

    assert metadata["app_name"] == "Edge Repository"
    assert len(tools) == 6
    assert all(tool.name.startswith("repo_") for tool in tools)
    assert all(tool.hints.read_only for tool in tools)
    assert get_tool_definition("repo_search", profile="repo").name == "repo_search"


def test_repo_fastmcp_registration_only_exposes_repo_tools() -> None:
    server = build_mcp_server(profile="repo")
    tools = asyncio.run(server.list_tools())

    assert tools
    assert all(tool.name.startswith("repo_") for tool in tools)
    assert all(tool.annotations is not None and tool.annotations.readOnlyHint is True for tool in tools)
    assert {tool.name for tool in tools} == {
        "repo_get_status",
        "repo_list_changed_files",
        "repo_git_diff",
        "repo_read_file",
        "repo_read_file_range",
        "repo_search",
    }


def test_repo_streamable_http_initialize_smoke() -> None:
    app = build_asgi_app(profile="repo")
    initialize_request = {
        "jsonrpc": "2.0",
        "id": "initialize-1",
        "method": "initialize",
        "params": {
            "protocolVersion": DEFAULT_NEGOTIATED_VERSION,
            "capabilities": {},
            "clientInfo": {
                "name": "pytest",
                "version": "0.0",
            },
        },
    }

    with TestClient(app) as client:
        health_response = client.get("/")
        assert health_response.status_code == 200
        assert health_response.json()["profile"] == "repo"
        assert health_response.json()["mcp_endpoint"] == "/mcp"

        initialize_response = client.post(
            "/mcp/",
            headers={
                "accept": "application/json, text/event-stream",
                "content-type": "application/json",
            },
            json=initialize_request,
        )

    assert initialize_response.status_code == 200
    payload = initialize_response.json()
    assert payload["result"]["protocolVersion"] == DEFAULT_NEGOTIATED_VERSION
    assert payload["result"]["serverInfo"]["name"] == "Edge Repository"
    assert payload["result"]["capabilities"]["tools"]["listChanged"] is False
