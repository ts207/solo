import asyncio

from mcp.types import DEFAULT_NEGOTIATED_VERSION
from starlette.testclient import TestClient

from project.apps.chatgpt.server import _build_runtime_wrapper, build_asgi_app, build_mcp_server
from project.apps.chatgpt.tool_catalog import get_tool_definition


def test_fastmcp_registration_carries_titles_and_annotations() -> None:
    server = build_mcp_server()

    compare_tool = server._tool_manager.get_tool("edge_compare_runs")
    issue_tool = server._tool_manager.get_tool("edge_issue_run")

    assert compare_tool is not None
    assert compare_tool.title == "Compare Edge runs"
    assert compare_tool.annotations is not None
    assert compare_tool.annotations.readOnlyHint is True
    assert compare_tool.annotations.openWorldHint is False

    assert issue_tool is not None
    assert issue_tool.title == "Issue Edge run"
    assert issue_tool.annotations is not None
    assert issue_tool.annotations.readOnlyHint is False
    assert issue_tool.annotations.destructiveHint is False


def test_list_tools_emits_openai_meta_fields() -> None:
    server = build_mcp_server()
    tools = asyncio.run(server.list_tools())

    render_tool = next(tool for tool in tools if tool.name == "edge_render_operator_summary")
    compare_tool = next(tool for tool in tools if tool.name == "edge_compare_runs")

    assert render_tool.meta is not None
    assert render_tool.meta["ui"]["resourceUri"] == "ui://edge/operator-dashboard.v1.html"
    assert render_tool.meta["openai/outputTemplate"] == "ui://edge/operator-dashboard.v1.html"
    assert render_tool.meta["openai/toolInvocation/invoking"] == "Rendering dashboard"

    assert compare_tool.meta is not None
    assert compare_tool.meta["securitySchemes"] == [{"type": "noauth"}]
    assert compare_tool.meta["ui"]["visibility"] == ["model", "app"]

    dashboard_tool = next(tool for tool in tools if tool.name == "edge_get_operator_dashboard")
    assert dashboard_tool.inputSchema == get_tool_definition("edge_get_operator_dashboard").input_schema

    render_schema = next(tool for tool in tools if tool.name == "edge_render_operator_summary").inputSchema
    assert render_schema == get_tool_definition("edge_render_operator_summary").input_schema
    assert render_schema["properties"]["sections"]["type"] == "array"


def test_streamable_http_initialize_smoke() -> None:
    app = build_asgi_app()
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
    assert payload["result"]["serverInfo"]["name"] == "Edge Operator"
    assert payload["result"]["capabilities"]["tools"]["listChanged"] is False


def test_runtime_wrapper_uses_schema_defaults_for_optional_fields() -> None:
    captured: dict[str, object] = {}

    def handler(
        *,
        task: str,
        thread_id: str | None = None,
        sandbox: str | None = None,
        model: str | None = None,
        profile: str | None = None,
        timeout_sec: int | None = None,
    ) -> dict[str, object]:
        captured["task"] = task
        captured["thread_id"] = thread_id
        captured["sandbox"] = sandbox
        captured["model"] = model
        captured["profile"] = profile
        captured["timeout_sec"] = timeout_sec
        return captured

    definition = get_tool_definition("edge_invoke_operator")
    wrapped = _build_runtime_wrapper(handler, definition)

    result = wrapped(task="Repair the repo.")

    assert result["task"] == "Repair the repo."
    assert result["thread_id"] is None
    assert result["sandbox"] == "workspace-write"
    assert result["model"] is None
    assert result["profile"] is None
    assert result["timeout_sec"] == 300


def test_runtime_wrapper_preserves_typed_schema_for_dashboard_limit() -> None:
    definition = get_tool_definition("edge_get_operator_dashboard")
    wrapped = _build_runtime_wrapper(lambda **kwargs: kwargs, definition)

    assert wrapped(limit="3")["limit"] == 3
    assert wrapped.__signature__.parameters["limit"].annotation is int
