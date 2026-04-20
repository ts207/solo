import asyncio
import contextlib
import json
from collections.abc import Sequence
from typing import Any

from mcp.types import DEFAULT_NEGOTIATED_VERSION

from project.apps.chatgpt.server import _build_runtime_wrapper, build_asgi_app, build_mcp_server
from project.apps.chatgpt.tool_catalog import get_tool_definition


async def start_lifespan(app: Any) -> asyncio.Task[Any]:
    started = asyncio.Event()
    shutdown = asyncio.Event()

    async def receive() -> dict[str, Any]:
        if not started.is_set():
            return {"type": "lifespan.startup"}
        await shutdown.wait()
        return {"type": "lifespan.shutdown"}

    async def send(message: dict[str, Any]) -> None:
        message_type = str(message.get("type", ""))
        if message_type == "lifespan.startup.complete":
            started.set()
        elif message_type == "lifespan.shutdown.complete":
            shutdown.set()
        elif message_type == "lifespan.startup.failed":
            started.set()
            raise RuntimeError(f"lifespan startup failed: {message}")

    task = asyncio.create_task(
        app(
            {"type": "lifespan", "asgi": {"version": "3.0", "spec_version": "2.0"}, "state": {}},
            receive,
            send,
        )
    )
    await asyncio.wait_for(started.wait(), timeout=5.0)
    return task


async def stop_lifespan(task: asyncio.Task[Any]) -> None:
    task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await task


async def request(
    app: Any,
    *,
    method: str,
    path: str,
    body: bytes = b"",
    headers: Sequence[tuple[bytes, bytes]] | None = None,
) -> tuple[int, bytes]:
    messages: list[dict[str, Any]] = []
    first = True

    async def receive() -> dict[str, Any]:
        nonlocal first
        if first:
            first = False
            return {"type": "http.request", "body": body, "more_body": False}
        return {"type": "http.disconnect"}

    async def send(message: dict[str, Any]) -> None:
        messages.append(message)

    await app(
        {
            "type": "http",
            "asgi": {"version": "3.0", "spec_version": "2.3"},
            "http_version": "1.1",
            "method": method.upper(),
            "scheme": "http",
            "path": path,
            "raw_path": path.encode("utf-8"),
            "query_string": b"",
            "headers": list(headers or []),
            "client": ("pytest", 50000),
            "server": ("testserver", 80),
            "root_path": "",
            "state": {},
        },
        receive,
        send,
    )

    start = next(message for message in messages if message["type"] == "http.response.start")
    chunks = [
        bytes(message.get("body", b""))
        for message in messages
        if message["type"] == "http.response.body"
    ]
    return int(start["status"]), b"".join(chunks)


async def json_request(
    app: Any,
    *,
    method: str,
    path: str,
    payload: dict[str, Any] | None = None,
    headers: Sequence[tuple[bytes, bytes]] | None = None,
) -> tuple[int, dict[str, Any]]:
    body = json.dumps(payload).encode("utf-8") if payload is not None else b""
    status, response_body = await request(
        app,
        method=method,
        path=path,
        body=body,
        headers=headers,
    )
    return status, json.loads(response_body.decode("utf-8"))


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

    async def _exercise() -> None:
        lifespan = await start_lifespan(app)
        try:
            health_status, health_payload = await json_request(app, method="GET", path="/")
            assert health_status == 200
            assert health_payload["mcp_endpoint"] == "/mcp"

            status, payload = await json_request(
                app,
                method="POST",
                path="/mcp/",
                payload=initialize_request,
                headers=[
                    (b"accept", b"application/json, text/event-stream"),
                    (b"content-type", b"application/json"),
                ],
            )
        finally:
            await stop_lifespan(lifespan)

        assert status == 200
        assert payload["result"]["protocolVersion"] == DEFAULT_NEGOTIATED_VERSION
        assert payload["result"]["serverInfo"]["name"] == "Edge Operator"
        assert payload["result"]["capabilities"]["tools"]["listChanged"] is False

    asyncio.run(_exercise())


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
