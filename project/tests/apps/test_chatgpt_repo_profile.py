import asyncio
import contextlib
import json
from collections.abc import Sequence
from typing import Any

from mcp.types import DEFAULT_NEGOTIATED_VERSION

from project.apps.chatgpt.server import build_asgi_app, build_mcp_server, build_server_blueprint
from project.apps.chatgpt.tool_catalog import (
    get_profile_metadata,
    get_tool_catalog,
    get_tool_definition,
)


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

    async def _exercise() -> None:
        lifespan = await start_lifespan(app)
        try:
            health_status, health_payload = await json_request(app, method="GET", path="/")
            assert health_status == 200
            assert health_payload["profile"] == "repo"
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
        assert payload["result"]["serverInfo"]["name"] == "Edge Repository"
        assert payload["result"]["capabilities"]["tools"]["listChanged"] is False

    asyncio.run(_exercise())
