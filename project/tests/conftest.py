"""Shared test configuration and canonical path constants.

All tests must import path roots from here rather than computing
their own via parents[N] — that pattern is fragile and wrong across
different nesting depths.
"""
from __future__ import annotations

import asyncio
import importlib.util
import inspect
import sys
from contextlib import asynccontextmanager
from dataclasses import dataclass
from pathlib import Path
from types import ModuleType
from typing import Any

# project/tests/conftest.py -> parents[0]=tests, parents[1]=project, parents[2]=EDGEE-main
REPO_ROOT: Path = Path(__file__).resolve().parents[2]
PROJECT_ROOT: Path = REPO_ROOT / "project"
SPEC_ROOT: Path = REPO_ROOT / "spec"


from project.io.parquet_compat import patch_pandas_parquet_fallback


def _install_mcp_test_stub() -> None:
    if importlib.util.find_spec("mcp") is not None:
        return

    default_protocol_version = "2025-03-26"

    @dataclass
    class ToolAnnotations:
        title: str | None = None
        readOnlyHint: bool = False
        destructiveHint: bool = False
        idempotentHint: bool = False
        openWorldHint: bool = False

    @dataclass
    class TextResourceContents:
        uri: str
        mimeType: str
        text: str
        _meta: dict[str, Any] | None = None

        @property
        def meta(self) -> dict[str, Any] | None:
            return self._meta

    @dataclass
    class Tool:
        name: str
        title: str | None = None
        description: str | None = None
        inputSchema: dict[str, Any] | None = None
        outputSchema: dict[str, Any] | None = None
        annotations: ToolAnnotations | None = None
        _meta: dict[str, Any] | None = None

        @property
        def meta(self) -> dict[str, Any] | None:
            return self._meta

    @dataclass
    class _RegisteredTool:
        name: str
        title: str | None
        description: str | None
        parameters: dict[str, Any]
        output_schema: dict[str, Any] | None
        annotations: ToolAnnotations | None

    class _ToolManager:
        def __init__(self) -> None:
            self._tools: dict[str, _RegisteredTool] = {}

        def add(self, tool: _RegisteredTool) -> None:
            self._tools[tool.name] = tool

        def get_tool(self, name: str) -> _RegisteredTool | None:
            return self._tools.get(name)

        def list_tools(self) -> list[_RegisteredTool]:
            return list(self._tools.values())

    class _SessionManager:
        @asynccontextmanager
        async def run(self) -> Any:
            yield

    def _schema_from_signature(handler: Any) -> dict[str, Any]:
        properties: dict[str, Any] = {}
        required: list[str] = []
        for name, parameter in inspect.signature(handler).parameters.items():
            if parameter.kind not in {
                inspect.Parameter.POSITIONAL_OR_KEYWORD,
                inspect.Parameter.KEYWORD_ONLY,
            }:
                continue
            properties[name] = {}
            if parameter.default is inspect._empty:
                required.append(name)
            else:
                properties[name]["default"] = parameter.default
        return {
            "type": "object",
            "properties": properties,
            "required": required,
        }

    class FastMCP:
        def __init__(self, name: str, **_: Any) -> None:
            self.name = name
            self._tool_manager = _ToolManager()
            self.session_manager = _SessionManager()
            self._resources: dict[str, Any] = {}

        def resource(self, uri: str) -> Any:
            def decorator(handler: Any) -> Any:
                self._resources[uri] = handler
                return handler

            return decorator

        def add_tool(
            self,
            handler: Any,
            *,
            name: str,
            title: str | None = None,
            description: str | None = None,
            annotations: ToolAnnotations | None = None,
        ) -> None:
            self._tool_manager.add(
                _RegisteredTool(
                    name=name,
                    title=title,
                    description=description,
                    parameters=_schema_from_signature(handler),
                    output_schema=None,
                    annotations=annotations,
                )
            )

        def streamable_http_app(self) -> Any:
            from starlette.applications import Starlette
            from starlette.responses import JSONResponse
            from starlette.routing import Route

            async def handle(request: Any) -> Any:
                payload = await request.json()
                request_id = payload.get("id")
                method = str(payload.get("method") or "").strip()
                if method != "initialize":
                    return JSONResponse(
                        {
                            "jsonrpc": "2.0",
                            "id": request_id,
                            "error": {"code": -32601, "message": "Method not found"},
                        },
                        status_code=404,
                    )
                return JSONResponse(
                    {
                        "jsonrpc": "2.0",
                        "id": request_id,
                        "result": {
                            "protocolVersion": default_protocol_version,
                            "serverInfo": {"name": self.name},
                            "capabilities": {"tools": {"listChanged": False}},
                        },
                    }
                )

            return Starlette(routes=[Route("/", endpoint=handle, methods=["POST"])])

    mcp_module = ModuleType("mcp")
    mcp_module.__path__ = []
    types_module = ModuleType("mcp.types")
    types_module.DEFAULT_NEGOTIATED_VERSION = default_protocol_version
    types_module.ToolAnnotations = ToolAnnotations
    types_module.TextResourceContents = TextResourceContents
    types_module.Tool = Tool
    server_module = ModuleType("mcp.server")
    server_module.__path__ = []
    fastmcp_module = ModuleType("mcp.server.fastmcp")
    fastmcp_module.FastMCP = FastMCP
    server_module.fastmcp = fastmcp_module
    mcp_module.types = types_module
    mcp_module.server = server_module
    sys.modules["mcp"] = mcp_module
    sys.modules["mcp.types"] = types_module
    sys.modules["mcp.server"] = server_module
    sys.modules["mcp.server.fastmcp"] = fastmcp_module


def _install_starlette_test_stub() -> None:
    if importlib.util.find_spec("starlette") is not None:
        return

    @dataclass
    class _Request:
        method: str
        path: str
        headers: dict[str, Any]
        payload: Any = None

        async def json(self) -> Any:
            return self.payload

    class _Response:
        media_type = "text/plain"

        def __init__(self, content: Any, status_code: int = 200) -> None:
            self.status_code = status_code
            self.content = content

        def json(self) -> Any:
            return self.content

    class JSONResponse(_Response):
        media_type = "application/json"

    class PlainTextResponse(_Response):
        media_type = "text/plain"

    @dataclass
    class Route:
        path: str
        endpoint: Any
        methods: list[str] | None = None

    @dataclass
    class Mount:
        path: str
        app: Any

    class Starlette:
        def __init__(self, *, routes: list[Any] | None = None, lifespan: Any = None) -> None:
            self.routes = list(routes or [])
            self.lifespan = lifespan

        def _match(self, method: str, path: str) -> tuple[Any, _Request] | tuple[None, _Request]:
            request = _Request(method=method, path=path, headers={})
            for route in self.routes:
                if isinstance(route, Route):
                    methods = {item.upper() for item in route.methods or []}
                    if route.path == path and (not methods or method.upper() in methods):
                        return route.endpoint, request
                elif isinstance(route, Mount):
                    prefix = route.path.rstrip("/")
                    if path == prefix or path.startswith(prefix + "/"):
                        nested = path[len(prefix) :] or "/"
                        request = _Request(method=method, path=nested, headers={})
                        return route.app, request
            return None, request

        def handle_request(
            self,
            method: str,
            path: str,
            *,
            headers: dict[str, Any] | None = None,
            json_payload: Any = None,
        ) -> Any:
            target, request = self._match(method, path)
            request.headers = dict(headers or {})
            request.payload = json_payload
            if target is None:
                return PlainTextResponse("Not found", status_code=404)
            if hasattr(target, "handle_request"):
                return target.handle_request(
                    method,
                    request.path,
                    headers=request.headers,
                    json_payload=request.payload,
                )
            response = target(request)
            if inspect.isawaitable(response):
                return asyncio.run(response)
            return response

    class CORSMiddleware:
        def __init__(self, app: Any, **_: Any) -> None:
            self.app = app

        def handle_request(
            self,
            method: str,
            path: str,
            *,
            headers: dict[str, Any] | None = None,
            json_payload: Any = None,
        ) -> Any:
            return self.app.handle_request(
                method,
                path,
                headers=headers,
                json_payload=json_payload,
            )

    class TestClient:
        __test__ = False

        def __init__(self, app: Any) -> None:
            self.app = app
            self._lifespan_cm: Any = None

        def __enter__(self) -> "TestClient":
            base_app = self.app.app if hasattr(self.app, "app") else self.app
            lifespan = getattr(base_app, "lifespan", None)
            if lifespan is not None:
                self._lifespan_cm = lifespan(base_app)
                asyncio.run(self._lifespan_cm.__aenter__())
            return self

        def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
            if self._lifespan_cm is not None:
                asyncio.run(self._lifespan_cm.__aexit__(exc_type, exc, tb))

        def get(self, path: str, *, headers: dict[str, Any] | None = None) -> Any:
            return self.app.handle_request("GET", path, headers=headers)

        def post(
            self,
            path: str,
            *,
            headers: dict[str, Any] | None = None,
            json: Any = None,
        ) -> Any:
            return self.app.handle_request("POST", path, headers=headers, json_payload=json)

    starlette_module = ModuleType("starlette")
    starlette_module.__path__ = []
    applications_module = ModuleType("starlette.applications")
    applications_module.Starlette = Starlette
    responses_module = ModuleType("starlette.responses")
    responses_module.JSONResponse = JSONResponse
    responses_module.PlainTextResponse = PlainTextResponse
    routing_module = ModuleType("starlette.routing")
    routing_module.Route = Route
    routing_module.Mount = Mount
    middleware_module = ModuleType("starlette.middleware")
    middleware_module.__path__ = []
    cors_module = ModuleType("starlette.middleware.cors")
    cors_module.CORSMiddleware = CORSMiddleware
    testclient_module = ModuleType("starlette.testclient")
    testclient_module.TestClient = TestClient
    starlette_module.applications = applications_module
    starlette_module.responses = responses_module
    starlette_module.routing = routing_module
    starlette_module.middleware = middleware_module
    starlette_module.testclient = testclient_module
    middleware_module.cors = cors_module
    sys.modules["starlette"] = starlette_module
    sys.modules["starlette.applications"] = applications_module
    sys.modules["starlette.responses"] = responses_module
    sys.modules["starlette.routing"] = routing_module
    sys.modules["starlette.middleware"] = middleware_module
    sys.modules["starlette.middleware.cors"] = cors_module
    sys.modules["starlette.testclient"] = testclient_module


patch_pandas_parquet_fallback()
_install_mcp_test_stub()
_install_starlette_test_stub()
