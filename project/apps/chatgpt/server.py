from __future__ import annotations

import contextlib
import importlib
import inspect
from typing import Any

from project.apps.chatgpt.resources import (
    WIDGET_MIME_TYPE,
    WIDGET_URI,
    build_widget_resource,
    load_widget_html,
)
from project.apps.chatgpt.tool_catalog import get_profile_metadata, get_tool_catalog


def build_server_blueprint(*, profile: str = "operator", repo_root: str | None = None) -> dict[str, Any]:
    metadata = get_profile_metadata(profile)
    tool_catalog = get_tool_catalog(profile)
    resources = [build_widget_resource()] if metadata["profile"] == "operator" else []
    blueprint: dict[str, Any] = {
        "app": {
            "name": metadata["app_name"],
            "version": metadata["version"],
            "description": metadata["description"],
            "profile": metadata["profile"],
        },
        "tools": [tool.model_dump() for tool in tool_catalog],
        "resources": resources,
    }
    if repo_root is not None:
        blueprint["app"]["repo_root"] = repo_root
    return blueprint


def _import_optional(module_name: str) -> Any:
    try:
        return importlib.import_module(module_name)
    except ImportError:
        return None


def _import_mcp_runtime() -> Any:
    runtime = _import_optional("mcp")
    if runtime is None:  # pragma: no cover - runtime dependency path
        raise RuntimeError(
            "The Python MCP SDK is not installed. Install it with `pip install \"mcp[cli]\"` before starting the Edge ChatGPT app server."
        )
    return runtime


def _load_symbol(dotted_path: str) -> Any:
    module_name, symbol_name = dotted_path.rsplit(".", 1)
    module = importlib.import_module(module_name)
    return getattr(module, symbol_name)


def _build_tool_annotations(definition: Any) -> Any:
    ToolAnnotations = _load_symbol("mcp.types.ToolAnnotations")
    hints = definition.hints
    return ToolAnnotations(
        title=definition.title,
        readOnlyHint=bool(hints.read_only),
        destructiveHint=bool(hints.destructive),
        idempotentHint=bool(hints.read_only),
        openWorldHint=bool(hints.open_world),
    )


def _build_tool_descriptor_meta(definition: Any) -> dict[str, Any]:
    descriptor_meta: dict[str, Any] = {
        "securitySchemes": [{"type": "noauth"}],
        "ui": {
            "visibility": ["model", "app"],
        },
        "openai/visibility": "public",
        "openai/widgetAccessible": bool(definition.ui_resource_uri),
        "openai/toolInvocation/invoking": definition.invoking_text,
        "openai/toolInvocation/invoked": definition.invoked_text,
    }
    if definition.ui_resource_uri:
        descriptor_meta["ui"]["resourceUri"] = definition.ui_resource_uri
    if definition.output_template_uri:
        descriptor_meta["openai/outputTemplate"] = definition.output_template_uri
    return descriptor_meta


def _build_runtime_wrapper(handler: Any, definition: Any) -> Any:
    input_model = _load_symbol(definition.input_model)
    fields = getattr(input_model, "model_fields", {})
    parameters: list[inspect.Parameter] = []

    for field_name, field in fields.items():
        default = inspect.Signature.empty
        if not field.is_required():
            default = field.get_default(call_default_factory=True)
        parameters.append(
            inspect.Parameter(
                field_name,
                kind=inspect.Parameter.KEYWORD_ONLY,
                default=default,
                annotation=field.annotation,
            )
        )

    def _wrapped(**kwargs: Any) -> Any:
        payload = input_model.model_validate(kwargs)
        return handler(**payload.model_dump())

    wrapped = _wrapped
    wrapped.__name__ = str(definition.name)
    wrapped.__doc__ = handler.__doc__ or definition.description
    wrapped.__module__ = handler.__module__
    wrapped.__signature__ = inspect.Signature(parameters=parameters)
    return wrapped


def build_mcp_server(*, profile: str = "operator", repo_root: str | None = None) -> Any:
    _import_mcp_runtime()
    FastMCP = _load_symbol("mcp.server.fastmcp.FastMCP")
    TextResourceContents = _load_symbol("mcp.types.TextResourceContents")
    MCPTool = _load_symbol("mcp.types.Tool")

    metadata = get_profile_metadata(profile)
    tool_catalog = get_tool_catalog(profile)
    tool_definitions = {tool.name: tool for tool in tool_catalog}

    class EdgeFastMCP(FastMCP):
        async def list_tools(self) -> list[Any]:
            tools = self._tool_manager.list_tools()
            return [
                MCPTool(
                    name=info.name,
                    title=info.title,
                    description=info.description,
                    inputSchema=dict(tool_definitions[info.name].input_schema or {}),
                    outputSchema=info.output_schema,
                    annotations=info.annotations,
                    _meta=_build_tool_descriptor_meta(tool_definitions[info.name]),
                )
                for info in tools
            ]

    mcp_server = EdgeFastMCP(
        metadata["app_name"],
        instructions=metadata["instructions"],
        stateless_http=True,
        json_response=True,
        streamable_http_path="/",
    )

    if metadata["profile"] == "operator":
        @mcp_server.resource(WIDGET_URI)
        def edge_operator_widget() -> Any:
            return TextResourceContents(
                uri=WIDGET_URI,
                mimeType=WIDGET_MIME_TYPE,
                text=load_widget_html(),
                _meta=build_widget_resource().get("_meta", {}),
            )

    for tool in tool_catalog:
        handler = _load_symbol(tool.handler)
        runtime_handler = _build_runtime_wrapper(handler, tool)
        mcp_server.add_tool(
            runtime_handler,
            name=tool.name,
            title=tool.title,
            description=tool.description,
            annotations=_build_tool_annotations(tool),
        )

    return mcp_server


def build_asgi_app(*, profile: str = "operator", repo_root: str | None = None) -> Any:
    _import_mcp_runtime()
    Starlette = _load_symbol("starlette.applications.Starlette")
    JSONResponse = _load_symbol("starlette.responses.JSONResponse")
    PlainTextResponse = _load_symbol("starlette.responses.PlainTextResponse")
    Route = _load_symbol("starlette.routing.Route")
    Mount = _load_symbol("starlette.routing.Mount")
    CORSMiddleware = _load_symbol("starlette.middleware.cors.CORSMiddleware")

    mcp_server = build_mcp_server(profile=profile, repo_root=repo_root)
    metadata = get_profile_metadata(profile)

    async def healthcheck(_request: Any) -> Any:
        return JSONResponse(
            {
                "ok": True,
                "app": "edge-chatgpt-app",
                "profile": metadata["profile"],
                "transport": "streamable-http",
                "mcp_endpoint": "/mcp",
            }
        )

    async def oauth_not_configured(_request: Any) -> Any:
        return PlainTextResponse("OAuth not configured for this server.", status_code=404)

    @contextlib.asynccontextmanager
    async def lifespan(_app: Any) -> Any:
        async with mcp_server.session_manager.run():
            yield

    app = Starlette(
        routes=[
            Route("/", endpoint=healthcheck, methods=["GET"]),
            Route("/.well-known/oauth-authorization-server", endpoint=oauth_not_configured, methods=["GET"]),
            Route("/oauth/.well-known/openid-configuration", endpoint=oauth_not_configured, methods=["GET"]),
            Mount("/mcp", app=mcp_server.streamable_http_app()),
        ],
        lifespan=lifespan,
    )
    return CORSMiddleware(
        app,
        allow_origins=["*"],
        allow_methods=["GET", "POST", "DELETE", "OPTIONS"],
        allow_headers=["*"],
        expose_headers=["Mcp-Session-Id"],
    )


def serve_streamable_http(*, host: str = "127.0.0.1", port: int = 8000, path: str = "/mcp", profile: str = "operator", repo_root: str | None = None) -> None:
    if path != "/mcp":
        raise ValueError("The current server scaffold expects path='/mcp'. Adjust build_asgi_app() before changing it.")

    _import_mcp_runtime()
    uvicorn = _import_optional("uvicorn")
    if uvicorn is None:  # pragma: no cover - runtime dependency path
        raise RuntimeError(
            "uvicorn is not installed. Install `mcp[cli]` or add `uvicorn` explicitly before running the Edge ChatGPT app server."
        )
    uvicorn.run(build_asgi_app(profile=profile, repo_root=repo_root), host=host, port=port)
