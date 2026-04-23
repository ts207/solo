from project.apps.chatgpt.backlog import IMPLEMENTATION_BACKLOG
from project.apps.chatgpt.tool_catalog import TOOL_CATALOG, get_tool_definition


def test_render_tool_carries_widget_metadata() -> None:
    render_tool = get_tool_definition("edge_render_operator_summary")

    assert render_tool.category == "render"
    assert render_tool.ui_resource_uri is not None
    assert render_tool.output_template_uri == render_tool.ui_resource_uri
    assert render_tool.hints.read_only is True


def test_read_only_hints_match_existing_edge_behavior() -> None:
    compare_tool = get_tool_definition("edge_compare_runs")
    dashboard_tool = get_tool_definition("edge_get_operator_dashboard")
    invoke_tool = get_tool_definition("edge_invoke_operator")
    issue_run_tool = get_tool_definition("edge_issue_run")
    preview_tool = get_tool_definition("edge_preview_plan")

    assert compare_tool.hints.read_only is True
    assert dashboard_tool.hints.read_only is True
    assert invoke_tool.hints.read_only is False
    assert invoke_tool.hints.destructive is True
    assert issue_run_tool.hints.read_only is False
    assert preview_tool.hints.read_only is False


def test_backlog_is_phase_ordered() -> None:
    phases = [item["phase"] for item in IMPLEMENTATION_BACKLOG]
    assert phases == sorted(phases)
    assert phases[0] == 1


def test_catalog_names_are_unique() -> None:
    names = [tool.name for tool in TOOL_CATALOG]
    assert len(names) == len(set(names))


def test_memory_summary_tool_is_read_only() -> None:
    tool = get_tool_definition("edge_get_memory_summary")
    assert tool.category == "data"
    assert tool.hints.read_only is True


def test_compare_runs_schema_has_max_six_runs() -> None:
    tool = get_tool_definition("edge_compare_runs")
    schema = tool.input_schema
    assert schema["properties"]["run_ids"]["maxItems"] == 6


def test_catalog_has_18_tools() -> None:
    assert len(TOOL_CATALOG) == 18


def test_new_stage_tools_exist() -> None:
    names = {tool.name for tool in TOOL_CATALOG}
    assert "edge_discover_run" in names
    assert "edge_validate_run" in names
    assert "edge_promote_run" in names
    assert "edge_list_theses" in names
    assert "edge_catalog_list" in names


def test_new_stage_tools_have_correct_categories() -> None:
    assert get_tool_definition("edge_discover_run").category == "mutation"
    assert get_tool_definition("edge_validate_run").category == "mutation"
    assert get_tool_definition("edge_promote_run").category == "mutation"
    assert get_tool_definition("edge_list_theses").category == "data"
    assert get_tool_definition("edge_catalog_list").category == "data"


def test_new_stage_tools_read_only_hints() -> None:
    assert get_tool_definition("edge_discover_run").hints.read_only is False
    assert get_tool_definition("edge_validate_run").hints.read_only is False
    assert get_tool_definition("edge_promote_run").hints.read_only is False
    assert get_tool_definition("edge_list_theses").hints.read_only is True
    assert get_tool_definition("edge_catalog_list").hints.read_only is True


def test_validate_run_schema_has_timeout() -> None:
    tool = get_tool_definition("edge_validate_run")
    schema = tool.input_schema
    props = schema["properties"]
    assert "timeout_sec" in props
    assert props["timeout_sec"]["default"] == 600


def test_discover_run_schema_has_proposal() -> None:
    tool = get_tool_definition("edge_discover_run")
    schema = tool.input_schema
    assert "proposal" in schema["required"]
    assert "run_id" in schema["properties"]
    assert "check" in schema["properties"]
