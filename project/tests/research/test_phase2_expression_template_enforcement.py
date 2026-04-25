from __future__ import annotations

from project.research.search.generator import generate_hypotheses_with_audit


def test_generator_does_not_emit_filter_only_hypotheses_by_default(tmp_path):
    search_spec = tmp_path / "search.yaml"
    search_spec.write_text(
        "\n".join(
            [
                "kind: search_spec",
                "triggers:",
                "  events:",
                "    - VOL_SHOCK",
                "expression_templates:",
                "  - continuation",
                "template_policy:",
                "  generic_templates_allowed: true",
                "  reason: test fixture",
                "horizons: [15m]",
                "directions: [long]",
                "entry_lag: 1",
            ]
        ),
        encoding="utf-8",
    )
    hypotheses, audit = generate_hypotheses_with_audit(search_space_path=search_spec)
    assert hypotheses
    assert all(spec.template_id == "continuation" for spec in hypotheses)
    assert all(getattr(spec, "filter_template_id", None) in (None, "") for spec in hypotheses)
    assert audit["counts"]["feasible"] >= 1


def test_generator_attaches_optional_filter_overlays_without_changing_primary_template(tmp_path):
    search_spec = tmp_path / "search.yaml"
    search_spec.write_text(
        "\n".join(
            [
                "kind: search_spec",
                "triggers:",
                "  events:",
                "    - VOL_SHOCK",
                "expression_templates:",
                "  - continuation",
                "template_policy:",
                "  generic_templates_allowed: true",
                "  reason: test fixture",
                "filter_templates:",
                "  - only_if_regime",
                "horizons: [15m]",
                "directions: [long]",
                "entry_lag: 1",
            ]
        ),
        encoding="utf-8",
    )
    hypotheses, _ = generate_hypotheses_with_audit(search_space_path=search_spec)
    assert any(getattr(spec, "filter_template_id", None) == "only_if_regime" for spec in hypotheses)
    assert all(spec.template_id == "continuation" for spec in hypotheses)


def test_generator_emits_single_direction_for_non_directional_templates(tmp_path):
    search_spec = tmp_path / "search.yaml"
    search_spec.write_text(
        "\n".join(
            [
                "kind: search_spec",
                "triggers:",
                "  events:",
                "    - LIQUIDATION_CASCADE_PROXY",
                "expression_templates:",
                "  - mean_reversion",
                "  - continuation",
                "template_policy:",
                "  generic_templates_allowed: true",
                "  reason: test fixture",
                "horizons: [15m]",
                "directions: [long, short]",
                "entry_lag: 1",
            ]
        ),
        encoding="utf-8",
    )
    hypotheses, _ = generate_hypotheses_with_audit(search_space_path=search_spec)

    mean_reversion_directions = {
        spec.direction for spec in hypotheses if spec.template_id == "mean_reversion"
    }
    continuation_directions = {
        spec.direction for spec in hypotheses if spec.template_id == "continuation"
    }

    assert mean_reversion_directions == {"long"}
    assert continuation_directions == {"long", "short"}
