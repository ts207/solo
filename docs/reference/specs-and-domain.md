# Specs and Domain Reference

Specs define authored truth. The compiled domain graph defines runtime-readable truth. Keep them synchronized.

## Authored Spec Roots

```text
spec/events/
spec/templates/
spec/proposals/
spec/states/
spec/features/
spec/regimes/
spec/runtime/
spec/theses/
spec/gates.yaml
spec/productive_search.yaml
spec/search_space.yaml
spec/historical_universe.csv
```

Important authored files:

```text
spec/events/event_registry_unified.yaml
spec/events/regime_routing.yaml
spec/templates/registry.yaml
spec/templates/event_template_registry.yaml
spec/gates.yaml
spec/search_space.yaml
project/configs/registries/search_limits.yaml
```

## Compiled Domain Graph

The compiled graph is:

```text
spec/domain/domain_graph.yaml
```

Runtime access goes through:

```text
project.domain.compiled_registry.get_domain_registry()
```

The compiled registry exposes:

- event definitions
- state definitions
- template operators
- regimes
- thesis definitions
- searchable families
- sequence and interaction definitions
- context-state mappings

After changing spec YAML, rebuild and verify:

```bash
PYTHONPATH=. ./.venv/bin/python project/scripts/build_domain_graph.py
PYTHONPATH=. ./.venv/bin/python project/scripts/check_domain_graph_freshness.py
```

## Event Definitions

Event definitions include:

- `event_type`
- `research_family`
- `canonical_regime`
- `signal_column`
- detector ownership
- governance tier
- operational role
- deployment disposition
- eligibility flags
- required features
- sequence eligibility
- template compatibility

Use `research_family` as the coarse search/template grouping key. Some compatibility aliases exist for legacy surfaces, but authored template compatibility should follow the current registry model.

## Template Definitions

`spec/templates/registry.yaml` defines:

- expression templates
- filter templates
- execution templates
- compatible families
- event-specific template overrides
- default template parameter grids

Expression templates are the primary search unit. Filter templates are overlays. Execution-template-only search branches are reserved and should not be emitted as standalone hypotheses.

## Search Specs

`spec/search_space.yaml` is the default broad search surface. It currently describes a hierarchical discovery path with trigger viability, template refinement, execution refinement, and context refinement.

`spec/productive_search.yaml` is a narrower flat search override focused on reliable candidate generation.

Search limits are controlled by:

```text
project/configs/registries/search_limits.yaml
```

Key limits include:

- max events per run
- max templates per run
- max horizons per run
- max directions per run
- max entry lags per run
- max hypotheses total
- max hypotheses per template
- max hypotheses per event family

## Gates

`spec/gates.yaml` controls discovery, bridge, and promotion-related thresholds.

Important groups:

- `gate_v1_phase2`
- `gate_v1_phase2_profiles.discovery`
- `gate_v1_phase2_profiles.promotion`
- `gate_v1_bridge`
- `promotion_confirmatory_gates`

Discovery profiles are intentionally higher recall. Promotion profiles are stricter. Do not confuse a discovery bridge pass with production readiness.

## Event and Template Compatibility

Planning and search use `check_hypothesis_feasibility()` to reject incompatible hypotheses before evaluation. The key incompatibility reason is:

```text
incompatible_template_family
```

When a proposal expands to zero hypotheses:

1. Inspect `validated_plan.json`.
2. Check the event's research family.
3. Check the template's compatible families.
4. Check event-specific template overrides.
5. Check search limits and trigger filters.

Do not infer absence of edge until the proposal has nonzero feasible hypotheses.

## Structured Proposal Compatibility

Current structured execution accepts primary anchors:

- event
- transition
- sequence

State anchors are deprecated as primary anchors. Use filters for state/context conditioning.

Entry lag must be at least 1:

```yaml
hypothesis:
  sampling_policy:
    entry_lag_bars: 1
```

## Proposal Examples

Cold-start example:

```text
spec/proposals/canonical_event_hypothesis.yaml
```

Bounded follow-on example:

```text
spec/proposals/canonical_event_hypothesis_h24.yaml
```

The bounded follow-on declares a `baseline_run_id`. It is not a safe cold-start proposal unless the baseline run exists in the active data root.

## Governance and Routing

Regime routing is authored in:

```text
spec/events/regime_routing.yaml
```

Audit it with:

```bash
PYTHONPATH=. ./.venv/bin/python -m project.scripts.regime_routing_audit --check
```

Event governance metadata affects:

- planning eligibility
- runtime eligibility
- promotion eligibility
- primary anchor eligibility
- paper-only downgrade behavior
- live approval requirements

## Guardrails

Do not widen any of these without saying so explicitly:

- symbols
- regimes
- templates
- detectors
- horizons
- date ranges

Do not relax thresholds or cost assumptions to rescue weak claims. If a research profile is intentionally permissive, document that it is for recall, not deployment.
