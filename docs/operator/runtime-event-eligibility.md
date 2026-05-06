# Runtime event eligibility

Use this check when you need to understand which governed events are eligible for promotion, paper runtime, or live runtime.

```bash
edge events runtime-eligibility
```

Emit Markdown instead of JSON:

```bash
edge events runtime-eligibility --markdown
```

Refresh the generated operator inventory:

```bash
edge events runtime-eligibility --output docs/generated/runtime_eligible_events.md
```

The compiled domain graph remains the source of truth. The generated Markdown report is an operator aid that makes blocking reasons visible without manually inspecting `spec/domain/domain_graph.yaml`.
