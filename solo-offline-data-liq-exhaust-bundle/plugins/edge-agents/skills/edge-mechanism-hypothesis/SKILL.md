---
name: edge-mechanism-hypothesis
description: Convert an Edge analyst report into 1-3 frozen bounded hypotheses. Use after diagnosis when the next step is to define a tighter mechanism, context filter, template choice, and invalidation logic.
---

# Edge Mechanism Hypothesis

Read `agents/mechanism-hypothesis.md` for the full spec before beginning.

## Inputs

- structured analyst report
- original proposal path or contents

## Hard requirements

- Produce at most 3 hypotheses.
- Keep one regime or context filter, one primary trigger family, one mechanism, and one main tradable expression per hypothesis.
- Freeze the event family, included events, regime or context filter, templates, mechanism statement, direction rationale, and invalidation logic.
- Do not widen scope unless the analyst report shows the current scope is structurally too narrow.
- Do not assign promotion classes or imply production readiness.

## Each output must include

- version and parent linkage
- mechanism statement
- trigger/event family and regime or context filter
- direction and rationale
- horizons and rationale
- context filter and rationale
- template and rationale
- invalidation logic
- likely failure mode
- allowed vs frozen knobs
- minimal success test
