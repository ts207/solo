# Commands and entry points

## Command layers

1. **`edge` CLI** — `project/cli.py` — canonical stage-oriented operator flows
2. **Dedicated service CLIs** — focused surfaces for specific services
3. **Make targets** — convenience wrappers and workflow bundles
4. **Direct scripts/modules** — generation, audits, benchmarks, lower-level maintenance

Use `edge` for lifecycle flows. Use Make for bundles. Use direct scripts only when operating below the stage CLI.

---

## `edge` CLI

### `edge discover`

```bash
edge discover plan --proposal <path>           # plan without running
edge discover run  --proposal <path>           # run discovery
edge discover run  --proposal <path> --run_id <id>   # reuse cached lake
edge discover list-artifacts --run_id <id>
```

`spec/proposals/canonical_event_hypothesis.yaml` is the canonical cold-start example.

### `edge validate`

```bash
edge validate run            --run_id <id>
edge validate report         --run_id <id>
edge validate diagnose       --run_id <id> [--program_id <prog>]
edge validate list-artifacts --run_id <id>
```

### `edge promote`

```bash
edge promote run            --run_id <id> --symbols BTCUSDT
edge promote export         --run_id <id>
edge promote list-artifacts --run_id <id>
```

### `edge deploy`

```bash
edge deploy list-theses
edge deploy inspect-thesis  --run_id <id>
edge deploy bind-config     --run_id <id>        # writes live_paper_<id>.yaml
edge deploy paper           --run_id <id> --config <config.yaml>
edge deploy live            --run_id <id> --config <config.yaml>
edge deploy status
```

### `edge ingest`

```bash
edge ingest --run_id <id> --symbols BTCUSDT --start YYYY-MM-DD --end YYYY-MM-DD
```

### `edge catalog`

```bash
edge catalog compare --run_id_a <id_a> --run_id_b <id_b>
edge catalog audit   [--run_id <id>]
```

---

## Console scripts (`pyproject.toml`)

| Script | Target | Role |
|--------|--------|------|
| `edge`, `backtest`, `edge-backtest` | `project.cli:main` | Canonical lifecycle CLI |
| `edge-run-all` | `project.pipelines.run_all:main` | Direct pipeline orchestrator |
| `edge-live-engine` | `project.scripts.run_live_engine:main` | Live-engine launcher |
| `edge-phase2-discovery` | `project.research.cli.candidate_discovery_cli:main` | Direct candidate discovery |
| `edge-promote` | `project.research.cli.promotion_cli:main` | Direct promotion service |
| `edge-smoke` | `project.reliability.cli_smoke:main` | Smoke and artifact validation |
| `edge-chatgpt-app` | `project.apps.chatgpt.cli:main` | ChatGPT app scaffold |
| `compile-strategy-blueprints` | `project.research.compile_strategy_blueprints:main` | Compile blueprints |
| `build-strategy-candidates` | `project.research.build_strategy_candidates:main` | Build candidate payloads |
| `ontology-consistency-audit` | `project.scripts.ontology_consistency_audit:main` | Ontology audit |

---

## Make targets

### Development

```bash
make test                    # pytest -q
make test-fast               # pytest -q -m "not slow"
make lint                    # ruff on changed files
make format                  # ruff format in-place on changed files
make format-check            # ruff format check on changed files
make style                   # lint + format-check
make minimum-green-gate      # compile + arch tests + spec QA + regressions
make governance              # governance/registry sync block
```

### Research lifecycle

```bash
make discover PROPOSAL=spec/proposals/your.yaml DISCOVER_ACTION=plan|run
make validate RUN_ID=<id>
make promote  RUN_ID=<id> SYMBOLS=BTCUSDT
make export   RUN_ID=<id>
make deploy-paper RUN_ID=<id>
```

### Pipeline bundles

```bash
make run                     # ingest + clean + features only
make baseline                # full discovery + packaging
make discover-concept        # concept-driven discovery
make discover-target SYMBOLS=BTCUSDT EVENT=VOL_SHOCK   # single-event
make golden-workflow         # canonical end-to-end smoke
make golden-certification    # golden workflow + certification manifest
make golden-synthetic-discovery
```

### Benchmarks

```bash
make benchmark-maintenance-smoke   # dry-run
make benchmark-maintenance         # full maintenance cycle
make benchmark-core
make discover-blueprints
make discover-edges
make discover-edges-from-raw
```

### Hygiene

```bash
make check-hygiene           # tracked-file, root-clutter, test-root policy
make clean                   # remove caches and temp files
make clean-runtime           # remove local runtime outputs
make debloat                 # cleanup + enforce hygiene
```

---

## Key direct scripts

### Domain and specs

```bash
PYTHONPATH=. python3 project/scripts/build_domain_graph.py       # rebuild compiled domain graph
PYTHONPATH=. python3 project/scripts/pipeline_governance.py --audit --sync
```

### Research results (auto-updated by hook)

```bash
PYTHONPATH=. python3 project/scripts/update_results_index.py    # rebuild docs/research/results.md
PYTHONPATH=. python3 project/scripts/update_reflections.py      # rebuild docs/research/reflections.md auto-section
```

### Live engine

```bash
PYTHONPATH=. python3 project/scripts/run_live_engine.py         # direct launcher
PYTHONPATH=. python3 project/scripts/certify_paper_startup.py   # 7-check startup cert (no credentials)
```

### Audits

```bash
PYTHONPATH=. python3 project/scripts/audit_detector_precision_recall.py
PYTHONPATH=. python3 project/scripts/audit_pipeline_stress.py
PYTHONPATH=. python3 project/scripts/audit_pit_compliance.py
PYTHONPATH=. python3 project/scripts/audit_promotion_flukes.py
```

---

## `PYTHONPATH=.` note

Most scripts and modules require `PYTHONPATH=.` since the package root is the repo root and there is no globally-installed editable version assumed. The `edge` console script handles this automatically after `pip install -e .`.
