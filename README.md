# Edge

Governed crypto research-to-runtime repo.

Lifecycle:

```text
discover → validate → promote → deploy
```

---

## First clone

```bash
pip install -e ".[dev]"
pre-commit install
pre-commit install --hook-type commit-msg
make agent-check
```

Use `make minimum-green-gate` before larger platform changes.

---

## Common tasks

### Discover a new edge

Read: [docs/operator/discover-edge.md](docs/operator/discover-edge.md)

```bash
make first-edge RUN_ID=<run_id> DATA_ROOT=<lake> START=<start> END=<end>
make discover-doctor RUN_ID=<run_id> DATA_ROOT=<lake>
make validate RUN_ID=<run_id> DATA_ROOT=<lake>
make promote RUN_ID=<run_id> SYMBOLS=BTCUSDT
```

### Trade an existing promoted thesis in paper mode

Read: [docs/operator/quickstart.md](docs/operator/quickstart.md)

```bash
make bind-config RUN_ID=<run_id>
make paper-run CONFIG=project/configs/live_paper_<run_id>.yaml
```

### Understand exact commands

Read: [docs/reference/commands.md](docs/reference/commands.md)

### AI-agent operation

Read: [AGENTS.md](AGENTS.md) and [CLAUDE.md](CLAUDE.md)

---

## Repo layout

- `project/` — executable source
- `spec/` — authored research and runtime contracts
- `docs/` — authored documentation
- `data/` — generated reports, run artifacts, and exported thesis bundles

Runtime configs live in `project/configs/`.

Promoted thesis artifacts live in:

```text
data/live/theses/<run_id>/promoted_theses.json
```

Config naming convention:

```text
monitor_only → project/configs/live_monitor_<run_id>.yaml
simulation   → project/configs/live_paper_<run_id>.yaml
trading      → project/configs/live_trading_<run_id>.yaml
```

---

## Key concepts

- **Thesis** — a promoted hypothesis bundle with validated edge evidence, ready for deployment.
- **Anchor** — the primary trigger event that initializes a hypothesis evaluation window.
- **Filter** — a context gate that restricts template applicability based on market state.
- `strategy_runtime` — the live/paper engine configuration section.

Discovery and promotion evidence are research artifacts, not deployment approval by themselves.
