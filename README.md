# Edge

Edge is a governed crypto research-to-runtime repository. The canonical lifecycle is:

```text
discover -> validate -> promote -> bind-config -> paper/live run
```

With a mounted lake available, the repo should feel optimized for:

```text
proposal -> first candidate -> validated edge candidate
```

The repo has two operator paths (the second is the current priority):

1. Trade an existing promoted thesis bundle.
2. Discover a new thesis, validate it, promote it, then bind and run it.

## First Commands

Zero → first edge (lake mounted):

```bash
make first-edge RUN_ID=liq_h24_01 DATA_ROOT=$LAKE
make explain-empty RUN_ID=liq_h24_01 DATA_ROOT=$LAKE
make validate RUN_ID=liq_h24_01 DATA_ROOT=$LAKE
```

Inspect before running:

```bash
edge proposal inspect --proposal spec/proposals/single_event_liquidation_exhaustion_reversal_bounce_h24_v1.yaml --run_id liq_h24_01 --data_root $LAKE
```

Interpret results after discovery:

```bash
edge discover list-artifacts --run_id liq_h24_01 --data_root $LAKE
edge discover summarize --run_id liq_h24_01 --data_root $LAKE
edge discover explain-empty --run_id liq_h24_01 --data_root $LAKE
```

Trade an existing thesis:

```bash
edge deploy bind-config --run_id <run_id>
edge deploy paper-run --config project/configs/live_paper_<run_id>.yaml
```

Discover then trade:

```bash
edge discover run --proposal spec/proposals/<proposal>.yaml --run_id <run_id> --data_root <lake>
edge validate run --run_id <run_id>
edge promote run --run_id <run_id> --symbols BTCUSDT
edge deploy bind-config --run_id <run_id>
edge deploy paper-run --config project/configs/live_paper_<run_id>.yaml
```

Makefile front door:

```bash
make first-edge RUN_ID=<run_id> DATA_ROOT=<lake> [PROPOSAL=spec/proposals/...yaml] [PROMOTION_PROFILE=research|deploy|disabled]
make discover PROPOSAL=spec/proposals/<proposal>.yaml RUN_ID=<run_id>
make list-artifacts RUN_ID=<run_id>
make summarize RUN_ID=<run_id>
make explain-empty RUN_ID=<run_id>
make validate RUN_ID=<run_id>
make promote RUN_ID=<run_id> SYMBOLS=BTCUSDT
make export RUN_ID=<run_id>
make bind-config RUN_ID=<run_id>
make paper-run CONFIG=project/configs/live_paper_<run_id>.yaml
```

## Lifecycle Stages

Stage 1 — first candidate

- Run one proposal and use `discover summarize` / `discover explain-empty`.

Stage 2 — validated candidate

- Run `validate` and keep the run_id stable.

Stage 3 — promotable edge

- Proposals often ship with `promotion_profile: disabled`. Use `--promotion_profile research|deploy` (or `make first-edge PROMOTION_PROFILE=...`) to run a promotable lane without editing the YAML.

Stage 4 — thesis export / paper run

- Only after promotion artifacts exist.

## Repo Layout

- `project/` executable source
- `spec/` authored research and runtime contracts
- `docs/` authored documentation
- `analysis/` offline exploratory and audit material
- `data/` generated reports, run artifacts, and exported thesis bundles

Runtime configs live in `project/configs/`.

Promoted thesis artifacts live in:

```text
data/live/theses/<run_id>/promoted_theses.json
```

## Docs

Start here:

- [docs/README.md](docs/README.md)
- [docs/operator/quickstart.md](docs/operator/quickstart.md)
- [docs/lifecycle/overview.md](docs/lifecycle/overview.md)
- [docs/lifecycle/deploy.md](docs/lifecycle/deploy.md)
- [docs/reference/commands.md](docs/reference/commands.md)

## Notes

- `edge deploy bind-config` emits a runtime config with exactly one thesis source.
- `paper-run` and `live-run` launch the canonical live-engine entrypoint.
- Discovery and promotion evidence are research artifacts, not deployment approval by themselves.
