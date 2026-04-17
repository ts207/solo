# Promote stage

## CLI

```bash
edge promote run            --run_id <run_id> --symbols BTCUSDT
edge promote export         --run_id <run_id>
edge promote list-artifacts --run_id <run_id>
```

---

## What promote does

Promote packages validated candidates into governed thesis artifacts consumable by the live stack. It is the bridge between research outputs and runtime-facing payloads.

**Promotion and export are separate operations.** Promotion decides what qualifies. Export decides how it is packaged into the live thesis contract. This separation lets you re-run export rules without pretending the promotion decision changed.

---

## Code path

```
project/promote/__init__.py                           ← façade
  └─ project/research/services/promotion_service.py  ← gates, packaging, blueprints
       └─ project/research/live_export.py             ← live thesis store + index
```

---

## Promotion gates (research profile)

Four gates are disabled for the `research` promotion profile:

| Gate | Default | Research profile |
|------|---------|-----------------|
| `min_events` | 100 | 0 |
| `allow_missing_negative_controls` | False | True |
| `dsr` gate | required | removed |
| `use_effective_q_value` | True (inflates q) | False |

These were removed because they consistently blocked valid research-track discoveries. The q_value gate and FDR remain active.

---

## Promotion classes and deployment states

| Promotion class | Default deployment state |
|----------------|--------------------------|
| `paper_promoted` | `paper_only` |
| `production_promoted` | `live_enabled` |

---

## Outputs

### Promotion artifacts

Under `data/reports/promotions/<run_id>/` and `data/reports/strategy_blueprints/<run_id>/`:
- `promotion_summary.json`
- `promotion_report.json`
- `promoted_blueprints.jsonl`
- Audit parquets

### Live thesis package

| File | Path |
|------|------|
| Thesis package | `data/live/theses/<run_id>/promoted_theses.json` |
| Global index | `data/live/theses/index.json` |

`live_export.py` validates `promoted_theses.json` against schema `promoted_theses_v1` and rejects malformed or incomplete lineage. Do not write these files manually.

---

## Binding a config for paper deployment

After promotion:
```bash
edge deploy bind-config --run_id <run_id>
# Writes: project/configs/live_paper_<run_id>.yaml
```

The bound config clones `project/configs/live_paper_btc_thesis_v1.yaml` and injects `thesis_run_id`. Do not edit the bound config manually.

---

## Operational rules

- Never bypass validation and promote directly from ad hoc tables
- Never write `promoted_theses.json` or `index.json` by hand
- If overlap/admission logic changes, update `project/portfolio/` and re-run affected test families
