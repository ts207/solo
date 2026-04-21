# Promote

Promote applies governed policy to validation-ready candidates, writes promotion artifacts, and exports live thesis bundles.

## Command Surface

```bash
edge promote run --run_id <run_id> --symbols BTCUSDT
```

Export only:

```bash
edge promote export --run_id <run_id>
edge deploy export --run_id <run_id>
```

Make wrappers:

```bash
make promote RUN_ID=<run_id> SYMBOLS=BTCUSDT
make export RUN_ID=<run_id>
```

## Validation Prerequisite

Promotion requires canonical validation output:

```text
data/reports/validation/<run_id>/validation_bundle.json
data/reports/validation/<run_id>/promotion_ready_candidates.parquet
```

If validation has not run, promotion is rejected. Discovery candidates alone are not promotion input.

## Implementation Surface

The CLI calls:

```text
project.promote.run()
  -> build_promotion_config()
  -> execute_promotion()
```

Promotion policy and artifact writing live in:

```text
project/research/services/promotion_service.py
project/research/promotion/
project/research/live_export.py
```

## Policy Inputs

Promotion evaluates:

- validation-ready candidates
- source candidate rows
- run manifest metadata
- objective and retail profile contracts
- ontology hash
- hypothesis index
- negative-control summary
- search-burden summary when available
- detector governance metadata
- multiplicity diagnostics
- cost survival and retail viability metrics

The default promotion config is stricter than discovery gates. Do not rescue weak claims by relaxing thresholds or cost assumptions without making that choice explicit.

## Exploratory Versus Confirmatory

Promotion distinguishes exploratory and confirmatory run modes. Exploratory discovery is not automatically production-ready. In the canonical path, promotion from exploratory sources is blocked unless policy explicitly allows discovery promotion.

Confirmatory runs require stronger lineage:

- locked candidates
- frozen spec hash alignment
- confirmatory source run mode
- validation lineage

## Outputs

Promotion reports:

```text
data/reports/promotions/<run_id>/promotion_audit.parquet
data/reports/promotions/<run_id>/promoted_candidates.parquet
data/reports/promotions/<run_id>/promotion_summary.csv
data/reports/promotions/<run_id>/promotion_diagnostics.json
data/reports/promotions/<run_id>/evidence_bundles.jsonl
data/reports/promotions/<run_id>/evidence_bundle_summary.parquet
data/reports/promotions/<run_id>/promotion_decisions.parquet
data/reports/promotions/<run_id>/promoted_thesis_contracts.json
data/reports/promotions/<run_id>/promoted_thesis_contracts.md
```

Live thesis export:

```text
data/live/theses/<run_id>/promoted_theses.json
data/live/theses/index.json
```

`execute_promotion()` calls `export_promoted_theses_for_run()` after writing promotion reports. A successful promotion run can therefore also materialize live thesis artifacts.

## Promotion Diagnostics

The most useful first file is:

```text
data/reports/promotions/<run_id>/promotion_diagnostics.json
```

Inspect:

- `promotion_input_mode`
- `decision_summary`
- `degraded_states`
- `detector_governance_policy`
- `live_thesis_export`
- `historical_trust`
- `error`

## Detector Governance Effects

Detector governance can downgrade or block deployment state:

- Non-trigger roles, non-promotion-eligible detectors, and fragile bands can force `paper_only`.
- Proxy evidence or non-production maturity can require stronger evidence.
- Production-promoted and live-eligible states require explicit live approval gates.

Promotion success does not mean live execution approval. It means a governed thesis artifact was created and passed export checks.

## Stop Conditions

Stop before deploy when:

- `promotion_diagnostics.json` contains `error`.
- `promoted_candidates.parquet` is empty and that was not expected.
- `evidence_bundles.jsonl` is missing for promoted rows.
- `promoted_theses.json` is missing.
- Thesis export fails validation through `ThesisStore.from_path(..., strict_live_gate=True)`.

## Minimal Verification

```bash
edge validate run --run_id <run_id>
edge promote run --run_id <run_id> --symbols BTCUSDT
edge promote export --run_id <run_id>
```

Then inspect:

```bash
edge deploy inspect --run_id <run_id>
```
