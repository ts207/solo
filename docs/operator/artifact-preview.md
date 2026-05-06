# Artifact preview

Use this command to inspect small previews of generated artifacts without opening notebooks or custom scripts.

```bash
edge artifacts preview --path data/reports/validation/<run_id>/validation_bundle.json
```

Supported formats:

- JSON
- JSONL
- CSV
- Parquet, when local pandas/pyarrow support is available

The command is read-only. Parquet preview degrades gracefully when optional local Parquet dependencies are unavailable.
