# Deploy readiness

Use this command before paper, shadow, or live runtime to explain which gates pass and which gates block a run.

```bash
edge deploy readiness --run_id <run_id> --runtime_mode monitor_only
```

For trading checks:

```bash
edge deploy readiness --run_id <run_id> --runtime_mode trading --monitor_report data/reports/monitor/<run_id>/monitor_report.json
```

The command is read-only. It reports thesis payload status, runtime-manifest validity, deployment-state compatibility, and the final deploy-admission result.
