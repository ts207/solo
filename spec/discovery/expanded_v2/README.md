# Expanded edge-cell discovery surface

This profile broadens the default bounded search from two event atoms to six and adds
trend, liquidity, and funding-persistence contexts while preserving the existing
`spec/discovery` defaults for compatibility.

Use it with:

```bash
python -m project.cli discover cells plan --spec_dir spec/discovery/expanded_v2 ...
python -m project.cli discover cells run  --spec_dir spec/discovery/expanded_v2 ...
```
