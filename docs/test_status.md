# Test Status

Validated in this environment:

```text
domain graph freshness: OK
detector polarity audit --fail-on-runtime-unknown: OK
template promotion contract validator: OK
runtime manifest validator on data/live/theses: OK
v10 direct regression functions: OK
zip integrity: OK
```

Full repository pytest was not completed in this environment. Prior targeted pytest invocations in this workspace did not terminate reliably, so the new v10 regression tests were executed directly by importing and calling test functions.

Recommended external validation:

```bash
PYTHONPATH=. pytest -q
make contract-check
```
