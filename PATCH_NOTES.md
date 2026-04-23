# Patch notes

This bundle was patched to improve repo integrity without changing your canonical data path assumptions.

## Changes

1. Replaced the wrapper root Makefile with a standalone repo-root interface.
2. Added `domain-graph` and `check-domain-graph` targets and exposed the documented supported-path benchmark target.
3. Upgraded `discover cells verify-data` to:
   - prefer real bar coverage when bars are available,
   - fall back to synthetic coverage only when bars are unavailable,
   - compute pre-run per-cell support counts when feature and market-context artifacts exist,
   - surface `support_status_counts` in outputs.
4. Added an expanded discovery profile under `spec/discovery/expanded_v2/`.
5. Rebuilt the compiled domain graph after patching.
