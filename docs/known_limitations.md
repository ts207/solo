# Known limitations

| Area | Limitation | Tracked by | Status |
|---|---|---:|---|
| Discovery | Silent feasibility drops can invalidate a run if not surfaced. | P0.1 | Hardened: zero-feasible runs now fail unless explicitly allowed. |
| Discovery | Gross-return t-statistics can overstate candidates when costs dominate. | P0.2 | Hardened: net t-stat and net expectancy are load-bearing gates. |
| Discovery | Funnel survival was not queryable as a first-class artifact. | P1.1 | Hardened: funnel JSON/index and CLI reader added. |
| Validation | Forward confirmation was optional for deploy promotion. | P1.2 | Hardened: deploy profile blocks promotion without confirmation. |
| Spec governance | Template/event registry synchronization can drift. | P2.4 | Mitigated: `check_registry_sync.py` and pre-commit/CI checks added. |
| Engineering | CI and pre-commit were absent. | P0.3/P0.4 | Added workflows, hooks, and commit-message checks. |
| Engineering | Large modules remain candidates for pure-move decomposition. | P1.3 | Todo. |
| Runtime | Snapshot replay determinism is not yet a required CI gate. | P2.6 | Todo. |
