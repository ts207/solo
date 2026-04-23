# Core v1 Complementarity Memo

This audit evaluates the relationship between `positive_funding` and `bullish_trend` to determine whether they contribute distinct discovery value or mostly duplicate the same rankings.

## Reference Slice

Reference slice: `BTCUSDT h1_2024` from `analysis/context_uplift`.

| Metric | Result | Interpretation |
| --- | ---: | --- |
| Event family overlap | 100.0% | Both contexts touch the same high-level regime families in this slice. |
| Event atom overlap | 100.0% | Shared event atoms: FUNDING_EXTREME_ONSET, FUNDING_PERSISTENCE_TRIGGER, VOL_SHOCK. |
| Search-bucket overlap | 100.0% | Both contexts evaluate the same `(event_atom, template, direction, horizon)` buckets. |
| Event count correlation | 0.9998 | They operate on nearly the same underlying event mass in aligned buckets. |
| Rank score correlation | 0.1120 | Low correlation indicates materially different ranking behavior inside the shared search space. |
| `positive_funding` event share | 77.0% | Broader benchmark lens across the shared buckets. |
| `bullish_trend` event share | 23.0% | Narrower, higher-conviction subset of the same bucket family. |

## Key Findings

1. Distinct alpha pockets

The low rank-score correlation in the reference slice shows that `positive_funding` and `bullish_trend` do not merely relabel the same winners. They review the same search buckets but surface different pockets as attractive.

2. Complementary partitioning of the same search space

`positive_funding` covers the broader event mass while `bullish_trend` isolates a materially smaller subset. That is a complementarity pattern, not a redundancy pattern, because the narrower selector changes the ranking profile without changing the underlying bucket family.

## Known Limits

This is strong evidence of local complementarity, not final proof of full production stability. The original memo was based on one smoke slice. The table below extends that check across the available 2024 BTC and ETH slices, but it is still bounded to the existing `analysis/context_uplift` campaign.

## Cross-Slice Complementarity Table

| Symbol | Slice | Event Family Overlap | Event Atom Overlap | Bucket Overlap | Event Count Corr | Rank Score Corr | PF Event Share | BT Event Share |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| BTCUSDT | full_2024 | 100.0% | 100.0% | 100.0% | 0.9998 | 0.1120 | 77.0% | 23.0% |
| BTCUSDT | h1_2024 | 100.0% | 100.0% | 100.0% | 0.9998 | 0.1120 | 77.0% | 23.0% |
| BTCUSDT | h2_2024 | 100.0% | 100.0% | 100.0% | 0.9998 | 0.1120 | 77.0% | 23.0% |
| ETHUSDT | full_2024 | 100.0% | 100.0% | 100.0% | 1.0000 | -0.1888 | 73.9% | 26.1% |
| ETHUSDT | h1_2024 | 100.0% | 100.0% | 100.0% | 1.0000 | -0.1888 | 73.9% | 26.1% |
| ETHUSDT | h2_2024 | 100.0% | 100.0% | 100.0% | 1.0000 | -0.1888 | 73.9% | 26.1% |

## Conclusion

Keeping `bullish_trend` in Core is justified by the available audit data. The current evidence supports three claims: `bullish_trend` is not redundant with `positive_funding`, the pair partitions the same bucket families differently enough to matter, and the complementarity pattern survives across the current 2024 BTC and ETH slices.
