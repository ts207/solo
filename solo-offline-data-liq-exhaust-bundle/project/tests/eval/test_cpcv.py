from __future__ import annotations

from project.eval.cpcv import build_cpcv_folds


def test_cpcv_builds_combinatorial_folds_with_row_purge() -> None:
    folds = build_cpcv_folds(
        12,
        n_groups=4,
        test_groups=2,
        purge_bars=1,
        embargo_bars=1,
    )

    assert len(folds) == 6
    first = folds[0]
    assert set(first.train_indices).isdisjoint(first.test_indices)
    for test_idx in first.test_indices:
        assert test_idx - 1 not in first.train_indices
        assert test_idx + 1 not in first.train_indices
