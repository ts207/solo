from __future__ import annotations

import itertools
from dataclasses import dataclass
from typing import Any

import pandas as pd


@dataclass(frozen=True)
class CPCVFold:
    fold_id: int
    train_indices: tuple[int, ...]
    test_indices: tuple[int, ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "fold_id": self.fold_id,
            "train_indices": list(self.train_indices),
            "test_indices": list(self.test_indices),
        }


def build_cpcv_folds(
    n_observations: int,
    *,
    n_groups: int,
    test_groups: int,
    purge_bars: int = 0,
    embargo_bars: int = 0,
) -> list[CPCVFold]:
    total = max(0, int(n_observations))
    groups = max(2, int(n_groups))
    choose = max(1, min(int(test_groups), groups - 1))
    if total < groups:
        raise ValueError("n_observations must be >= n_groups")
    group_ids = pd.Series(range(total)) * groups // total
    folds: list[CPCVFold] = []
    purge = max(0, int(purge_bars))
    embargo = max(0, int(embargo_bars))
    for fold_id, selected in enumerate(itertools.combinations(range(groups), choose), start=1):
        test_mask = group_ids.isin(selected)
        test_indices = tuple(int(i) for i in group_ids.index[test_mask])
        blocked = set(test_indices)
        for idx in test_indices:
            blocked.update(range(max(0, idx - purge), idx))
            blocked.update(range(idx + 1, min(total, idx + 1 + embargo)))
        train_indices = tuple(int(i) for i in range(total) if i not in blocked)
        folds.append(CPCVFold(fold_id, train_indices, test_indices))
    return folds
