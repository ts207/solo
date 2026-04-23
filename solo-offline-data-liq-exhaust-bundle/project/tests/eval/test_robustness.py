from __future__ import annotations

import pandas as pd

from project.eval.robustness import block_bootstrap_pnl


def test_block_bootstrap_dollar_mode_keeps_initial_drawdown():
    pnl = pd.Series([-1.0] * 20)
    result = block_bootstrap_pnl(
        pnl_series=pnl,
        block_size_bars=5,
        n_iterations=20,
        random_seed=0,
        pnl_mode="dollar",
    )

    assert result["bootstrap_drawdown_p50"] > 0.0
