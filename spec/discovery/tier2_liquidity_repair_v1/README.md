# Tier 2 Liquidity Repair Surface

Runnable liquidity-stress repair surface using runtime contexts only.

This surface intentionally excludes `LIQUIDITY_SHOCK` until its zero-support
data-contract issue is understood, and excludes `low_liquidity` because the
current joined data lacks the required `liquidity_regime` condition.
