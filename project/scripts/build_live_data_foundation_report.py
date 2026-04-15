from __future__ import annotations

import argparse
import sys
from pathlib import Path

from project.core.config import get_data_root
from project.research.services.live_data_foundation_service import write_live_data_foundation_report


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Build a BTC-first live data foundation report from existing quality artifacts."
    )
    parser.add_argument("--run_id", required=True)
    parser.add_argument("--symbol", default="BTCUSDT")
    parser.add_argument("--market", default="perp")
    parser.add_argument("--timeframe", default="5m")
    parser.add_argument("--feature_schema_version", default="v2")
    parser.add_argument("--data_root", default=None)
    parser.add_argument("--config", default="spec/benchmarks/btc_live_foundation.yaml")
    parser.add_argument("--out_dir", default=None)
    args = parser.parse_args()

    data_root = Path(args.data_root) if args.data_root else get_data_root()
    config_path = Path(args.config) if str(args.config).strip() else None
    out_dir = Path(args.out_dir) if args.out_dir else None
    out_path = write_live_data_foundation_report(
        data_root=data_root,
        run_id=str(args.run_id),
        symbol=str(args.symbol).strip().upper(),
        timeframe=str(args.timeframe).strip().lower(),
        market=str(args.market).strip().lower(),
        feature_schema_version=str(args.feature_schema_version).strip(),
        config_path=config_path,
        out_dir=out_dir,
    )
    print(str(out_path))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
