import argparse
import logging
from pathlib import Path

from project.research.benchmarks.discovery_benchmark import run_benchmark


def main():
    parser = argparse.ArgumentParser(description="Run Discovery V2 Stabilization Benchmarks")
    parser.add_argument("--spec", type=str, help="Path to benchmark spec YAML")
    parser.add_argument("--log-level", default="INFO", help="Logging level")

    args = parser.parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper()),
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
    )

    spec_path = Path(args.spec) if args.spec else None

    if spec_path:
        run_benchmark(spec_path)
    else:
        run_benchmark()

if __name__ == "__main__":
    main()
