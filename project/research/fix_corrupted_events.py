from project.core.config import get_data_root
import pandas as pd
from pathlib import Path
import argparse
import sys

# Add project root to path
# Now we can import the project's own write_parquet if we want,
# or just use pandas since we know we have pyarrow in the venv for this run.
# To be safe and consistent with the project's IO, we'll use the lib's write_parquet.
from project.io.utils import write_parquet, read_parquet


def fix_file(path: Path):
    if not path.exists():
        print(f"Skipping {path}: does not exist")
        return

    # Check if it's actually a CSV misnamed as parquet
    try:
        # Try reading as parquet
        pd.read_parquet(path)
        print(f"File {path} is already a valid Parquet file.")
    except Exception:
        # If it fails, try reading as CSV
        try:
            df = pd.read_csv(path)
            print(f"File {path} is a CSV misnamed as Parquet. Converting...")
            # Use the project's write_parquet which handles temp files and replacements
            # and ensures we use the correct engine.
            write_parquet(df, path)
            print(f"Successfully converted {path} to Parquet.")
        except Exception as e:
            print(f"Failed to process {path}: {e}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--run_id", required=True)
    args = parser.parse_args()

    data_root = get_data_root()
    reports_dir = data_root / "reports"

    # List of known files that might be corrupted based on analyze_... usage of merge_event_csv
    # The pattern is data/reports/<reports_dir_from_spec>/<run_id>/<events_file_from_spec>

    # Use glob with ** for recursive search, or just rglob without leading **
    potential_files = list(reports_dir.glob(f"**/{args.run_id}/*.parquet"))

    print(f"Found {len(potential_files)} potential parquet files to check for run_id {args.run_id}")
    for p in potential_files:
        fix_file(p)


if __name__ == "__main__":
    main()
