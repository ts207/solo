#!/usr/bin/env python3
import subprocess
import sys
import fnmatch
import os

PROTECTED_GLOBS = [
    "data/live/theses/*",
    "data/reports/approval/*",
    "project/configs/live_trading_*.yaml",
    "project/configs/live_production.yaml",
    "deploy/systemd/*.service",
    ".env*",
    "deploy/env/*.env",
]

PROTECTED_PREFIXES = [
    "data/live/theses/",
    "data/reports/approval/",
]

def check_status():
    try:
        # Check tracked modifications (staged and unstaged)
        status = subprocess.check_output(["git", "status", "--porcelain"], text=True)
    except subprocess.CalledProcessError:
        return []

    modified_files = []
    for line in status.splitlines():
        if len(line) < 4:
            continue
        # Format is 'XY path' where XY is status
        path = line[3:].strip()
        # Handle renames 'R  old -> new'
        if " -> " in path:
            path = path.split(" -> ")[1]
        modified_files.append(path)
    
    return modified_files

def get_violations(modified_files):
    violations = []
    for path in modified_files:
        is_protected = False
        for prefix in PROTECTED_PREFIXES:
            if path.startswith(prefix):
                is_protected = True
                break
        
        if not is_protected:
            for pattern in PROTECTED_GLOBS:
                if fnmatch.fnmatch(path, pattern):
                    is_protected = True
                    break
        
        if is_protected:
            violations.append(path)
    return sorted(list(set(violations)))

def main():
    modified = check_status()
    violations = get_violations(modified)

    if violations:
        print("PROTECTED ARTIFACT WRITE POLICY VIOLATION", file=sys.stderr)
        print("The following files are protected and should not be modified by agents:", file=sys.stderr)
        for v in violations:
            print(f"  - {v}", file=sys.stderr)
        print("\nAction Required: Revert these changes. If they are necessary, a human operator must perform them or explicitly override this check.", file=sys.stderr)
        sys.exit(1)

    print("Protected path check passed.")
    sys.exit(0)

if __name__ == "__main__":
    main()
