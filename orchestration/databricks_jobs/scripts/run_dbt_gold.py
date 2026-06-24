#!/usr/bin/env python3
import argparse
import subprocess
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[3]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--project-dir", default="cdc_gold")
    parser.add_argument("--select", default=None)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    project_dir = REPO_ROOT / args.project_dir

    cmd = ["dbt", "build"]
    if args.select:
        cmd.extend(["--select", args.select])

    result = subprocess.run(cmd, cwd=project_dir, check=False)
    return result.returncode


if __name__ == "__main__":
    sys.exit(main())
