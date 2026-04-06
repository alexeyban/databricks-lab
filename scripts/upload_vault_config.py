#!/usr/bin/env python3
"""
Upload pipeline_configs/datavault/dv_model.json to the Unity Catalog Volume
so vault notebooks can read it at runtime.

Usage:
    set -a && source .env && set +a
    python3 scripts/upload_vault_config.py
"""
import os
import requests
from pathlib import Path

LOCAL_PATH  = Path(__file__).parent.parent / "pipeline_configs/datavault/dv_model.json"
VOLUME_PATH = "/Volumes/workspace/default/mnt/pipeline_configs/datavault/dv_model.json"


def main() -> None:
    host  = os.environ["DATABRICKS_HOST"].rstrip("/")
    token = os.environ["DATABRICKS_TOKEN"]

    if not LOCAL_PATH.exists():
        raise FileNotFoundError(f"Local file not found: {LOCAL_PATH}")

    url = f"{host}/api/2.0/fs/files{VOLUME_PATH}"
    headers = {"Authorization": f"Bearer {token}"}

    print(f"Uploading {LOCAL_PATH} → {VOLUME_PATH} ...")
    with LOCAL_PATH.open("rb") as f:
        resp = requests.put(url, headers=headers, data=f)

    if resp.status_code in (200, 204):
        print("Upload successful.")
    else:
        print(f"Upload failed: {resp.status_code} {resp.text}")
        raise SystemExit(1)


if __name__ == "__main__":
    main()
