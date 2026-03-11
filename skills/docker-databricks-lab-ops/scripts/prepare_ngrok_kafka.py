#!/usr/bin/env python3
import argparse
import json
import subprocess
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--local-port", type=int, default=9093)
    parser.add_argument("--api-url", default="http://127.0.0.1:4040/api/tunnels")
    parser.add_argument("--log-path", default="logs/ngrok-kafka.log")
    parser.add_argument("--timeout-seconds", type=int, default=30)
    return parser.parse_args()


def load_tunnels(api_url: str) -> dict | None:
    try:
        with urllib.request.urlopen(api_url, timeout=2) as response:
            return json.loads(response.read().decode("utf-8"))
    except (urllib.error.URLError, TimeoutError, json.JSONDecodeError):
        return None


def find_tcp_tunnel(api_url: str, local_port: int) -> dict | None:
    payload = load_tunnels(api_url)
    if not payload:
        return None

    expected = f"localhost:{local_port}"
    for tunnel in payload.get("tunnels", []):
        public_url = tunnel.get("public_url", "")
        config = tunnel.get("config", {})
        addr = config.get("addr", "")
        if public_url.startswith("tcp://") and addr.endswith(expected):
            return tunnel
    return None


def start_ngrok(local_port: int, log_path: Path) -> None:
    log_path.parent.mkdir(parents=True, exist_ok=True)
    with log_path.open("ab") as log_file:
        subprocess.Popen(
            ["ngrok", "tcp", str(local_port), "--log=stdout"],
            stdout=log_file,
            stderr=subprocess.STDOUT,
            stdin=subprocess.DEVNULL,
            start_new_session=True,
        )


def ensure_tunnel(api_url: str, local_port: int, timeout_seconds: int, log_path: Path) -> dict:
    tunnel = find_tcp_tunnel(api_url, local_port)
    if tunnel:
        return tunnel

    start_ngrok(local_port, log_path)
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        tunnel = find_tcp_tunnel(api_url, local_port)
        if tunnel:
            return tunnel
        time.sleep(1)

    raise RuntimeError(f"ngrok tunnel for localhost:{local_port} was not ready within {timeout_seconds} seconds")


def main() -> int:
    args = parse_args()
    tunnel = ensure_tunnel(
        api_url=args.api_url,
        local_port=args.local_port,
        timeout_seconds=args.timeout_seconds,
        log_path=Path(args.log_path),
    )

    public_url = tunnel["public_url"].removeprefix("tcp://")
    host, port = public_url.split(":", 1)
    print(
        json.dumps(
            {
                "public_url": tunnel["public_url"],
                "host": host,
                "port": int(port),
                "local_port": args.local_port,
            },
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as exc:
        print(json.dumps({"error": str(exc)}))
        sys.exit(2)
