#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../../.." && pwd)"

PRODUCT_ITERATIONS="${1:-20}"
ORDER_ITERATIONS="${2:-40}"

cd "${REPO_ROOT}"

ITERATIONS="${PRODUCT_ITERATIONS}" python3 generators/load_products_generator.py
ITERATIONS="${ORDER_ITERATIONS}" python3 generators/load_generator.py
