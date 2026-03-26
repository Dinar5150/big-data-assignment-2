#!/bin/bash
set -euo pipefail

cd /app
source .venv/bin/activate

INDEX_ROOT="${1:-/indexer}"

python store_index.py "$INDEX_ROOT"
