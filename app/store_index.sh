#!/bin/bash
set -euo pipefail

cd /app
source .venv/bin/activate

python store_index.py "${1:-/indexer}"
