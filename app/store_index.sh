#!/bin/bash
set -euo pipefail

cd /app
source .venv/bin/activate

python app.py "${1:-/indexer}"
