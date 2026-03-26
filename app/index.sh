#!/bin/bash
set -euo pipefail

cd /app

INPUT_PATH="${1:-/input/data}"

echo "Creating index from $INPUT_PATH"
bash create_index.sh "$INPUT_PATH"

echo "Storing index in Cassandra"
bash store_index.sh
