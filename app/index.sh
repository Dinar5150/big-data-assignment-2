#!/bin/bash
set -euo pipefail

cd /app

INPUT_PATH="${1:-/input/data}"
DEFAULT_HDFS_INPUT="/input/data"

if hdfs dfs -test -e "$INPUT_PATH"; then
  HDFS_INPUT_PATH="$INPUT_PATH"
elif [ -e "$INPUT_PATH" ]; then
  mkdir -p /app/build
  LOCAL_PREPARED_FILE="/app/build/local_index_input.tsv"
  EXISTING_HDFS_FILE="/app/build/existing_index_input.tsv"
  MERGED_FILE="/app/build/merged_index_input.tsv"

  python3 prepare_index_input.py "$INPUT_PATH" "$LOCAL_PREPARED_FILE"

  if hdfs dfs -test -e "$DEFAULT_HDFS_INPUT"; then
    hdfs dfs -text "$DEFAULT_HDFS_INPUT/part-*" > "$EXISTING_HDFS_FILE"
    cat "$EXISTING_HDFS_FILE" "$LOCAL_PREPARED_FILE" > "$MERGED_FILE"
  else
    cp "$LOCAL_PREPARED_FILE" "$MERGED_FILE"
  fi

  hdfs dfs -rm -r -f "$DEFAULT_HDFS_INPUT" || true
  hdfs dfs -mkdir -p "$DEFAULT_HDFS_INPUT"
  hdfs dfs -put -f "$MERGED_FILE" "$DEFAULT_HDFS_INPUT/part-00000"
  HDFS_INPUT_PATH="$DEFAULT_HDFS_INPUT"
else
  echo "Input path not found: $INPUT_PATH"
  exit 1
fi

echo "Creating index from $HDFS_INPUT_PATH"
bash create_index.sh "$HDFS_INPUT_PATH"

echo "Storing index in Cassandra"
bash store_index.sh
