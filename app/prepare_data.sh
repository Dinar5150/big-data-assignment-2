#!/bin/bash
set -euo pipefail

cd /app
source .venv/bin/activate

export PYSPARK_DRIVER_PYTHON="$(which python)"
unset PYSPARK_PYTHON

mkdir -p dataset

LOCAL_PARQUET_PATH="${1:-${PARQUET_LOCAL_PATH:-/app/dataset/a.parquet}}"
DOCUMENT_LIMIT="${DOCUMENT_LIMIT:-1000}"

if [ ! -f "$LOCAL_PARQUET_PATH" ]; then
  echo "Parquet file not found: $LOCAL_PARQUET_PATH"
  echo "Put one parquet file from the provided dataset in app/dataset, for example app/dataset/a.parquet, and run again."
  exit 1
fi

PARQUET_TO_USE="$LOCAL_PARQUET_PATH"
PARQUET_FILE_NAME="$(basename "$PARQUET_TO_USE")"
HDFS_PARQUET_PATH="/parquet/$PARQUET_FILE_NAME"

echo "Uploading parquet to HDFS"
hdfs dfs -mkdir -p /parquet
hdfs dfs -put -f "$PARQUET_TO_USE" "$HDFS_PARQUET_PATH"

echo "Running PySpark data preparation"
spark-submit prepare_data.py "hdfs://cluster-master:9000$HDFS_PARQUET_PATH" "$DOCUMENT_LIMIT"

echo "Data preparation finished"
hdfs dfs -ls /parquet || true
hdfs dfs -ls /data || true
hdfs dfs -ls /input || true
