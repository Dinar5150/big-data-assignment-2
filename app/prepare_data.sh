#!/bin/bash
set -euo pipefail

cd /app
source .venv/bin/activate

export PYSPARK_DRIVER_PYTHON="$(which python)"
unset PYSPARK_PYTHON

mkdir -p dataset

LOCAL_PARQUET_PATH="${PARQUET_LOCAL_PATH:-/app/dataset/a.parquet}"
FALLBACK_PARQUET_PATH="/app/dataset/source.parquet"
DEFAULT_PARQUET_URL="https://huggingface.co/datasets/wikimedia/wikipedia/resolve/main/20231101.simple/train-00000-of-00001.parquet?download=true"
PARQUET_URL="${PARQUET_URL:-$DEFAULT_PARQUET_URL}"
DOCUMENT_LIMIT="${DOCUMENT_LIMIT:-1000}"

if [ -f "$LOCAL_PARQUET_PATH" ]; then
  PARQUET_TO_USE="$LOCAL_PARQUET_PATH"
  echo "Using local parquet file: $PARQUET_TO_USE"
else
  PARQUET_TO_USE="$FALLBACK_PARQUET_PATH"
  if [ ! -f "$PARQUET_TO_USE" ]; then
    echo "Local Kaggle parquet not found, downloading fallback parquet"
    python -c "import sys, urllib.request; urllib.request.urlretrieve(sys.argv[1], sys.argv[2])" "$PARQUET_URL" "$PARQUET_TO_USE"
  fi
fi

PARQUET_FILE_NAME="$(basename "$PARQUET_TO_USE")"
HDFS_PARQUET_PATH="/parquet/$PARQUET_FILE_NAME"

echo "Uploading parquet to HDFS"
hdfs dfs -mkdir -p /parquet
hdfs dfs -put -f "$PARQUET_TO_USE" "$HDFS_PARQUET_PATH"

echo "Running PySpark data preparation"
spark-submit \
  --master local[1] \
  --conf spark.driver.memory=1g \
  --conf spark.executor.memory=512m \
  --conf spark.default.parallelism=1 \
  --conf spark.sql.shuffle.partitions=1 \
  prepare_data.py "hdfs://cluster-master:9000$HDFS_PARQUET_PATH" "$DOCUMENT_LIMIT"

echo "Data preparation finished"
hdfs dfs -ls /parquet || true
hdfs dfs -ls /data || true
hdfs dfs -ls /input || true
