#!/bin/bash
set -euo pipefail

cd /app
source .venv/bin/activate

export PYSPARK_DRIVER_PYTHON=$(which python)
unset PYSPARK_PYTHON

PARQUET_FILE="/app/a.parquet"

if [ ! -f "$PARQUET_FILE" ]; then
    echo "Missing a.parquet"
    echo "Put a.parquet in app/ and run again."
    exit 1
fi

hdfs dfs -put -f "$PARQUET_FILE" / && \
    spark-submit --driver-memory 2g prepare_data.py && \
    echo "done data preparation!" && \
    hdfs dfs -ls /data && \
    hdfs dfs -ls /input/data
