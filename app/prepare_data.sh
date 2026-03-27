#!/bin/bash
set -euo pipefail

cd /app
source .venv/bin/activate

export PYSPARK_DRIVER_PYTHON="$(which python)"
unset PYSPARK_PYTHON

if [ ! -f /app/dataset/a.parquet ]; then
    echo "Missing /app/dataset/a.parquet"
    exit 1
fi

hdfs dfs -put -f /app/dataset/a.parquet / && \
    spark-submit prepare_data.py && \
    echo "Putting data to hdfs" && \
    hdfs dfs -ls /data && \
    hdfs dfs -ls /input/data && \
    echo "done data preparation!"
