#!/bin/bash
set -euo pipefail

cd /app
source .venv/bin/activate

QUERY="${*:-}"

if [ -z "$QUERY" ]; then
  echo "Usage: bash search.sh \"your query\""
  exit 1
fi

export PYSPARK_DRIVER_PYTHON="$(which python)"
export PYSPARK_PYTHON=./.venv/bin/python

printf '%s\n' "$QUERY" | spark-submit \
  --master yarn \
  --archives /app/.venv.tar.gz#.venv \
  --conf spark.executor.instances=1 \
  --conf spark.executor.cores=1 \
  --conf spark.executor.memory=512m \
  --conf spark.driver.memory=512m \
  --conf spark.executorEnv.PYSPARK_PYTHON=./.venv/bin/python \
  --conf spark.yarn.appMasterEnv.PYSPARK_PYTHON=./.venv/bin/python \
  --conf spark.default.parallelism=2 \
  --conf spark.sql.shuffle.partitions=2 \
  query.py
