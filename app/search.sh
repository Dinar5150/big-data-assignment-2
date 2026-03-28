#!/bin/bash
set -euo pipefail

cd /app

QUERY="${*:-}"
PYTHON_BIN="/usr/bin/python3"
PYTHON_SITE_PACKAGES="/app/.venv/lib/python3.8/site-packages:/app/.venv/lib64/python3.8/site-packages"

if [ -z "$QUERY" ]; then
    echo "Usage: bash search.sh \"your query\""
    exit 1
fi

env \
    -u VIRTUAL_ENV \
    -u PYTHONHOME \
    -u PYSPARK_DRIVER_PYTHON \
    -u PYSPARK_PYTHON \
    PYTHONPATH="${PYTHON_SITE_PACKAGES}" \
    /usr/local/spark/bin/spark-submit \
    --master yarn \
    --deploy-mode cluster \
    --driver-memory 512m \
    --num-executors 1 \
    --executor-cores 1 \
    --executor-memory 512m \
    --conf spark.yarn.am.waitTime=300s \
    --conf spark.pyspark.python="${PYTHON_BIN}" \
    --conf spark.pyspark.driver.python="${PYTHON_BIN}" \
    --conf spark.executorEnv.PYSPARK_PYTHON="${PYTHON_BIN}" \
    --conf spark.yarn.appMasterEnv.PYSPARK_PYTHON="${PYTHON_BIN}" \
    --conf spark.executorEnv.PYTHONPATH="${PYTHON_SITE_PACKAGES}" \
    --conf spark.yarn.appMasterEnv.PYTHONPATH="${PYTHON_SITE_PACKAGES}" \
    query.py "$QUERY"
