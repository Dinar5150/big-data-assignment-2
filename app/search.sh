#!/bin/bash
set -euo pipefail

cd /app

QUERY="${*:-}"
PYTHON_BIN="/usr/bin/python3"
PYTHON_SITE_PACKAGES="/app/.venv/lib/python3.8/site-packages:/app/.venv/lib64/python3.8/site-packages"
QUERY_FILE="/tmp/query_input.txt"
RUNNER_FILE="/tmp/query_entry.py"

if [ -z "$QUERY" ]; then
    echo "Please pass a query."
    exit 1
fi

printf '%s\n' "$QUERY" > "$QUERY_FILE"
cat > "$RUNNER_FILE" <<'PY'
import io
import sys

with open("query_input.txt", "r", encoding="utf-8") as handle:
    sys.stdin = io.StringIO(handle.read())

import query

query.main()
PY

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
    --files "${QUERY_FILE}" \
    --py-files query.py \
    --conf spark.yarn.am.waitTime=300s \
    --conf spark.pyspark.python="${PYTHON_BIN}" \
    --conf spark.pyspark.driver.python="${PYTHON_BIN}" \
    --conf spark.executorEnv.PYSPARK_PYTHON="${PYTHON_BIN}" \
    --conf spark.yarn.appMasterEnv.PYSPARK_PYTHON="${PYTHON_BIN}" \
    --conf spark.executorEnv.PYTHONPATH="${PYTHON_SITE_PACKAGES}" \
    --conf spark.yarn.appMasterEnv.PYTHONPATH="${PYTHON_SITE_PACKAGES}" \
    "$RUNNER_FILE"

rm -f "$QUERY_FILE"
rm -f "$RUNNER_FILE"
