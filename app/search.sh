#!/bin/bash
set -euo pipefail

cd /app
source .venv/bin/activate

QUERY="${*:-}"

if [ -z "$QUERY" ]; then
    echo "Usage: bash search.sh \"your query\""
    exit 1
fi

export PYSPARK_PYTHON=./.venv/bin/python

spark-submit \
    --master yarn \
    --deploy-mode cluster \
    --archives /app/.venv.tar.gz#.venv \
    --conf spark.executorEnv.PYSPARK_PYTHON=./.venv/bin/python \
    --conf spark.yarn.appMasterEnv.PYSPARK_PYTHON=./.venv/bin/python \
    query.py "$QUERY"
