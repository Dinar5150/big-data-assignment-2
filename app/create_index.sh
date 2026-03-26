#!/bin/bash
set -euo pipefail

cd /app

INPUT_PATH="${1:-/input/data}"
TMP_ROOT="/tmp/indexer"
INDEX_ROOT="/indexer"
LOCAL_BUILD_DIR="/app/build/indexer"

STREAMING_JAR="$(find "$HADOOP_HOME" -name "hadoop-streaming-*.jar" | head -n 1)"

if [ -z "$STREAMING_JAR" ]; then
  echo "Could not find hadoop streaming jar"
  exit 1
fi

chmod +x mapreduce/*.py

echo "Removing old HDFS index folders"
hdfs dfs -rm -r -f "$TMP_ROOT" || true
hdfs dfs -rm -r -f "$INDEX_ROOT" || true

echo "Running mapreduce pipeline 1"
hadoop jar "$STREAMING_JAR" \
  -D mapreduce.job.name="search-index-pipeline-1" \
  -D mapreduce.job.reduces=1 \
  -input "$INPUT_PATH" \
  -output "$TMP_ROOT/pipeline1" \
  -mapper "python3 mapper1.py" \
  -reducer "python3 reducer1.py" \
  -file mapreduce/mapper1.py \
  -file mapreduce/reducer1.py

echo "Running mapreduce pipeline 2"
hadoop jar "$STREAMING_JAR" \
  -D mapreduce.job.name="search-index-pipeline-2" \
  -D mapreduce.job.reduces=1 \
  -input "$TMP_ROOT/pipeline1" \
  -output "$TMP_ROOT/pipeline2" \
  -mapper "python3 mapper2.py" \
  -reducer "python3 reducer2.py" \
  -file mapreduce/mapper2.py \
  -file mapreduce/reducer2.py

echo "Collecting pipeline outputs locally"
rm -rf "$LOCAL_BUILD_DIR"
mkdir -p "$LOCAL_BUILD_DIR/raw"

hdfs dfs -text "$TMP_ROOT/pipeline1/part-*" > "$LOCAL_BUILD_DIR/raw/pipeline1.tsv"
hdfs dfs -text "$TMP_ROOT/pipeline2/part-*" > "$LOCAL_BUILD_DIR/raw/pipeline2.tsv"

echo "Splitting final outputs"
python3 split_index_outputs.py \
  "$LOCAL_BUILD_DIR/raw/pipeline1.tsv" \
  "$LOCAL_BUILD_DIR/raw/pipeline2.tsv" \
  "$LOCAL_BUILD_DIR/final"

echo "Uploading final index outputs to HDFS"
hdfs dfs -mkdir -p "$INDEX_ROOT/vocabulary"
hdfs dfs -mkdir -p "$INDEX_ROOT/index"
hdfs dfs -mkdir -p "$INDEX_ROOT/documents"
hdfs dfs -mkdir -p "$INDEX_ROOT/stats"

hdfs dfs -put -f "$LOCAL_BUILD_DIR/final/vocabulary.tsv" "$INDEX_ROOT/vocabulary/part-00000"
hdfs dfs -put -f "$LOCAL_BUILD_DIR/final/index.tsv" "$INDEX_ROOT/index/part-00000"
hdfs dfs -put -f "$LOCAL_BUILD_DIR/final/documents.tsv" "$INDEX_ROOT/documents/part-00000"
hdfs dfs -put -f "$LOCAL_BUILD_DIR/final/stats.tsv" "$INDEX_ROOT/stats/part-00000"

echo "Final HDFS index folders"
hdfs dfs -ls "$INDEX_ROOT"
