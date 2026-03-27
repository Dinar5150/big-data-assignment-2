#!/bin/bash
set -euo pipefail

cd /app

INPUT_PATH="${1:-/input/data}"
TMP_PATH="/tmp/indexer"
INDEX_PATH="/indexer"
BUILD_PATH="/app/build/indexer"
STREAMING_JAR=$(find "$HADOOP_HOME" -name "hadoop-streaming-*.jar" | head -n 1)

if [ -z "$STREAMING_JAR" ]; then
    echo "Missing hadoop streaming jar"
    exit 1
fi

chmod +x mapreduce/*.py

hdfs dfs -rm -r -f "$TMP_PATH" || true
hdfs dfs -rm -r -f "$INDEX_PATH" || true

hadoop jar "$STREAMING_JAR" \
    -D mapreduce.job.name="index-1" \
    -D mapreduce.job.reduces=1 \
    -input "$INPUT_PATH" \
    -output "$TMP_PATH/pipeline1" \
    -mapper "python3 mapper1.py" \
    -reducer "python3 reducer1.py" \
    -file mapreduce/mapper1.py \
    -file mapreduce/reducer1.py

hadoop jar "$STREAMING_JAR" \
    -D mapreduce.job.name="index-2" \
    -D mapreduce.job.reduces=1 \
    -input "$TMP_PATH/pipeline1" \
    -output "$TMP_PATH/pipeline2" \
    -mapper "python3 mapper2.py" \
    -reducer "python3 reducer2.py" \
    -file mapreduce/mapper2.py \
    -file mapreduce/reducer2.py

rm -rf "$BUILD_PATH"
mkdir -p "$BUILD_PATH"

hdfs dfs -text "$TMP_PATH/pipeline1/part-*" > "$BUILD_PATH/pipeline1.tsv"
hdfs dfs -text "$TMP_PATH/pipeline2/part-*" > "$BUILD_PATH/pipeline2.tsv"

python3 split_index_outputs.py "$BUILD_PATH/pipeline1.tsv" "$BUILD_PATH/pipeline2.tsv" "$BUILD_PATH"

hdfs dfs -mkdir -p "$INDEX_PATH/vocabulary"
hdfs dfs -mkdir -p "$INDEX_PATH/index"
hdfs dfs -mkdir -p "$INDEX_PATH/documents"
hdfs dfs -mkdir -p "$INDEX_PATH/stats"

hdfs dfs -put -f "$BUILD_PATH/vocabulary.tsv" "$INDEX_PATH/vocabulary/part-00000"
hdfs dfs -put -f "$BUILD_PATH/index.tsv" "$INDEX_PATH/index/part-00000"
hdfs dfs -put -f "$BUILD_PATH/documents.tsv" "$INDEX_PATH/documents/part-00000"
hdfs dfs -put -f "$BUILD_PATH/stats.tsv" "$INDEX_PATH/stats/part-00000"

hdfs dfs -ls "$INDEX_PATH"
