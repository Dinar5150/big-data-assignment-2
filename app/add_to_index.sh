#!/bin/bash
set -euo pipefail

cd /app
source .venv/bin/activate

if [ $# -ne 1 ]; then
    echo "Please pass one local text file."
    exit 1
fi

LOCAL_FILE="$1"

if [ ! -f "$LOCAL_FILE" ]; then
    echo "Missing local file: $LOCAL_FILE"
    exit 1
fi

DOC_TITLE="$(basename "$LOCAL_FILE")"
DOC_TITLE="${DOC_TITLE%.txt}"
DOC_ID="$(date +%s)"
SAFE_TITLE="$(printf '%s' "$DOC_TITLE" | tr ' ' '_' | tr -cd '[:alnum:]_.-')"
HDFS_NAME="${DOC_ID}_${SAFE_TITLE}.txt"

hdfs dfs -mkdir -p /data
hdfs dfs -put -f "$LOCAL_FILE" "/data/$HDFS_NAME"

REBUILD_SCRIPT="/tmp/rebuild_input.py"
cat > "$REBUILD_SCRIPT" <<'PY'
from pyspark.sql import SparkSession


spark = SparkSession.builder.appName("rebuild input").master("local").getOrCreate()

spark.sparkContext._jsc.hadoopConfiguration().set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "true")
spark._jvm.org.apache.hadoop.fs.FileSystem.get(
    spark._jsc.hadoopConfiguration()
).delete(
    spark._jvm.org.apache.hadoop.fs.Path("hdfs://cluster-master:9000/input/data"),
    True,
)


def doc_to_line(item):
    import os

    path, text = item
    filename = os.path.basename(path)

    if not filename.endswith(".txt") or "_" not in filename:
        return None

    doc_id, title = filename[:-4].split("_", 1)
    text = " ".join((text or "").split())

    if not text:
        return None

    return "\t".join([doc_id, title, text])


docs = spark.sparkContext.wholeTextFiles("hdfs://cluster-master:9000/data/*")
docs.map(doc_to_line).filter(lambda line: line is not None).coalesce(1).saveAsTextFile("hdfs://cluster-master:9000/input/data")

spark.stop()
PY

spark-submit "$REBUILD_SCRIPT"
rm -f "$REBUILD_SCRIPT"

bash index.sh

echo "Indexed document:"
echo -e "${DOC_ID}\t${DOC_TITLE}"
