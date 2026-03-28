#!/bin/bash
set -euo pipefail

cd /app
source .venv/bin/activate

if [ $# -ne 1 ] && [ $# -ne 3 ]; then
    echo "Please pass one local text file."
    echo "You can also pass a document id and title."
    exit 1
fi

LOCAL_FILE="$1"

if [ ! -f "$LOCAL_FILE" ]; then
    echo "Missing local file: $LOCAL_FILE"
    exit 1
fi

mapfile -t DOC_INFO < <(python3 - "$@" <<'PY'
import hashlib
import os
import re
import sys
import tempfile

from pathvalidate import sanitize_filename


def infer_doc_id(path):
    filename = os.path.splitext(os.path.basename(path))[0]
    match = re.match(r"^(\d+)[_-](.+)$", filename)
    if match:
        return match.group(1), match.group(2).replace("_", " ")

    cleaned = re.sub(r"[^A-Za-z0-9]+", " ", filename).strip()
    if not cleaned:
        cleaned = "Added document"

    digest = hashlib.sha1(os.path.abspath(path).encode("utf-8")).hexdigest()
    generated_id = str(int(digest[:12], 16) % 900000000 + 100000000)
    return generated_id, cleaned


args = sys.argv[1:]
local_file = args[0]

if len(args) == 3:
    doc_id = args[1]
    doc_title = args[2]
else:
    doc_id, doc_title = infer_doc_id(local_file)

with open(local_file, "r", encoding="utf-8") as handle:
    text = handle.read().strip()

if not text:
    raise RuntimeError("The local file is empty.")

filename = sanitize_filename(f"{doc_id}_{doc_title}").replace(" ", "_") + ".txt"

with tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8", suffix=".txt") as temp:
    temp.write(text)
    temp_path = temp.name

print(temp_path)
print(doc_id)
print(doc_title)
print(filename)
PY
)

TEMP_FILE="${DOC_INFO[0]}"
DOC_ID="${DOC_INFO[1]}"
DOC_TITLE="${DOC_INFO[2]}"
HDFS_NAME="${DOC_INFO[3]}"

cleanup() {
    rm -f "$TEMP_FILE"
}

trap cleanup EXIT

hdfs dfs -mkdir -p /data
hdfs dfs -put -f "$TEMP_FILE" "/data/$HDFS_NAME"

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
