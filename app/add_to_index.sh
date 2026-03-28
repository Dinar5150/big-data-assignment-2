#!/bin/bash
set -euo pipefail

cd /app
source .venv/bin/activate

if [ $# -ne 1 ] && [ $# -ne 3 ]; then
    echo "Usage: bash add_to_index.sh <local_file> [doc_id doc_title]"
    exit 1
fi

LOCAL_FILE="$1"

if [ ! -f "$LOCAL_FILE" ]; then
    echo "Missing local file: $LOCAL_FILE"
    exit 1
fi

mapfile -t DOC_INFO < <(python3 - "$@" <<'PY'
import os
import sys
import tempfile
import time
from pathlib import Path

from pathvalidate import sanitize_filename


def infer_metadata(local_file, doc_id_arg, title_arg):
    if doc_id_arg and title_arg:
        return doc_id_arg, title_arg

    stem = Path(local_file).stem

    if "_" in stem:
        doc_id, title = stem.split("_", 1)
        if doc_id and title:
            return doc_id, title.replace("_", " ")

    return str(int(time.time())), stem.replace("_", " ")


def main():
    if len(sys.argv) not in (2, 4):
        raise SystemExit("Usage: python - <local_file> [doc_id doc_title]")

    local_file = sys.argv[1]
    doc_id_arg = sys.argv[2] if len(sys.argv) == 4 else ""
    title_arg = sys.argv[3] if len(sys.argv) == 4 else ""

    if not os.path.isfile(local_file):
        raise SystemExit(f"Missing local file: {local_file}")

    with open(local_file, "r", encoding="utf-8") as f:
        text = f.read()

    if not text.strip():
        raise SystemExit("Local file is empty.")

    doc_id, title = infer_metadata(local_file, doc_id_arg, title_arg)
    hdfs_name = sanitize_filename(f"{doc_id}_{title}").replace(" ", "_") + ".txt"

    fd, temp_path = tempfile.mkstemp(prefix="add_to_index_", suffix=".txt")
    os.close(fd)

    with open(temp_path, "w", encoding="utf-8") as f:
        f.write(text)

    print(temp_path)
    print(doc_id)
    print(title)
    print(hdfs_name)


if __name__ == "__main__":
    main()
PY
)

TEMP_FILE="${DOC_INFO[0]}"
DOC_ID="${DOC_INFO[1]}"
DOC_TITLE="${DOC_INFO[2]}"
HDFS_NAME="${DOC_INFO[3]}"
REBUILD_SCRIPT=""

cleanup() {
    rm -f "$TEMP_FILE"
    if [ -n "${REBUILD_SCRIPT:-}" ]; then
        rm -f "$REBUILD_SCRIPT"
    fi
}

trap cleanup EXIT

hdfs dfs -mkdir -p /data
hdfs dfs -put -f "$TEMP_FILE" "/data/$HDFS_NAME"

REBUILD_SCRIPT="$(mktemp /tmp/rebuild_input_XXXX.py)"
cat > "$REBUILD_SCRIPT" <<'PY'
import os
import subprocess

from pyspark.sql import SparkSession


def doc_to_line(item):
    path, text = item
    filename = os.path.basename(path)

    if not filename.endswith(".txt") or "_" not in filename:
        return None

    doc_id, title = filename[:-4].split("_", 1)
    text = " ".join((text or "").split())

    if not text:
        return None

    return "\t".join([doc_id, title, text])


def main():
    spark = (
        SparkSession.builder
        .appName("rebuild input")
        .master("local")
        .getOrCreate()
    )

    try:
        subprocess.run(["hdfs", "dfs", "-rm", "-r", "-f", "/input/data"], check=True)
        subprocess.run(["hdfs", "dfs", "-mkdir", "-p", "/input"], check=True)

        docs = spark.sparkContext.wholeTextFiles("hdfs://cluster-master:9000/data/*")
        (
            docs.map(doc_to_line)
            .filter(lambda line: line is not None)
            .coalesce(1)
            .saveAsTextFile("hdfs://cluster-master:9000/input/data")
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
PY

spark-submit "$REBUILD_SCRIPT"
bash index.sh

echo "Indexed document:"
echo -e "${DOC_ID}\t${DOC_TITLE}"
