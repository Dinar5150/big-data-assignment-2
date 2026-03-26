import glob
import os
import re
import shutil
import subprocess
import sys

from pathvalidate import sanitize_filename
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


HDFS_URI = os.environ.get("HDFS_URI", "hdfs://cluster-master:9000")
LOCAL_DATA_DIR = "/app/data"


def run_cmd(command):
    subprocess.run(command, check=True)


def make_safe_title(title):
    title = (title or "").strip()
    title = re.sub(r"\s+", "_", title)
    title = sanitize_filename(title)
    title = title.replace(" ", "_")
    if not title:
        title = "untitled"
    return title[:120]


def clean_text(text):
    text = text or ""
    return re.sub(r"\s+", " ", text).strip()


def make_safe_doc_id(doc_id):
    doc_id = re.sub(r"[^0-9A-Za-z-]", "", doc_id)
    if not doc_id:
        doc_id = "0"
    return doc_id


def prepare_local_folder():
    if os.path.isdir(LOCAL_DATA_DIR):
        shutil.rmtree(LOCAL_DATA_DIR)
    os.makedirs(LOCAL_DATA_DIR, exist_ok=True)


def parse_doc_from_path(item):
    path, text = item
    filename = os.path.basename(path)
    if not filename.endswith(".txt"):
        return None

    base_name = filename[:-4]
    if "_" not in base_name:
        return None

    doc_id, title = base_name.split("_", 1)
    text = clean_text(text)
    if not text:
        return None

    return "\t".join([doc_id, title, text])


def main():
    parquet_path = sys.argv[1] if len(sys.argv) > 1 else f"{HDFS_URI}/parquet/a.parquet"
    document_limit = int(sys.argv[2]) if len(sys.argv) > 2 else 1000

    spark = (
        SparkSession.builder
        .appName("prepare-data")
        .master("local[1]")
        .config("spark.sql.parquet.enableVectorizedReader", "true")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )

    prepare_local_folder()

    df = (
        spark.read.parquet(parquet_path)
        .select("id", "title", "text")
        .where(F.col("id").isNotNull())
        .where(F.col("title").isNotNull())
        .where(F.col("text").isNotNull())
        .where(F.length(F.trim(F.col("text"))) > 0)
        .limit(document_limit)
    )

    written = 0
    for row in df.toLocalIterator():
        doc_id = make_safe_doc_id(str(row["id"]).strip())
        title = make_safe_title(str(row["title"]))
        text = clean_text(row["text"])
        if not doc_id or not text:
            continue

        file_name = f"{doc_id}_{title}.txt"
        file_path = os.path.join(LOCAL_DATA_DIR, file_name)
        with open(file_path, "w", encoding="utf-8") as handle:
            handle.write(text)
        written += 1

    if written < document_limit:
        raise RuntimeError(f"Only wrote {written} documents, expected at least {document_limit}.")

    print(f"Wrote {written} documents.")

    run_cmd(["hdfs", "dfs", "-rm", "-r", "-f", "/data"])
    run_cmd(["hdfs", "dfs", "-rm", "-r", "-f", "/input/data"])
    run_cmd(["hdfs", "dfs", "-mkdir", "-p", "/data"])
    for file_path in sorted(glob.glob(os.path.join(LOCAL_DATA_DIR, "*.txt"))):
        run_cmd(["hdfs", "dfs", "-put", "-f", file_path, "/data"])

    docs_rdd = spark.sparkContext.wholeTextFiles(f"{HDFS_URI}/data/*", minPartitions=1)
    input_rdd = docs_rdd.map(parse_doc_from_path).filter(lambda item: item is not None).coalesce(1)
    input_rdd.saveAsTextFile(f"{HDFS_URI}/input/data")

    spark.stop()


if __name__ == "__main__":
    main()
