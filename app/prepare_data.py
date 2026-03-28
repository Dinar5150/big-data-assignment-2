import os
import shutil
import subprocess

from pathvalidate import sanitize_filename
from pyspark.sql import SparkSession


spark = SparkSession.builder \
    .appName('data preparation') \
    .master("local") \
    .config("spark.sql.parquet.enableVectorizedReader", "false") \
    .getOrCreate()


if os.path.isdir("data"):
    shutil.rmtree("data")
os.makedirs("data", exist_ok=True)


df = spark.read.parquet("hdfs://cluster-master:9000/a.parquet")
n = 1000
df = df.select(['id', 'title', 'text']) \
    .dropna(subset=['id', 'title', 'text']) \
    .filter("trim(text) <> ''")

total_docs = df.count()

if total_docs < n:
    raise RuntimeError(f"Not enough usable documents in parquet file: found {total_docs}, need at least {n}")

rows = df.limit(n).collect()


def create_doc(row):
    filename = "data/" + sanitize_filename(str(row['id']) + "_" + row['title']).replace(" ", "_") + ".txt"
    with open(filename, "w", encoding="utf-8") as f:
        f.write(row['text'])


for row in rows:
    create_doc(row)


subprocess.run(["hdfs", "dfs", "-rm", "-r", "-f", "/data"], check=True)
subprocess.run(["hdfs", "dfs", "-rm", "-r", "-f", "/input/data"], check=True)
subprocess.run(["hdfs", "dfs", "-mkdir", "-p", "/input"], check=True)
subprocess.run(["hdfs", "dfs", "-put", "-f", "data", "/"], check=True)


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


docs = spark.sparkContext.wholeTextFiles("hdfs://cluster-master:9000/data/*")
docs.map(doc_to_line).filter(lambda line: line is not None).coalesce(1).saveAsTextFile("hdfs://cluster-master:9000/input/data")

spark.stop()
