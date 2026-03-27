import os
import shutil
import subprocess

from pathvalidate import sanitize_filename
from tqdm import tqdm
from pyspark.sql import SparkSession


spark = SparkSession.builder \
    .appName('data preparation') \
    .master("local") \
    .config("spark.sql.parquet.enableVectorizedReader", "true") \
    .getOrCreate()


if os.path.isdir("data"):
    shutil.rmtree("data")
os.makedirs("data", exist_ok=True)


df = spark.read.parquet("/a.parquet")
n = 1000
df = df.select(['id', 'title', 'text']) \
    .dropna(subset=['id', 'title', 'text']) \
    .filter("trim(text) <> ''") \
    .sample(fraction=100 * n / df.count(), seed=0) \
    .limit(n)


def create_doc(row):
    filename = "data/" + sanitize_filename(str(row['id']) + "_" + row['title']).replace(" ", "_") + ".txt"
    with open(filename, "w", encoding="utf-8") as f:
        f.write(row['text'])


df.foreach(create_doc)


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
