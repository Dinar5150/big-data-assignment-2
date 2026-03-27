import math
import re
import sys

from cassandra.cluster import Cluster
from pyspark.sql import SparkSession


K1 = 1.0
B = 0.75


def tokenize(text):
    return re.findall(r"[a-z0-9]+", text.lower())


def read_query():
    query = sys.stdin.read().strip()
    if not query and len(sys.argv) > 1:
        query = " ".join(sys.argv[1:]).strip()
    return query


def bm25(tf, df, doc_length, total_docs, avg_doc_length):
    if tf <= 0 or df <= 0 or total_docs <= 0 or avg_doc_length <= 0:
        return 0.0

    idf = math.log(total_docs / float(df))
    bottom = tf + K1 * ((1.0 - B) + B * (doc_length / avg_doc_length))

    if bottom == 0:
        return 0.0

    return idf * ((tf * (K1 + 1.0)) / bottom)


query = read_query()
terms = tokenize(query)

if not terms:
    print("No query terms provided.")
    sys.exit(0)


spark = SparkSession.builder.appName("query").getOrCreate()
sc = spark.sparkContext

cluster = Cluster(["cassandra-server"])
session = cluster.connect("search_engine")

try:
    stats = {}
    for row in session.execute("SELECT stat_name, stat_value FROM corpus_stats"):
        stats[row.stat_name] = row.stat_value

    total_docs = int(stats.get("total_docs", 0))
    avg_doc_length = float(stats.get("avg_doc_length", 0.0))

    if total_docs == 0 or avg_doc_length == 0:
        print("No index statistics found.")
        sys.exit(0)

    rows = []
    for term in terms:
        df_row = session.execute("SELECT df FROM vocabulary WHERE term = %s", (term,)).one()
        if df_row is None:
            continue

        df = int(df_row.df)
        postings = session.execute(
            "SELECT doc_id, title, tf, doc_length FROM postings WHERE term = %s",
            (term,),
        )

        for posting in postings:
            score = bm25(int(posting.tf), df, int(posting.doc_length), total_docs, avg_doc_length)
            rows.append((posting.doc_id, posting.title, score))

    if not rows:
        print("No results found.")
        sys.exit(0)

    top = (
        sc.parallelize(rows)
        .map(lambda row: ((row[0], row[1]), row[2]))
        .reduceByKey(lambda left, right: left + right)
        .takeOrdered(10, key=lambda row: -row[1])
    )

    if not top:
        print("No results found.")
        sys.exit(0)

    for (doc_id, title), score in top:
        print(f"{doc_id}\t{title.replace('_', ' ')}")
finally:
    session.shutdown()
    cluster.shutdown()
    spark.stop()
