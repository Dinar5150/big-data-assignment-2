import os
import math
import re
import sys
from pyspark.sql import SparkSession


K1 = 1.0
B = 0.75


def tokenize(text):
    return re.findall(r"[a-z0-9]+", text.lower())


def read_query():
    return sys.stdin.read().strip()


def bm25(tf, df, doc_length, total_docs, avg_doc_length):
    if tf <= 0 or df <= 0 or total_docs <= 0 or avg_doc_length <= 0:
        return 0.0

    idf = math.log(total_docs / float(df))
    bottom = tf + K1 * ((1.0 - B) + B * (doc_length / avg_doc_length))

    if bottom == 0:
        return 0.0

    return idf * ((tf * (K1 + 1.0)) / bottom)


def write_output(sc, lines):
    output_path = os.environ.get("SEARCH_OUTPUT_PATH", "").strip()
    if not output_path:
        return

    hdfs_path = f"hdfs://cluster-master:9000{output_path}"
    filesystem = sc._jvm.org.apache.hadoop.fs.FileSystem.get(sc._jsc.hadoopConfiguration())
    filesystem.delete(sc._jvm.org.apache.hadoop.fs.Path(hdfs_path), True)
    sc.parallelize(lines, 1).saveAsTextFile(hdfs_path)


def main():
    query = read_query()
    terms = tokenize(query)

    if not terms:
        print("No query terms provided.")
        return

    spark = SparkSession.builder.appName("query").getOrCreate()
    sc = spark.sparkContext

    from cassandra.cluster import Cluster

    cluster = Cluster(["cassandra-server"])
    session = cluster.connect("search_engine")

    stats = {}
    for row in session.execute("SELECT stat_name, stat_value FROM corpus_stats"):
        stats[row.stat_name] = row.stat_value

    total_docs = int(stats.get("total_docs", 0))
    avg_doc_length = float(stats.get("avg_doc_length", 0.0))

    output_lines = []

    if total_docs == 0 or avg_doc_length == 0:
        output_lines = ["No index statistics found."]
    else:
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
                score = bm25(
                    int(posting.tf),
                    df,
                    int(posting.doc_length),
                    total_docs,
                    avg_doc_length,
                )
                rows.append((posting.doc_id, posting.title, score))

        if not rows:
            output_lines = ["No results found."]
        else:
            top = (
                sc.parallelize(rows)
                .map(lambda row: ((row[0], row[1]), row[2]))
                .reduceByKey(lambda left, right: left + right)
                .takeOrdered(10, key=lambda row: -row[1])
            )

            if not top:
                output_lines = ["No results found."]
            else:
                output_lines = [f"{doc_id}\t{title.replace('_', ' ')}" for (doc_id, title), score in top]

    write_output(sc, output_lines)

    for line in output_lines:
        print(line)

    session.shutdown()
    cluster.shutdown()
    spark.stop()


if __name__ == "__main__":
    main()
