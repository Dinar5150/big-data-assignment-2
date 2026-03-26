import math
import os
import re
import sys
from collections import Counter

from cassandra.cluster import Cluster
from pyspark.sql import SparkSession


CASSANDRA_HOST = os.environ.get("CASSANDRA_HOST", "cassandra-server")
KEYSPACE = os.environ.get("CASSANDRA_KEYSPACE", "search_engine")
K1 = float(os.environ.get("BM25_K1", "1.0"))
B = float(os.environ.get("BM25_B", "0.75"))


def tokenize(text):
    return re.findall(r"[a-z0-9]+", text.lower())


def read_query():
    stdin_value = sys.stdin.read().strip()
    if stdin_value:
        return stdin_value
    if len(sys.argv) > 1:
        return " ".join(sys.argv[1:]).strip()
    return ""


def load_stats(session):
    rows = session.execute("SELECT stat_name, stat_value FROM corpus_stats")
    stats = {row.stat_name: row.stat_value for row in rows}
    total_docs = int(stats.get("total_docs", 0))
    avg_doc_length = float(stats.get("avg_doc_length", 0.0))
    return total_docs, avg_doc_length


def bm25_score(tf, df, doc_length, total_docs, avg_doc_length, qtf):
    if tf <= 0 or df <= 0 or total_docs <= 0 or avg_doc_length <= 0:
        return 0.0

    idf = math.log(total_docs / float(df))
    denominator = tf + K1 * ((1.0 - B) + B * (doc_length / avg_doc_length))
    if denominator == 0:
        return 0.0

    base_score = idf * ((tf * (K1 + 1.0)) / denominator)
    return base_score * qtf


def main():
    query_text = read_query()
    query_terms = tokenize(query_text)

    if not query_terms:
        print("No query terms provided.")
        return

    spark = SparkSession.builder.appName("bm25-query").getOrCreate()
    sc = spark.sparkContext

    cluster = Cluster([CASSANDRA_HOST])
    session = cluster.connect(KEYSPACE)

    try:
        total_docs, avg_doc_length = load_stats(session)
        if total_docs == 0:
            print("No index statistics found.")
            return

        query_counter = Counter(query_terms)
        df_stmt = session.prepare("SELECT df FROM vocabulary WHERE term = ?")
        postings_stmt = session.prepare(
            "SELECT term, doc_id, tf, title, doc_length FROM postings WHERE term = ?"
        )

        rows_for_spark = []
        for term, qtf in query_counter.items():
            df_row = session.execute(df_stmt, (term,)).one()
            if df_row is None:
                continue

            df_value = int(df_row.df)
            for posting in session.execute(postings_stmt, (term,)):
                rows_for_spark.append(
                    (
                        posting.term,
                        posting.doc_id,
                        posting.title,
                        int(posting.tf),
                        int(posting.doc_length),
                        df_value,
                        qtf,
                    )
                )

        if not rows_for_spark:
            print("No results found.")
            return

        rdd = sc.parallelize(rows_for_spark, 2)
        scored = (
            rdd.map(
                lambda row: (
                    (row[1], row[2]),
                    bm25_score(row[3], row[5], row[4], total_docs, avg_doc_length, row[6]),
                )
            )
            .reduceByKey(lambda left, right: left + right)
        )

        top_results = scored.takeOrdered(10, key=lambda item: -item[1])

        if not top_results:
            print("No results found.")
            return

        for (doc_id, title), score in top_results:
            print(f"{doc_id}\t{title.replace('_', ' ')}")
    finally:
        session.shutdown()
        cluster.shutdown()
        spark.stop()


if __name__ == "__main__":
    main()
