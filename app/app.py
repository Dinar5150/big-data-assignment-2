import subprocess
import sys
import time

from cassandra.cluster import Cluster
from cassandra.concurrent import execute_concurrent_with_args


CASSANDRA_HOST = "cassandra-server"
KEYSPACE = "search_engine"


def hdfs_lines(path):
    output = subprocess.check_output(["hdfs", "dfs", "-text", f"{path}/part-*"], text=True)
    return [line.strip() for line in output.splitlines() if line.strip()]


def connect():
    last_error = None
    for _ in range(60):
        try:
            cluster = Cluster([CASSANDRA_HOST])
            session = cluster.connect()
            return cluster, session
        except Exception as exc:
            last_error = exc
            time.sleep(5)
    raise RuntimeError(f"Could not connect to Cassandra: {last_error}")


def wait_for_schema(cluster, seconds=2):
    control = getattr(cluster, "control_connection", None)
    if control is not None:
        control.wait_for_schema_agreement(wait_time=15)
    time.sleep(seconds)


def insert_many(session, query, rows, concurrency=100, chunk_size=2000):
    prepared = session.prepare(query)
    pending = []

    for row in rows:
        pending.append(row)
        if len(pending) >= chunk_size:
            results = execute_concurrent_with_args(session, prepared, pending, concurrency=concurrency)
            for success, result in results:
                if not success:
                    raise result
            pending = []

    if pending:
        results = execute_concurrent_with_args(session, prepared, pending, concurrency=concurrency)
        for success, result in results:
            if not success:
                raise result


def main():
    index_root = sys.argv[1] if len(sys.argv) > 1 else "/indexer"

    cluster, session = connect()
    session.default_timeout = 60

    try:
        session.execute(
            f"""
            CREATE KEYSPACE IF NOT EXISTS {KEYSPACE}
            WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': 1}}
            """
        )
        wait_for_schema(cluster, seconds=1)
        session.set_keyspace(KEYSPACE)

        session.execute("DROP TABLE IF EXISTS vocabulary")
        session.execute("DROP TABLE IF EXISTS postings")
        session.execute("DROP TABLE IF EXISTS documents")
        session.execute("DROP TABLE IF EXISTS corpus_stats")
        wait_for_schema(cluster, seconds=1)

        session.execute("CREATE TABLE vocabulary (term text PRIMARY KEY, df int)")
        session.execute("CREATE TABLE postings (term text, doc_id text, tf int, title text, doc_length int, PRIMARY KEY (term, doc_id))")
        session.execute("CREATE TABLE documents (doc_id text PRIMARY KEY, title text, doc_length int)")
        session.execute("CREATE TABLE corpus_stats (stat_name text PRIMARY KEY, stat_value double)")
        wait_for_schema(cluster, seconds=2)

        vocabulary_rows = []
        for line in hdfs_lines(f"{index_root}/vocabulary"):
            term, df = line.split("\t")
            vocabulary_rows.append((term, int(df)))
        insert_many(session, "INSERT INTO vocabulary (term, df) VALUES (?, ?)", vocabulary_rows)

        document_rows = []
        for line in hdfs_lines(f"{index_root}/documents"):
            doc_id, title, doc_length = line.split("\t")
            document_rows.append((doc_id, title, int(doc_length)))
        insert_many(
            session,
            "INSERT INTO documents (doc_id, title, doc_length) VALUES (?, ?, ?)",
            document_rows,
        )

        stats_rows = []
        for line in hdfs_lines(f"{index_root}/stats"):
            stat_name, stat_value = line.split("\t")
            stats_rows.append((stat_name, float(stat_value)))
        insert_many(
            session,
            "INSERT INTO corpus_stats (stat_name, stat_value) VALUES (?, ?)",
            stats_rows,
            concurrency=10,
            chunk_size=10,
        )

        posting_rows = []
        for line in hdfs_lines(f"{index_root}/index"):
            term, df, doc_id, title, tf, doc_length = line.split("\t")
            posting_rows.append((term, doc_id, int(tf), title, int(doc_length)))
        insert_many(
            session,
            "INSERT INTO postings (term, doc_id, tf, title, doc_length) VALUES (?, ?, ?, ?, ?)",
            posting_rows,
        )

        print("done storing index")
    finally:
        session.shutdown()
        cluster.shutdown()


if __name__ == "__main__":
    main()
