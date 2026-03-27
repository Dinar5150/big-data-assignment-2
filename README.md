# big-data-assignment-2

Simple search engine assignment using PySpark, Hadoop MapReduce, and Cassandra.

## Run

1. Put `a.parquet` in `app/`.
2. If you want, `app/dataset/a.parquet` also works as a fallback.
3. Run:

```bash
docker compose up
```

The master container mounts `app/` to `/app` and runs `/app/app.sh`.

## Main HDFS paths

- `/data`
- `/input/data`
- `/tmp/indexer`
- `/indexer`
