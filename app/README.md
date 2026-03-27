# app

This folder is mounted to `/app` inside the master container.

## Files

- `app.sh`: runs the whole flow
- `start-services.sh`: starts Hadoop and YARN
- `prepare_data.sh`: uploads `a.parquet` and runs PySpark data preparation
- `create_index.sh`: runs the MapReduce jobs
- `store_index.sh`: stores the index in Cassandra
- `index.sh`: runs create + store
- `search.sh`: runs a query on YARN

## Paths used

- local parquet: `/app/a.parquet`
- fallback parquet: `/app/dataset/a.parquet`
- HDFS documents: `/data`
- HDFS input: `/input/data`
- HDFS temp index: `/tmp/indexer`
- HDFS final index: `/indexer`
