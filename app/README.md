# app

All runnable assignment files are inside this folder because the template mounts `./app` to `/app` in the master container.

## Main scripts

- `app.sh`: starts services, installs dependencies, prepares data, builds the index, and runs a sample search
- `start-services.sh`: starts HDFS, YARN, and MapReduce history server
- `prepare_data.sh`: uses one parquet file from `app/dataset`, puts it in HDFS, and runs the PySpark data preparation
- `create_index.sh`: runs the Hadoop streaming jobs and writes index outputs to HDFS
- `store_index.sh`: reads index outputs from HDFS and stores them in Cassandra
- `index.sh`: runs `create_index.sh` and then `store_index.sh`
- `search.sh`: runs `query.py` on YARN

## HDFS folders used

- `/parquet`: source parquet file
- `/data`: plain text documents
- `/input/data`: one-partition MapReduce input
- `/tmp/indexer`: intermediate MapReduce outputs
- `/indexer`: final index outputs

## Cassandra keyspace

- Keyspace: `search_engine`
