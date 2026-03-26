#!/bin/bash
set -euo pipefail

echo "Starting HDFS daemons"
$HADOOP_HOME/sbin/start-dfs.sh

echo "Starting YARN daemons"
$HADOOP_HOME/sbin/start-yarn.sh

echo "Starting MapReduce history server"
mapred --daemon start historyserver

echo "Waiting a bit for HDFS"
sleep 10

echo "Leaving safe mode if needed"
hdfs dfsadmin -safemode leave || true

echo "Creating Spark jar folder in HDFS"
hdfs dfs -mkdir -p /apps/spark/jars
hdfs dfs -put -f /usr/local/spark/jars/* /apps/spark/jars/
hdfs dfs -chmod -R 755 /apps/spark/jars

echo "Creating root HDFS home"
hdfs dfs -mkdir -p /user/root

echo "HDFS report"
hdfs dfsadmin -report || true

echo "Running processes"
jps -lm || true
