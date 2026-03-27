#!/bin/bash
set -euo pipefail

$HADOOP_HOME/sbin/start-dfs.sh
$HADOOP_HOME/sbin/start-yarn.sh
mapred --daemon start historyserver

hdfs dfsadmin -safemode leave || true

hdfs dfs -mkdir -p /apps/spark/jars
hdfs dfs -put -f /usr/local/spark/jars/* /apps/spark/jars/
hdfs dfs -chmod -R 755 /apps/spark
hdfs dfs -mkdir -p /user/root

jps -lm
