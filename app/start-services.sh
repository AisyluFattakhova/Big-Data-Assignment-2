#!/bin/bash

service ssh start
ssh-keyscan -H cluster-master >> ~/.ssh/known_hosts
ssh-keyscan -H cluster-slave-1 >> ~/.ssh/known_hosts

/usr/local/hadoop/sbin/start-dfs.sh
/usr/local/hadoop/sbin/start-yarn.sh

echo "Waiting for HDFS"
until hdfs dfsadmin -safemode wait > /dev/null 2>&1; do sleep 2; done

echo "Uploading Spark jars to HDFS"
hdfs dfs -rm -r /spark 2>/dev/null
hdfs dfs -mkdir -p /spark/jars
hdfs dfs -put /usr/local/spark/jars/* /spark/jars/
echo "Spark jars uploaded."

echo "Waiting for Cassandra"
while ! python3 -c "import socket; s = socket.socket(); s.settimeout(1); s.connect(('cassandra-server', 9042))" 2>/dev/null; do 
    sleep 2
done