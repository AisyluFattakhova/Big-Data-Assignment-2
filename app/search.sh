#!/bin/bash

QUERY=$1

# Fail immediately if anything fails
set -e

# 1. Hadoop/YARN Paths
export HADOOP_HOME=/usr/local/hadoop
export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
export YARN_CONF_DIR=$HADOOP_HOME/etc/hadoop
export SPARK_DIST_CLASSPATH=$(hadoop classpath)

# 2. Java 17 compatibility
JAVA_OPTS="--add-opens=java.base/java.lang=ALL-UNNAMED \
--add-opens=java.base/java.lang.invoke=ALL-UNNAMED \
--add-opens=java.base/java.lang.reflect=ALL-UNNAMED \
--add-opens=java.base/java.io=ALL-UNNAMED \
--add-opens=java.base/java.net=ALL-UNNAMED \
--add-opens=java.base/java.nio=ALL-UNNAMED \
--add-opens=java.base/java.util=ALL-UNNAMED \
--add-opens=java.base/java.util.concurrent=ALL-UNNAMED \
--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED \
--add-opens=java.base/sun.nio.ch=ALL-UNNAMED \
--add-opens=java.base/sun.nio.cs=ALL-UNNAMED \
--add-opens=java.base/sun.security.action=ALL-UNNAMED \
--add-opens=java.management/java.lang.management=ALL-UNNAMED"

echo "Searching for '$QUERY'..."

# 3. Submit to YARN
spark-submit \
    --master yarn \
    --deploy-mode client \
    --driver-java-options "$JAVA_OPTS" \
    --conf "spark.executor.extraJavaOptions=$JAVA_OPTS" \
    --conf spark.ui.enabled=false \
    --conf spark.yarn.jars=hdfs:///spark/jars/* \
    /app/query.py "$QUERY"