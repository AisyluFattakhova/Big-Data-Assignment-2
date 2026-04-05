#!/bin/bash

echo "Starting data preparation"



# Run Spark job
# spark-submit --master yarn --deploy-mode client /app/prepare_data.py || \
spark-submit --master local[*] /app/prepare_data.py

echo "Checking HDFS data"

hdfs dfs -ls /data
hdfs dfs -ls /input/data

echo "Data preparation finished"