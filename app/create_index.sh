#!/bin/bash
STREAM_JAR=$(find /usr/local/hadoop/share/hadoop/tools/lib/ -name "hadoop-streaming*.jar" | head -n 1)
# for indexing
hdfs dfs -rm -r /indexer/index 2>/dev/null
hadoop jar $STREAM_JAR \
    -file /app/mapreduce/mapper1.py -mapper "/usr/bin/python3 mapper1.py" \
    -file /app/mapreduce/reducer1.py -reducer "/usr/bin/python3 reducer1.py" \
    -input /input/data \
    -output /indexer/index

# For doc stats
hdfs dfs -rm -r /indexer/stats 2>/dev/null
hadoop jar $STREAM_JAR \
    -file /app/mapreduce/mapper2.py -mapper "/usr/bin/python3 mapper2.py" \
    -file /app/mapreduce/reducer2.py -reducer "/usr/bin/python3 reducer2.py" \
    -input /input/data \
    -output /indexer/stats