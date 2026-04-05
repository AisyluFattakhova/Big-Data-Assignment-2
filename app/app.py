from pyspark.sql import SparkSession

def run():
    
   
    spark = SparkSession.builder \
    .appName("HdfsToCassandra") \
    .config("spark.driver.extraJavaOptions", "--add-opens=java.base/java.lang=ALL-UNNAMED") \
    .config("spark.executor.extraJavaOptions", "--add-opens=java.base/java.lang=ALL-UNNAMED") \
    .getOrCreate()
    index_data = spark.sparkContext.textFile("/indexer/index/part*")
    
    def save_index_partition(partition_iterator):
        from cassandra.cluster import Cluster

        # cluster = Cluster(['127.0.0.1'])
        cluster = Cluster(['cassandra-server'])

        session = cluster.connect('search_engine')
        
        session.default_timeout = 60 

        prep_index = session.prepare("INSERT INTO inverted_index (word, postings) VALUES (?, ?)")
        prep_vocab = session.prepare("INSERT INTO vocabulary (word, doc_count) VALUES (?, ?)")
        
        for line in partition_iterator:
            parts = line.split('\t')
            if len(parts) == 2:
                word, postings = parts[0], parts[1]
                doc_count = len(postings.split('|'))
                
                try:
                    session.execute(prep_index, (word, postings))
                    session.execute(prep_vocab, (word, doc_count))
                except Exception as e:
                    print(f"Skipping a line due to error: {e}")
                    
        cluster.shutdown()

    index_data.foreachPartition(save_index_partition)
    
   
    stats_data  = spark.sparkContext.textFile("/indexer/stats/part*")
    
    def save_stats_partition(partition_iterator):
        from cassandra.cluster import Cluster
        # cluster = Cluster(['127.0.0.1'])
        cluster = Cluster(['cassandra-server'])
        session = cluster.connect('search_engine')
        session.default_timeout = 60
        
        prep_stats = session.prepare("INSERT INTO doc_stats (doc_id, doc_length) VALUES (?, ?)")
        
        for line in partition_iterator:
            parts = line.split('\t')
            if len(parts) == 2:
                doc_id, length = parts[0], int(parts[1])
                session.execute(prep_stats, (doc_id, length))
        cluster.shutdown()



    stats_data.foreachPartition(save_stats_partition)
    
    print("data moved to Cassandra")
    spark.stop()

if __name__ == "__main__":
    run()