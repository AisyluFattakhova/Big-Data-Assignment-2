import sys
import math
from pyspark.sql import SparkSession

def main():
    if len(sys.argv) < 2:
        print("Usage: spark-submit query.py <query_word>")
        return

    query_terms = [word.lower() for word in sys.argv[1:]]
    
    # Initialize Spark
    # spark = SparkSession.builder.appName("BM25Search").getOrCreate()
    spark = (
    SparkSession.builder
    .appName("BM25Search")
    .config("spark.yarn.jars", "hdfs:///spark/jars/*")
    .getOrCreate()
)
    spark.sparkContext.setLogLevel("ERROR")
    sc = spark.sparkContext

    def get_data_from_cassandra(terms):
        from cassandra.cluster import Cluster
        # cluster = Cluster(['127.0.0.1'])
        cluster = Cluster(['cassandra-server'])

        session = cluster.connect('search_engine')
        
        N = session.execute("SELECT count(*) FROM doc_stats").one()[0]
        avgdl = session.execute("SELECT avg(doc_length) FROM doc_stats").one()[0]
        
        idf_map = {}
        for term in terms:
            row = session.execute("SELECT doc_count FROM vocabulary WHERE word=%s", (term,)).one()
            if row:
                nq = row.doc_count
                idf_map[term] = math.log(1 + (N - nq + 0.5) / (nq + 0.5))
        
        postings_data = []
        for term in terms:
            row = session.execute("SELECT postings FROM inverted_index WHERE word=%s", (term,)).one()
            if row and row.postings:
                # SPLIT BY PIPE (|) - Matches your new Reducer
                for p in row.postings.split('|'):
                    if not p or ':' not in p:
                        continue
                    
                    # USE RSPLIT to handle colons in titles safely
                    parts = p.rsplit(':', 1)
                    if len(parts) == 2:
                        doc_id = parts[0]
                        tf = int(parts[1])
                        postings_data.append((doc_id, (term, tf)))

        unique_docs = list(set([p[0] for p in postings_data]))
        doc_lengths = {}
        for d_id in unique_docs:
            row = session.execute("SELECT doc_length FROM doc_stats WHERE doc_id=%s", (d_id,)).one()
            if row:
                doc_lengths[d_id] = row.doc_length
        
        cluster.shutdown()
        return N, avgdl, idf_map, postings_data, doc_lengths

    # Execute the retrieval
    N, avgdl, idf_map, postings_data, doc_lengths = get_data_from_cassandra(query_terms)

    if not postings_data:
        print(f"\nNo results found for: {' '.join(query_terms)}\n")
        spark.stop()
        return

  
    postings_rdd = sc.parallelize(postings_data)
    b_idf = sc.broadcast(idf_map)
    b_lengths = sc.broadcast(doc_lengths)
    b_avgdl = sc.broadcast(avgdl)

    def calculate_bm25(item):
        doc_id, (word, tf) = item
        idf = b_idf.value.get(word, 0)
        d_len = b_lengths.value.get(doc_id, b_avgdl.value)
        k1, b = 1.5, 0.75
        numerator = tf * (k1 + 1)
        denominator = tf + k1 * (1 - b + b * (d_len / b_avgdl.value))
        return (doc_id, idf * (numerator / denominator))

    results = postings_rdd.map(calculate_bm25) \
                          .reduceByKey(lambda a, b: a + b) \
                          .takeOrdered(10, key=lambda x: -x[1])

    # Print Results
    print(f"\n{'='*85}")
    print(f"BM25 TOP 10 RESULTS FOR: {' '.join(query_terms)}")
    print(f"{'='*85}")
    print(f"{'Rank':<5} | {'Doc ID':<10} | {'Document Title':<50} | {'Score'}")
    print(f"{'-'*85}")

    for i, (full_doc_info, score) in enumerate(results, 1):
        if "_" in full_doc_info:
            doc_id, doc_title = full_doc_info.split("_", 1)
        else:
            doc_id = "N/A"
            doc_title = full_doc_info

        # Clean the title for display
        clean_title = doc_title.replace("_", " ")
        if len(clean_title) > 47:
            clean_title = clean_title[:47] + "..."

        print(f"{i:<5} | {doc_id:<10} | {clean_title:<50} | {score:.4f}")
    
    print(f"{'='*85}\n")

    spark.stop()

if __name__ == "__main__":
    main()