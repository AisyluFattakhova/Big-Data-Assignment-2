from cassandra.cluster import Cluster

def run():
    
    
    # cluster = Cluster(['127.0.0.1'])
    cluster = Cluster(['cassandra-server'])

    
    session = cluster.connect()
    
    session.default_timeout = 60
    
    session.execute("DROP KEYSPACE IF EXISTS search_engine")
    
    session.execute("""
        
        CREATE KEYSPACE IF NOT EXISTS search_engine 
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
    """)
    
    session.set_keyspace('search_engine')
    print("keyspace created")
    # Word index
    session.execute("""
        CREATE TABLE IF NOT EXISTS inverted_index (
            word text PRIMARY KEY,
            postings text
        );
    """)
    
    # Document word counts for ranking
    session.execute("""
        CREATE TABLE IF NOT EXISTS doc_stats (
            doc_id text PRIMARY KEY,
            doc_length int
        );
    """)
    print("tables created")
    # Vocabulary with document counts for IDF
    session.execute("""
        CREATE TABLE IF NOT EXISTS vocabulary (
            word text PRIMARY KEY,
            doc_count int
        );
    """)
    
    print("vocabulary table created")
    print("schema created")
    cluster.shutdown()

if __name__ == "__main__":
    run()