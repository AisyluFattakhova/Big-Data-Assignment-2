import os
import sys
from pyspark.sql import SparkSession

os.system(f"{sys.executable} -m pip install pathvalidate cassandra-driver")
try:
    from pathvalidate import sanitize_filename
except ImportError:
    os.system("pip3 install pathvalidate")
    from pathvalidate import sanitize_filename

spark = SparkSession.builder \
    .appName('data preparation') \
    .config("spark.sql.parquet.enableVectorizedReader", "true") \
    .getOrCreate()

DATA_DIR = "/app/data"
PARQUET_FILE = "/app/a.parquet"

# Ensure the local data directory exists
os.makedirs(DATA_DIR, exist_ok=True)

# if Parquet exists, extract it into the app/data folder
if os.path.exists(PARQUET_FILE):
    print(f"Parquet file found at {PARQUET_FILE}. Extracting sample...")
    df = spark.read.parquet("file://" + PARQUET_FILE)
    
    n = 100
    total_count = df.count()
    sample_size = min(n, total_count)
    
    # Take a sample and collect to driver to write local files
    df_sample = df.select(['id', 'title', 'text']).limit(sample_size)
    
    for row in df_sample.collect():
        # Sanitize filename (ID_Title.txt)
        clean_title = sanitize_filename(str(row['id']) + "_" + row['title']).replace(" ", "_")
        filename = os.path.join(DATA_DIR, clean_title + ".txt")
        
        # Write local file if it doesn't already exist
        if not os.path.exists(filename):
            with open(filename, "w", encoding="utf-8") as f:
                f.write(row['text'])
else:
    print("No Parquet file found. Proceeding with existing files in /app/data...")

# Move all .txt files from local app/data/ to HDFS /data/
print("Uploading all local text files to HDFS /data...")
os.system("hdfs dfs -mkdir -p /data")
# -f forces overwrite to ensure HDFS has the latest versions of the files
os.system(f"hdfs dfs -put -f {DATA_DIR}/*.txt /data/")

# Create the MapReduce-ready input at /input/data
print("Creating the final MapReduce input at /input/data...")
# Read all text files from HDFS
raw_rdd = spark.sparkContext.wholeTextFiles("/data/*.txt")

def format_line(item):
    path, content = item
    filename = path.split("/")[-1].replace(".txt", "")
    
    if "_" in filename:
        parts = filename.split("_", 1)
        doc_id = parts[0]
        doc_title = parts[1]
    else:
        doc_id = "0"
        doc_title = filename

    clean_text = content.replace("\n", " ").replace("\r", " ").replace("\t", " ")
    
    return f"{doc_id}\t{doc_title}\t{clean_text}"

os.system("hdfs dfs -rm -r /input/data 2>/dev/null")

formatted_rdd = raw_rdd.map(format_line)
formatted_rdd.coalesce(1).saveAsTextFile("/input/data")

print("Finished data preparation.")
spark.stop()