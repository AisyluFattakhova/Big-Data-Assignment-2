# big-data-assignment2

# How to run
## Step 1: Install prerequisites
- Docker
- Docker compose
## Step 2: How to Run

The repository is fully containerized. To execute the entire pipeline:
1.  _(Optional)_ Place the `a.parquet` file into the `app/` folder.
- If provided, the system will extract and process up to 1000 documents automatically.
- If not provided, the system will still work using the pre-existing `.txt` files located in the `app/data/` directory.
1.  Run:
    ``` bash
       docker compose up --build
    ```
2.  The system will automatically start Hadoop, index 1000 documents, load Cassandra, and run a sample query for "history".
3.  To run a custom query manually:
    ```bash
    docker exec -it cluster-master bash /app/search.sh "your_query"
    ```
