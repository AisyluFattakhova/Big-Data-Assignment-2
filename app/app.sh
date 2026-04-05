#!/bin/bash
cd /app

bash /app/start-services.sh


apt-get update && apt-get install -y python3-pip
python3 -m pip install -r /app/requirements.txt

bash /app/prepare_data.sh
bash /app/index.sh


# Start the search
bash /app/search.sh "history"

tail -f /dev/null