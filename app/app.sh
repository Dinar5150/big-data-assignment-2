#!/bin/bash
set -euo pipefail

cd /app

echo "Starting ssh service"
service ssh restart

echo "Starting Hadoop services"
bash start-services.sh

echo "Creating Python virtual environment"
rm -rf .venv .venv.tar.gz
python3 -m venv .venv
source .venv/bin/activate

echo "Installing Python packages"
pip install --no-cache-dir -r requirements.txt

echo "Packing virtual environment for Spark on YARN"
venv-pack -f -o .venv.tar.gz

echo "Preparing data"
bash prepare_data.sh

echo "Creating and storing the index"
bash index.sh

echo "Running demo searches"
bash search.sh "computer science"
bash search.sh "history war"
bash search.sh "music art"
