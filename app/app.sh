#!/bin/bash
set -euo pipefail

cd /app

service ssh restart
bash start-services.sh

python3 -m venv .venv
source .venv/bin/activate
python -m pip install --no-cache-dir --prefer-binary -r requirements.txt

bash prepare_data.sh
bash index.sh
bash search.sh "computer science"
bash search.sh "history"
