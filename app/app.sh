#!/bin/bash
set -euo pipefail

cd /app

rm -rf .venv .venv.tar.gz

service ssh restart
bash start-services.sh

python3 -m venv .venv
source .venv/bin/activate
python -m pip install --no-cache-dir --upgrade pip setuptools wheel
python -m pip install --no-cache-dir --prefer-binary -r requirements.txt
venv-pack -f -o .venv.tar.gz

bash prepare_data.sh
bash index.sh
bash search.sh "this is a query!"
