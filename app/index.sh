#!/bin/bash
set -euo pipefail

cd /app

bash create_index.sh
bash store_index.sh
