#!/usr/bin/env bash
# One-command setup: dependencies + Kafka, then starts the producer.
set -euo pipefail
cd "$(dirname "$0")"

docker compose up -d --wait

echo "Kafka is up on localhost:9092. Replaying the GitHub dataset..."
DATASET_PATH="${DATASET_PATH:-C:/Users/ilios/Downloads/2026-10-15-15.json.gz}"
exec bash ./run.sh produce_messages.py --input "$DATASET_PATH" "$@"
