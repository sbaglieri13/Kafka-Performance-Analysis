#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

echo ">> Cleanup Docker environment..."

echo ">> Stopping and removing containers..."
# Stop and remove all containers, networks, and volumes defined in the Docker Compose file
docker compose down --volumes > /dev/null 2>&1

echo ">> Cleaning up networks..."
# Remove all unused Docker networks
docker network prune -f > /dev/null 2>&1

echo ">> Cleaning data files..."
# Remove all JSONL files from the data directory
rm -f ./data/*.jsonl

echo ">> Docker environment cleaned up successfully!"
