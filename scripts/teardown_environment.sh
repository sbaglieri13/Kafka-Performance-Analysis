#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

echo ">> Cleanup Docker environment..."

echo ">> Stopping and removing containers..."
docker compose down --volumes > /dev/null 2>&1

echo ">> Cleaning up networks..."
docker network prune -f > /dev/null 2>&1

echo ">> Docker environment cleaned up successfully!"
