#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

echo ">> Stop and cleanup Docker environment..."
docker-compose down --volumes

echo ">> Docker environment cleaned up successfully"
