#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

echo "================================================="
echo "Cleanup Docker environment"
echo "================================================="

echo ">> Stopping and removing containers..."
# Stop and remove all containers, networks, and volumes defined in the Docker Compose file
docker compose down --volumes > /dev/null 2>&1

echo ">> Cleaning up networks..."
# Remove all unused Docker networks
docker network prune -f > /dev/null 2>&1

read -p ">> Delete producer and consumer data files (*_messages_recap.jsonl)? (y/n): " -n 1 -r
echo ""
    
if [[ $REPLY =~ ^[Yy]$ ]]; then
    echo ">> Cleaning producer and consumer data files..."
    rm -f ./data/*_messages_recap.jsonl
    echo "   ✓ files deleted"
else
    echo "   X files skipped"
fi

echo ""
read -p ">> Delete producer stats files (*_stats.json)? (y/n): " -n 1 -r
echo ""

if [[ $REPLY =~ ^[Yy]$ ]]; then
    echo ">> Cleaning producer stats files..."
    rm -f ./data/stats/*_stats.json
    echo "   ✓ files deleted"
else
    echo "   X files skipped"
fi

echo "================================================="
echo ">> Docker environment cleaned up successfully!"
echo "================================================="
