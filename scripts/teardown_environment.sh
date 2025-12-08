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

read -p ">> Delete producer and consumer data files (*.jsonl)? (y/n): " -n 1 -r
echo ""
    
if [[ $REPLY =~ ^[Yy]$ ]]; then
    echo ">> Cleaning producer and consumer data files..."
    rm -f ./data/*_messages.jsonl
    echo "   ✓ JSONL files deleted"
else
    echo "   X Producer and consumer data files skipped"
fi

echo ""
read -p ">> Delete analysis reports (latency_report.json)? (y/n): " -n 1 -r
echo ""

if [[ $REPLY =~ ^[Yy]$ ]]; then
    echo ">> Cleaning analysis reports..."
    rm -f ./data/latency_report*.json
    echo "   ✓ Report files deleted"
else
    echo "   X Analysis reports skipped"
fi

echo "================================================="
echo ">> Docker environment cleaned up successfully!"
echo "================================================="
