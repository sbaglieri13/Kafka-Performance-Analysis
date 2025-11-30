#!/bin/bash
set -euo pipefail

echo "=== Run script setup_environment.sh ==="
./Scripts/setup_environment.sh

echo "=== Run script experiments.sh ==="
./Scripts/run_experiments.sh

echo "=== All completed! ==="
