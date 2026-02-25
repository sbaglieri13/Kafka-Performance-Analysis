#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

# Load modules
source "$(dirname "$0")/modules/network_config.sh"
source "$(dirname "$0")/modules/experiment_config.sh"
source "$(dirname "$0")/modules/docker_build.sh"
source "$(dirname "$0")/modules/docker_deploy.sh"
source "$(dirname "$0")/modules/experiment_timer.sh"

echo "=========================================="
echo "Select experiment mode:"
echo "1) Time-based (by duration in seconds)"
echo "2) Message-based (by target number of messages)"
echo "=========================================="
read -p "Enter choice [1-2]: " -n 1 -r EXPERIMENT_MODE
echo ""

if [[ "$EXPERIMENT_MODE" != "1" && "$EXPERIMENT_MODE" != "2" ]]; then
    echo "Invalid choice! Please enter 1 or 2."
    exit 1
fi

export EXPERIMENT_MODE

# ============================================================
# MAIN EXECUTION FLOW
# ============================================================

# Step 1: Setup network emulation
setup_network_emulation

# Step 2: Configure experiment duration
configure_experiment_duration

# Step 3: Configure experiment parameters
configure_experiment_parameters

# Step 4: Build Docker images
build_docker_images

# Step 5: Deploy Docker environment
deploy_docker_environment

# Step 6: Run experiment 
if [[ "$EXPERIMENT_MODE" == "1" ]]; then
    # Time-based
    TOTAL_EXPERIMENT_TIME=$((EXPERIMENT_DURATION)) 
    experiment_timer $TOTAL_EXPERIMENT_TIME
else
    # Message-based
    echo ""
    echo ">> Processing ${TARGET_MESSAGES} messages"

    # Wait until all producers have finished sending messages
    TIMEOUT=1800  # 30 minuti max
    ELAPSED=0
    
    while [[ $ELAPSED -lt $TIMEOUT ]]; do
        # Check if all producers have finished (stats files exist)
        
        if ls ./data/stats/*_stats.json 1> /dev/null 2>&1; then
            echo ""
            echo ">> Waiting consumers..."
            sleep 60  # Attende che i consumer completino l'elaborazione
            echo "✓ Done!"
            break
        fi
        
        echo -n "."
        sleep 5
        ELAPSED=$((ELAPSED + 5))
    done
    
    if [[ $ELAPSED -ge $TIMEOUT ]]; then
        echo ""
        echo "X Timeout reached! Check if producers are still running."
        exit 1
    fi
fi

python scripts/update_stats_with_jmx.py "$RUN_ID" "$TOPIC_NAME"