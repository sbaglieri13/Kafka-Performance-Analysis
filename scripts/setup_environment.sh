#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

# Load modules
source "$(dirname "$0")/modules/network_config.sh"
source "$(dirname "$0")/modules/experiment_config.sh"
source "$(dirname "$0")/modules/docker_build.sh"
source "$(dirname "$0")/modules/docker_deploy.sh"
source "$(dirname "$0")/modules/experiment_timer.sh"

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

# Step 6: Run experiment timer
TOTAL_EXPERIMENT_TIME=$((EXPERIMENT_DURATION + 60))  # +60 for producer warmup
experiment_timer $TOTAL_EXPERIMENT_TIME
