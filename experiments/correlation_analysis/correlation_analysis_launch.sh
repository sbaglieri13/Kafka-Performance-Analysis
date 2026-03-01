#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/../.."

PARTITION_COUNTS=(3 6 9 12)
PAYLOAD_SIZE_BYTES=1000
BATCH_SIZES=(10 30 50 70 100 120 150 170 190 210 230 250 280 300)
WARMUP_DURATION=60
EXPERIMENT_DURATION=60 
CONSUMER_WAIT=30
N_RUNS=5 # number of repetitions per configuration

progress_bar() {
    local duration=$1
    local phase_name=$2
    local bar_length=50
    local elapsed=0
    
    echo "${phase_name} (${duration}s)"
    
    while [ $elapsed -lt $duration ]; do
        sleep 1
        elapsed=$((elapsed + 1))
        local remaining=$((duration - elapsed))
        local percentage=$((elapsed * 100 / duration))
        local filled=$((percentage * bar_length / 100))
        local empty=$((bar_length - filled))
        
        local bar=$(printf "%${filled}s" | tr ' ' '#')
        bar="${bar}$(printf "%${empty}s")"
        
        printf "\r [%-${bar_length}s] %3d%% - %ds remaining " "$bar" "$percentage" "$remaining"
    done
    
    local full_bar=$(printf "%${bar_length}s" | tr ' ' '#')
    printf "\r [%-${bar_length}s] 100%% - Completed!    \n" "$full_bar"
    echo ""
}


echo "=========================================="
echo "Correlation Analysis Experiment"
echo "Configuration:"
echo "  - Partition to test: ${PARTITION_COUNTS[*]}"
echo "  - Batch sizes: ${BATCH_SIZES[*]}"
echo "  - Payload size: ${PAYLOAD_SIZE_BYTES} bytes"
echo "  - Experiment duration: ${EXPERIMENT_DURATION}s"
echo "  - Repetitions per config: ${N_RUNS}"
echo "  - Send mode: continuous (heavy load)"
echo "=========================================="

# Build Docker images
echo ""
echo ">> Building Docker images..."
docker compose build --quiet broker_1 broker_2 broker_3 producer_1 producer_2 consumer_1_g1 consumer_2_g1 consumer_1_g2 network_emulator kafka_ui > /dev/null 2>&1
echo ">> Build completed!"

# Loop through partition counts and batch sizes
for PARTITION_COUNT in "${PARTITION_COUNTS[@]}"; do

    TOPIC_NAME="test_${PARTITION_COUNT}_partitions"
    echo ""
    echo "=========================================="
    echo "Testing with Partition Count: ${PARTITION_COUNT}"
    echo "Topic: ${TOPIC_NAME}"
    echo "=========================================="

    for BATCH_SIZE in "${BATCH_SIZES[@]}"; do

        echo ""
        echo ">> Batch size: ${BATCH_SIZE}"

        for RUN in $(seq 1 "${N_RUNS}"); do
            echo ""
            echo "  >> Run ${RUN}/${N_RUNS}"

            RUN_ID="batch_${BATCH_SIZE}_part_${PARTITION_COUNT}_run${RUN}"

            export TOPIC_NAME="${TOPIC_NAME}"
            export PARTITION_COUNT="${PARTITION_COUNT}"
            export RUN_ID="${RUN_ID}"
            export PAYLOAD_SIZE_BYTES="${PAYLOAD_SIZE_BYTES}"
            export NB_MESSAGES_PER_BATCH="${BATCH_SIZE}"
            export TARGET_MESSAGES=999999999 # high value to ensure producers keep sending for the entire duration
            export EXPERIMENT_DURATION="${EXPERIMENT_DURATION}"
            export SEND_MODE="continuous" # heavy load, no rate limit
            export LAMBDA_RATE=0 # not used in continuous mode

            docker compose down -v --remove-orphans > /dev/null 2>&1 || true
            sleep 2

            echo "  Starting environment..."

            docker compose up -d network_emulator > /dev/null 2>&1
            sleep 3
            docker compose up -d broker_1 broker_2 broker_3 kafka_ui > /dev/null 2>&1

            # Wait for Kafka brokers to be ready
            until docker exec broker_1 bash -c "nc -z localhost 29092" >/dev/null 2>&1; do
                sleep 10
            done

            if [[ -n "$TOPIC_NAME" && -n "$PARTITION_COUNT" ]]; then
                docker exec broker_1 kafka-topics \
                    --bootstrap-server broker_1:29092 \
                    --create \
                    --topic "$TOPIC_NAME" \
                    --partitions "$PARTITION_COUNT" \
                    --replication-factor 3 \
                    2>&1 | grep -q "Created topic" && echo "   ✓ Created topic: $TOPIC_NAME" || echo "   X Topic already exists: $TOPIC_NAME"
            else
                echo "   !!! Warning: TOPIC_NAME or PARTITION_COUNT not set, skipping topic creation !!!"
            fi

            docker compose up -d consumer_1_g1 consumer_2_g1 consumer_1_g2 > /dev/null 2>&1
            sleep 10

            until docker logs consumer_1_g1 2>&1 | grep -q "Subscribed to topic" && \
                  docker logs consumer_2_g1 2>&1 | grep -q "Subscribed to topic" && \
                  docker logs consumer_1_g2 2>&1 | grep -q "Subscribed to topic"; do
                sleep 2
            done

            sleep 15

            docker compose up -d producer_1 > /dev/null 2>&1

            echo "  Running Experiment..."
            echo ""
            progress_bar ${WARMUP_DURATION} "Warmup phase (producers initializing)"
            progress_bar ${EXPERIMENT_DURATION} "Production phase (active message flow)"
            
            echo "  Waiting for consumers to finish processing..."
            sleep ${CONSUMER_WAIT}

            # Update stats with JMX and stop environment
            echo "  Updating stats with JMX..."
            python scripts/update_stats_with_jmx.py "$RUN_ID" "$TOPIC_NAME" > /dev/null 2>&1
            echo "  Stopping environment..."
            docker compose down -v --remove-orphans > /dev/null 2>&1
            rm -f ./data/*_messages_recap.jsonl

            echo ""
            echo "    ✓ Run ${RUN}/${N_RUNS} for batch=${BATCH_SIZE} part=${PARTITION_COUNT} completed!"
            sleep 2
        done

        echo ""
        echo "  ✓ All ${N_RUNS} runs for batch size ${BATCH_SIZE} completed!"
    done

    echo ""
    echo "✓ All batch sizes for ${PARTITION_COUNT} partitions completed!"
done

echo ""
echo "=========================================="
echo "All experiments completed!"
echo "=========================================="

