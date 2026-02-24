#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/../.."

source scripts/modules/experiment_timer.sh 

PARTITION_COUNTS=(3 6 9 12)
PAYLOAD_SIZE_BYTES=1000
BATCH_SIZES=(10 30 50 70 100 120 150 170 190 210 230 250 280 300)
EXPERIMENT_DURATION=60

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
echo "  - Send mode: continuous (heavy load)"
echo "=========================================="

# Build delle immagini Docker
echo ""
echo ">> Building Docker images..."
docker compose build --quiet broker_1 broker_2 broker_3 producer_1 producer_2 consumer_1_g1 consumer_2_g1 consumer_1_g2 network_emulator kafka_ui > /dev/null 2>&1
echo ">> Build completed!"

# loop per ogni combinazione di PARTITION_COUNT e BATCH_SIZE
experiment_count=0
failed_experiments=0

for PARTITION_COUNT in "${PARTITION_COUNTS[@]}"; do

    TOPIC_NAME="test_${PARTITION_COUNT}_partitions"
    echo ""
    echo "=========================================="
    echo "Testing with Partition Count: ${PARTITION_COUNT}"
    echo "Topic: ${TOPIC_NAME}"
    echo "=========================================="
    
    for BATCH_SIZE in "${BATCH_SIZES[@]}"; do
        experiment_count=$((experiment_count + 1))

        echo ""
        echo ">> Batch size: ${BATCH_SIZE}"
        
        RUN_ID="batch_${BATCH_SIZE}_part_${PARTITION_COUNT}"
        
        export TOPIC_NAME="${TOPIC_NAME}"
        export PARTITION_COUNT="${PARTITION_COUNT}"
        export RUN_ID="${RUN_ID}"
        export PAYLOAD_SIZE_BYTES="${PAYLOAD_SIZE_BYTES}"
        export NB_MESSAGES_PER_BATCH="${BATCH_SIZE}"
        export TARGET_MESSAGES=999999999 # Numero elevato per modalità time-based
        export EXPERIMENT_DURATION="${EXPERIMENT_DURATION}"
        export SEND_MODE="continuous" # heavy load, senza rate limit
        export LAMBDA_RATE=0 # non usato in continuous mode
        
        docker compose down > /dev/null 2>&1 || true
        sleep 2

        echo "  Starting environment..."

        docker compose up -d network_emulator > /dev/null 2>&1
        sleep 3
        docker compose up -d broker_1 broker_2 broker_3 kafka_ui > /dev/null 2>&1
        
        # Attendo che broker_1 risponda sulla porta interna 29092
        until docker exec broker_1 bash -c "nc -z localhost 29092" >/dev/null 2>&1; do
        sleep 10
        done
        
        if [[ -n "$TOPIC_NAME" && -n "$PARTITION_COUNT" ]]; then
            docker exec -it broker_1 kafka-topics \
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
    
        experiment_timer ${EXPERIMENT_DURATION}

        # Aggiorna i file stats 
        echo "  Updating stats with JMX..."
        python scripts/update_stats_with_jmx.py "$RUN_ID" "$TOPIC_NAME" > /dev/null 2>&1
        echo "  Stopping environment..."
        docker compose down > /dev/null 2>&1
        rm -f ./data/*_messages_recap.jsonl
        echo ""
        echo "  ✓ Batch size ${BATCH_SIZE} completed!"
        sleep 2
    done
    
    echo ""
    echo "✓ All batch sizes for ${PARTITION_COUNT} partitions completed!"
done

echo ""
echo "=========================================="
echo "Experiments completed!"
echo "=========================================="

