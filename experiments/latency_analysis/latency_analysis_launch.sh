#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/../.."

PARTITION_COUNTS=(3 6 9 12)
LAMBDA_RATES=(10 50 100 200 500 800 1000)
NB_VALUES=(1 5 10 15 20 25 30 40 50)
PAYLOAD_SIZE_BYTES=1000
WARMUP_DURATION=60
EXPERIMENT_DURATION=60 
CONSUMER_WAIT=30
N_RUNS=5

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
echo "Latency Analysis Experiment"
echo "Configuration:"
echo "  - Partitions to test: ${PARTITION_COUNTS[*]}"
echo "  - Lambda rates: ${LAMBDA_RATES[*]} msg/s"
echo "  - Batch sizes (Nb): ${NB_VALUES[*]}"
echo "  - Payload size: ${PAYLOAD_SIZE_BYTES} bytes"
echo "  - Experiment duration: ${EXPERIMENT_DURATION}s"
echo "  - Repetitions per config: ${N_RUNS}"
echo "  - Send mode: poisson"
echo "=========================================="

echo ""
echo ">> Building Docker images..."
docker compose build --quiet broker_1 broker_2 broker_3 producer_1 consumer_1_g1 consumer_2_g1 network_emulator kafka_ui > /dev/null 2>&1
echo ">> Build completed!"


for PARTITION_COUNT in "${PARTITION_COUNTS[@]}"; do

    TOPIC_NAME="test_latency_${PARTITION_COUNT}_partitions"
    echo ""
    echo "=========================================="
    echo "Testing with Partition Count: ${PARTITION_COUNT}"
    echo "Topic: ${TOPIC_NAME}"
    echo "=========================================="

    for LAMBDA in "${LAMBDA_RATES[@]}"; do

        echo ""
        echo ">> Lambda: ${LAMBDA} msg/s"

        for NB in "${NB_VALUES[@]}"; do
            echo ""
            echo "  >> Nb: ${NB} msg/batch"

            for RUN in $(seq 1 "${N_RUNS}"); do
                echo ""
                echo "  >> Run ${RUN}/${N_RUNS}"

                RUN_ID="lam${LAMBDA}_nb${NB}_part${PARTITION_COUNT}_r${RUN}"

                export TOPIC_NAME="${TOPIC_NAME}"
                export PARTITION_COUNT="${PARTITION_COUNT}"
                export RUN_ID="${RUN_ID}"
                export PAYLOAD_SIZE_BYTES="${PAYLOAD_SIZE_BYTES}"
                export NB_MESSAGES_PER_BATCH="${NB}"
                export TARGET_MESSAGES=999999999
                export EXPERIMENT_DURATION="${EXPERIMENT_DURATION}"
                export SEND_MODE="poisson"
                export LAMBDA_RATE="${LAMBDA}"

                docker compose down -v --remove-orphans > /dev/null 2>&1 || true
                sleep 2

                echo "  Starting environment..."

                docker compose up -d network_emulator > /dev/null 2>&1
                sleep 3
                docker compose up -d broker_1 broker_2 broker_3 kafka_ui > /dev/null 2>&1

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

                docker compose up -d consumer_1_g1 consumer_2_g1 > /dev/null 2>&1
                sleep 10

                until docker logs consumer_1_g1 2>&1 | grep -q "Subscribed to topic" && \
                    docker logs consumer_2_g1 2>&1 | grep -q "Subscribed to topic"; do
                    sleep 2
                done


                sleep 15

                docker compose up -d producer_1 > /dev/null 2>&1

                echo "  Running Experiment..."
                echo ""
                progress_bar ${WARMUP_DURATION} "Warmup phase (producers initializing)"
                progress_bar ${EXPERIMENT_DURATION} "Production phase (active message flow)"

                echo "  Waiting for consumer to finish processing..."
                until ls ./data/consumer_1_g1_${RUN_ID}_messages_recap.jsonl >/dev/null 2>&1 && \
                    ls ./data/consumer_2_g1_${RUN_ID}_messages_recap.jsonl >/dev/null 2>&1; do
                    sleep 5
                done
                sleep 5
                
                echo "  Generating latency report..."
                python experiments/latency_analysis/generate_report_latency.py --run_id "$RUN_ID"

                sleep 20

                echo "  Stopping environment..."
                docker compose down -v --remove-orphans > /dev/null 2>&1
                rm -f ./data/*_messages_recap.jsonl
                rm -f ./data/stats/*_stats.json
                echo ""
                echo "    ✓ Run ${RUN}/${N_RUNS} for lam=${LAMBDA} nb=${NB} part=${PARTITION_COUNT} completed!"
                sleep 2
            done

            echo ""
            echo "  ✓ All ${N_RUNS} runs for Nb=${NB} completed!"
        done

        echo ""
        echo "✓ All Nb values for lambda=${LAMBDA} completed!"
    done

    echo ""
    echo "✓ All lambda values for ${PARTITION_COUNT} partitions completed!"
done

echo ""
echo "=========================================="
echo "All experiments completed!"
echo "=========================================="
