#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/../.."

# Source dello script di deploy esistente
source ./scripts/modules/docker_deploy.sh

PARTITION_COUNTS=(3 6 9 12)
PAYLOAD_SIZE_BYTES=1000
TARGET_MESSAGES=50000
BATCH_SIZES=(1 2 3 4 5 6 7 8 9 10 12 13 15 16 18 20 21 23 24 26 29 30 32 35 37 40 41 45 49 50)
N_RUNS=10  # Numero di ripetizioni per ogni configurazione

STATS_DIR="./data/stats"

echo "=========================================="
echo "Network Bandwidth Evaluation Experiment"
echo "Configuration:"
echo "  - Partitions to test: ${PARTITION_COUNTS[*]}"
echo "  - Batch sizes: ${BATCH_SIZES[*]}"
echo "  - Runs per config: ${N_RUNS}"
echo "=========================================="

# Build delle immagini Docker
echo ""
echo ">> Building Docker images..."
docker compose build --quiet broker_1 broker_2 broker_3 producer_1 consumer_1_g1 consumer_2_g1 consumer_1_g2 network_emulator kafka_ui > /dev/null 2>&1
echo ">> Build completed!"

for PARTITION_COUNT in "${PARTITION_COUNTS[@]}"; do
    TOPIC_NAME="test_${PARTITION_COUNT}_partitions"

    echo ""
    echo "=========================================="
    echo "Testing with Partition Count: ${PARTITION_COUNT}"
    echo "Topic: ${TOPIC_NAME}"
    echo "Payload: ${PAYLOAD_SIZE_BYTES} bytes"
    echo "Target messages: ${TARGET_MESSAGES}"
    echo "Send mode: continuous (heavy load)"
    echo "=========================================="

    for BATCH_SIZE in "${BATCH_SIZES[@]}"; do
        echo ""
        echo ">> Batch size: ${BATCH_SIZE}"

        for RUN in $(seq 1 "${N_RUNS}"); do
            echo ""
            echo "  >> Run ${RUN}/${N_RUNS}"

            RUN_ID="batch_${BATCH_SIZE}_part_${PARTITION_COUNT}_run${RUN}"

            export TOPIC_NAME
            export PARTITION_COUNT
            export RUN_ID
            export PAYLOAD_SIZE_BYTES
            export NB_MESSAGES_PER_BATCH="${BATCH_SIZE}"
            export TARGET_MESSAGES
            export EXPERIMENT_DURATION=999999999  # Tempo infinito per modalità message-based
            export SEND_MODE=continuous # heavy load continuo, senza rate limit
            export LAMBDA_RATE=0  # non usato in continuous mode

            docker compose down -v --remove-orphans > /dev/null 2>&1 || true
            sleep 2

            echo "  Starting environment..."
            deploy_docker_environment > /dev/null 2>&1

            echo "  Waiting for stats files..."

            # Aspetta che i file stats.json vengano creati
            TIMEOUT=1800  # 30 minuti max
            ELAPSED=0

            while [[ $ELAPSED -lt $TIMEOUT ]]; do
                if [[ -f "${STATS_DIR}/producer_1_${RUN_ID}_stats.json" ]]; then
                    echo "  ✓ Stats files created!"
                    break
                fi

                sleep 5
                ELAPSED=$((ELAPSED + 5))

                # Progress ogni 15s
                if [[ $((ELAPSED % 15)) -eq 0 ]]; then
                    echo "    Still waiting... (${ELAPSED}s elapsed)"
                fi
            done

            # Se il timeout è scaduto, termina l'esperimento e passa al successivo
            if [[ $ELAPSED -ge $TIMEOUT ]]; then
                echo "    X Timeout! Stats files not created for run=${RUN} batch=${BATCH_SIZE} part=${PARTITION_COUNT}"
                docker compose down -v --remove-orphans > /dev/null 2>&1 || true
                continue  
            fi

            # Aggiorna i file stats 
            echo "  Updating stats with JMX..."
            python scripts/update_stats_with_jmx.py "$RUN_ID" "$TOPIC_NAME" > /dev/null 2>&1

            docker compose down -v --remove-orphans > /dev/null 2>&1
            # Pulisce i file di riepilogo dei messaggi
            rm -f ./data/*_messages_recap.jsonl

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
echo "Experiments completed!"
echo "=========================================="

