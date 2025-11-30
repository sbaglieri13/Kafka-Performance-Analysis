#!/usr/bin/env bash
set -euo pipefail

echo "=== Starting experiments ==="

PARTITIONS=("3" "6" "9" "12")
PAYLOADS=("200" "300" "500" "700" "800" "1000" "1200" "1500" "1800" "2000" "2300" "2600" "3000" "3300" "3600" "4000" "5000" "6000")

# valori di Nb (numero di messaggi per batch)
MESSAGES_PER_BATCH_LIST=("1" "10" "20" "50")
RUNTIME=60           # durata in secondi di ogni run
LINGER_MS=1          # 1 ms
BATCH_SIZE_BYTES=1048576  # 1 MB

topic_for_partitions() {
  case "$1" in
    3)  echo "test-topic-3"  ;;
    6)  echo "test-topic-6"  ;;
    9)  echo "test-topic-9"  ;;
    12) echo "test-topic-12" ;;
    *)  echo "test-topic-$1" ;;
  esac
}

for p in "${PARTITIONS[@]}"; do
  TOPIC_NAME="$(topic_for_partitions "$p")"

  for s in "${PAYLOADS[@]}"; do
    for Nb in "${MESSAGES_PER_BATCH_LIST[@]}"; do
      echo "=== Run partitions=$p payload=$s Nb=$Nb topic=$TOPIC_NAME ==="

      # pulizia metriche del run precedente
      rm -f data/producer_metrics_* data/consumer_metrics_*

      # RUN_ID unico per il run (partitions + payload + Nb)
      RUN_ID="p${p}_s${s}_nb${Nb}"

      # avvio con override delle variabili d'ambiente
      PARTITION_COUNT="$p" PAYLOAD_SIZE_BYTES="$s" TOPIC_NAME="$TOPIC_NAME" \
      RUN_ID="$RUN_ID" \
      MESSAGES_PER_BATCH="$Nb" LINGER_MS="$LINGER_MS" BATCH_SIZE_BYTES="$BATCH_SIZE_BYTES" \
      docker compose up --build -d \
        producer1 producer2 \
        consumer-g1-c1 consumer-g1-c2 consumer-g2-c1

      sleep "$RUNTIME"

      # stop dei client
      docker compose stop \
        producer1 producer2 \
        consumer-g1-c1 consumer-g1-c2 consumer-g2-c1

      # analisi e append al summary
      python ".\analyzer\analyzer.py"
    done
  done
done

docker compose stop kafka1 kafka2 kafka3 kafka-ui

echo "=== All experiments completed ==="
