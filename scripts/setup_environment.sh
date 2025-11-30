#!/usr/bin/env bash
set -euo pipefail

# 1) Stop e rimozione di TUTTI i container + volumi del compose
echo ">> Stopping and removing containers + volumes..."
docker compose down -v kafka1 kafka2 kafka3 kafka-ui \
  producer1 producer2 \
  consumer-g1-c1 consumer-g1-c2 consumer-g2-c1

# 2) Avvio solo dei broker (e della UI) in background
echo ">> Starting Kafka cluster..."
docker compose up --build -d kafka1 kafka2 kafka3 kafka-ui

echo ">> Waiting 20s for brokers to be ready..."
sleep 20

# 3) Creazione topic di test
echo ">> Creating topics..."

docker exec -it kafka1 kafka-topics \
  --bootstrap-server kafka1:29092 \
  --create --if-not-exists \
  --topic test-topic-3 \
  --partitions 3 --replication-factor 3

docker exec -it kafka1 kafka-topics \
  --bootstrap-server kafka1:29092 \
  --create --if-not-exists \
  --topic test-topic-6 \
  --partitions 6 --replication-factor 3

docker exec -it kafka1 kafka-topics \
  --bootstrap-server kafka1:29092 \
  --create --if-not-exists \
  --topic test-topic-9 \
  --partitions 9 --replication-factor 3

docker exec -it kafka1 kafka-topics \
  --bootstrap-server kafka1:29092 \
  --create --if-not-exists \
  --topic test-topic-12 \
  --partitions 12 --replication-factor 3

echo ">> Topics created. Cluster ready for experiments."
