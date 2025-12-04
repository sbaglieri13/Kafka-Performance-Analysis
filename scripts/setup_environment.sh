#!/usr/bin/env bash
set -euo pipefail

# Vai nella root del progetto (cartella sopra scripts)
cd "$(dirname "$0")/.."

echo ">> Starting Kafka cluster..."
echo ">> Starting brokers and UI..."
docker compose up --build -d broker_1 broker_2 broker_3 kafka_ui

echo ">> Waiting for brokers to be ready..."

# Attendo che broker_1 risponda sulla porta interna 29092
until docker exec broker_1 bash -c "nc -z localhost 29092" >/dev/null 2>&1; do
  sleep 10
  echo ">> Brokers not ready yet, trying again..."
done

echo ">> Brokers ready, creating topics..."

docker exec -it broker_1 kafka-topics \
  --bootstrap-server broker_1:29092 \
  --create \
  --topic test_partitions_3 \
  --partitions 3 --replication-factor 3 \
  2>&1 | grep -v "could collide" # scarta il messaggio di errore del tipo "WARNING: Due to limitations in metric names, topics with a period ('.') or underscore ('_') could collide. To avoid issues it is best to use either, but not both."

docker exec -it broker_1 kafka-topics \
  --bootstrap-server broker_1:29092 \
  --create \
  --topic test_partitions_6 \
  --partitions 6 --replication-factor 3 \
  2>&1 | grep -v "could collide" # scarta il messaggio di errore del tipo "WARNING: Due to limitations in metric names, topics with a period ('.') or underscore ('_') could collide. To avoid issues it is best to use either, but not both."

docker exec -it broker_1 kafka-topics \
  --bootstrap-server broker_1:29092 \
  --create \
  --topic test_partitions_9 \
  --partitions 9 --replication-factor 3 \
  2>&1 | grep -v "could collide" # scarta il messaggio di errore del tipo "WARNING: Due to limitations in metric names, topics with a period ('.') or underscore ('_') could collide. To avoid issues it is best to use either, but not both."


docker exec -it broker_1 kafka-topics \
  --bootstrap-server broker_1:29092 \
  --create \
  --topic test_partitions_12 \
  --partitions 12 --replication-factor 3 \
  2>&1 | grep -v "could collide" # scarta il messaggio di errore del tipo "WARNING: Due to limitations in metric names, topics with a period ('.') or underscore ('_') could collide. To avoid issues it is best to use either, but not both."


echo ">> Topics created! Here are the current topics:"
docker exec broker_1 kafka-topics --bootstrap-server broker_1:29092 --list

echo ">> Starting producers and consumers"
docker compose up --build -d producer_1 producer_2 consumer_1_g1 consumer_2_g1 consumer_1_g2


echo ">> Environment setup completed successfully!"