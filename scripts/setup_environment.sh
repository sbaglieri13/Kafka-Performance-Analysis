#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

read -p ">> Do you want to rebuild Docker images? (y/n): " -n 1 -r
echo ""

if [[ $REPLY =~ ^[Yy]$ ]]; then
    read -p ">> Show build logs? (y/n): " -n 1 -r
    echo ""
    echo ">> Starting build process..."
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        # Build con log
        echo ">> Building images (with logs)..."
        docker compose build network_emulator broker_1 broker_2 broker_3 producer_1 producer_2 consumer_1_g1 consumer_2_g1 consumer_1_g2 message_generator kafka_ui
    else
        # Build senza log (quiet)
        echo ">> Building images (quiet mode)..."
        docker compose build --quiet network_emulator broker_1 broker_2 broker_3 producer_1 producer_2 consumer_1_g1 consumer_2_g1 consumer_1_g2 message_generator kafka_ui
    fi
    echo ">> Build completed!"
    echo ""
else
    echo ">> Skipping build, using existing images..."
    echo ""
fi

echo ">> Starting Kafka cluster..."

echo ">> Starting network emulator..."
docker compose up -d network_emulator > /dev/null 2>&1
sleep 3
echo ">> Network emulator started!"

echo ">> Starting brokers and UI..."
docker compose up -d broker_1 broker_2 broker_3 kafka_ui > /dev/null 2>&1

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
  2>&1 | grep -q "Created topic" && echo "   ✓ Created topic: test_partitions_3" || echo "   ℹ Topic already exists: test_partitions_3"

docker exec -it broker_1 kafka-topics \
  --bootstrap-server broker_1:29092 \
  --create \
  --topic test_partitions_6 \
  --partitions 6 --replication-factor 3 \
  2>&1 | grep -q "Created topic" && echo "   ✓ Created topic: test_partitions_6" || echo "   ℹ Topic already exists: test_partitions_3"

docker exec -it broker_1 kafka-topics \
  --bootstrap-server broker_1:29092 \
  --create \
  --topic test_partitions_9 \
  --partitions 9 --replication-factor 3 \
  2>&1 | grep -q "Created topic" && echo "   ✓ Created topic: test_partitions_9" || echo "   ℹ Topic already exists: test_partitions_3"

docker exec -it broker_1 kafka-topics \
  --bootstrap-server broker_1:29092 \
  --create \
  --topic test_partitions_12 \
  --partitions 12 --replication-factor 3 \
  2>&1 | grep -q "Created topic" && echo "   ✓ Created topic: test_partitions_12" || echo "   ℹ Topic already exists: test_partitions_3"

echo ">> Starting message generator service..."
docker compose up -d message_generator > /dev/null 2>&1

echo ">> Waiting for message generator to be ready..."
until curl -s http://localhost:8000/docs >/dev/null 2>&1; do
    sleep 1
    echo ">> Message generator not ready yet..."
done
echo ">> Message generator is ready!"


echo ">> Starting consumers..."
docker compose up -d consumer_1_g1 consumer_2_g1 consumer_1_g2 > /dev/null 2>&1

echo ">> Waiting for consumers to subscribe to topics..."
sleep 5

# Verifica che i consumer siano effettivamente connessi al broker
until docker logs consumer_1_g1 2>&1 | grep -q "Subscribed to topic" && \
      docker logs consumer_2_g1 2>&1 | grep -q "Subscribed to topic" && \
      docker logs consumer_1_g2 2>&1 | grep -q "Subscribed to topic"; do
    sleep 2
    echo ">> Consumers not ready yet, waiting..."
done

echo ">> All consumers subscribed and ready!"

echo ">> Starting producers..."
docker compose up -d producer_1 producer_2 > /dev/null 2>&1
echo ">> Producers started!"

echo ">> Environment setup completed successfully!"