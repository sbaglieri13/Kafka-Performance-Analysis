#!/usr/bin/env bash

# ============================================================
# AVVIO DEI SERVIZI DOCKER 
# ============================================================
deploy_docker_environment() {
    echo "============================================="
    echo "Starting Docker Environment"
    echo "============================================="
    
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
    sleep 10
    
    # Verifica che i consumer siano effettivamente connessi al broker
    until docker logs consumer_1_g1 2>&1 | grep -q "Subscribed to topic" && \
          docker logs consumer_2_g1 2>&1 | grep -q "Subscribed to topic" && \
          docker logs consumer_1_g2 2>&1 | grep -q "Subscribed to topic"; do
        sleep 2
        echo ">> Consumers not ready yet, waiting..."
    done
    
    echo ">> All consumers subscribed and ready!"
    
    echo ">> Waiting for consumer group rebalancing..."
    sleep 15  
    
    echo ">> Starting producers..."
    docker compose up -d producer_1 producer_2 > /dev/null 2>&1
    echo ">> Producers started!"
    
    echo ">> Environment setup completed successfully!"
    
    echo "============================================="
    echo "Docker Environment is up and running!"
    echo "============================================="
}
