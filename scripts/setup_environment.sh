#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

# ============================================================
# FUNZIONE PROGRESS BAR ESPERIMENTO
# ============================================================
experiment_timer() {
    local total_duration=$1
    local warmup_duration=60  # Producer warmup time
    local experiment_duration=$((total_duration - warmup_duration))
    
    echo ""
    echo "============================================="
    echo "Experiment Started"
    echo "============================================="
    echo "Total duration: ${total_duration}s (${warmup_duration}s warmup + ${experiment_duration}s production)"
    echo ""
    
    local elapsed=0
    local interval=1  # Update ogni secondo
    local bar_length=50
    
    # Warmup phase
    echo ">> Warmup phase (producers initializing)..."
    while [ $elapsed -lt $warmup_duration ]; do
        sleep $interval
        elapsed=$((elapsed + interval))
        
        local remaining=$((warmup_duration - elapsed))
        local percentage=$((elapsed * 100 / warmup_duration))
        local filled=$((percentage * bar_length / 100))
        local empty=$((bar_length - filled))
        
        # Genera progress bar 
        local bar=$(printf "%${filled}s" | tr ' ' '#')
        bar="${bar}$(printf "%${empty}s")"
        
        printf "\r   [ %-${bar_length}s ] %3d%% - %ds remaining   " "$bar" "$percentage" "$remaining"
    done
    
    local full_bar=$(printf "%${bar_length}s" | tr ' ' '#')
    printf "\r   [ %-${bar_length}s ] 100%% - Warmup completed!     \n" "$full_bar"
    echo ""
    
    # Production phase
    echo ">> Production phase (active message flow)..."
    
    while [ $elapsed -lt $total_duration ]; do
        sleep $interval
        elapsed=$((elapsed + interval))
        
        local remaining=$((total_duration - elapsed))
        local production_elapsed=$((elapsed - warmup_duration))
        local percentage=$((production_elapsed * 100 / experiment_duration))
        local filled=$((percentage * bar_length / 100))
        local empty=$((bar_length - filled))
        
        # Genera progress bar
        local bar=$(printf "%${filled}s" | tr ' ' '#')
        bar="${bar}$(printf "%${empty}s")"
        
        printf "\r   [ %-${bar_length}s ] %3d%% - %ds remaining   " "$bar" "$percentage" "$remaining"
    done
    
    local full_bar=$(printf "%${bar_length}s" | tr ' ' '#')
    printf "\r   [ %-${bar_length}s ] 100%% - Production completed!     \n" "$full_bar"
    
    echo ""
    echo "============================================="
    echo "✓ Experiment Completed!"
    echo "============================================="
    echo ""
    echo "Total time: ${total_duration}s"
    echo "You can now stop the environment with: ./scripts/teardown_environment.sh"
    echo ""
}


# ============================================================
# CONFIGURAZIONE INTERATTIVA NETWORK EMULATION
# ============================================================

# Funzione per configurare una singola rete 
configure_single_network() {
    local network_name=$1
    local network_var=$2
    
    echo ""
    echo "--- Configure $network_name ---"
    read -p "Apply rules to $network_name? (y/n): " -n 1 -r
    echo ""
    
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        echo "Select rule type:"
        echo "  1) Delay"
        echo "  2) Loss"
        echo "  3) Delay + Loss"
        read -p "Choice (1-3): " -n 1 -r rule_choice
        echo ""
        
        local rule=""
        
        case $rule_choice in
            1)
                read -p "Delay (ms) [default: 200]: " delay
                delay=${delay:-200}
                read -p "Jitter (ms) [default: 50]: " jitter
                jitter=${jitter:-50}
                rule="delay:${delay}ms:${jitter}ms"
                ;;
            2)
                read -p "Packet loss (%) [default: 30]: " loss
                loss=${loss:-30}
                rule="loss:${loss}%"
                ;;
            3)
                read -p "Delay (ms) [default: 200]: " delay
                delay=${delay:-200}
                read -p "Jitter (ms) [default: 50]: " jitter
                jitter=${jitter:-50}
                read -p "Packet loss (%) [default: 10]: " loss
                loss=${loss:-30}
                rule="delay:${delay}ms:${jitter}ms,loss:${loss}%"
                ;;
            *)
                echo "Invalid choice, skipping..."
                rule="none"
                ;;
        esac
        
        sed -i "s/^$network_var=.*/$network_var=$rule/" network_emulator/network_rules.conf
        echo "   ✓ $network_name configured: $rule"
    else
        echo "   X $network_name: no emulation"
    fi
}

configure_network_rules() {
    echo "============================================="
    echo "Network Emulation Configuration"
    echo "============================================="
    echo ""
    echo "Available networks:"
    echo "  1) BROKERS       (172.20.10.254)"
    echo "  2) PRODUCERS     (172.20.20.254)"
    echo "  3) CONSUMERS_G1  (172.20.30.254)"
    echo "  4) CONSUMERS_G2  (172.20.40.254)"
    echo ""
    echo "Available rules:"
    echo "  - delay    (add latency)"
    echo "  - loss     (packet loss)"
    echo "  - none     (no emulation)"
    echo ""
    
    # Inizializza file configurazione
    cat > network_emulator/network_rules.conf << 'EOF'
# Network Emulation Rules - Auto-generated
BROKERS=none
PRODUCERS=none
CONSUMERS_G1=none
CONSUMERS_G2=none
EOF
    
    # Configura ogni rete
    configure_single_network "BROKERS" "BROKERS"
    configure_single_network "PRODUCERS" "PRODUCERS"
    configure_single_network "CONSUMERS_G1" "CONSUMERS_G1"
    configure_single_network "CONSUMERS_G2" "CONSUMERS_G2"
    
    echo ""
    echo "============================================="
    echo "Configuration saved!"
    echo "============================================="
    echo ""
    cat network_emulator/network_rules.conf
    echo ""
    read -p "Proceed with this configuration? (y/n): " -n 1 -r
    echo ""
    
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        echo "Setup cancelled."
        exit 0
    fi
}

echo "============================================="
echo "Setup Network Emulation"
echo "============================================="
# Chiedi se applicare regole 
NETWORK_RULES_CONFIGURED="false"  
read -p "Configure network emulation rules? (y/n): " -n 1 -r
echo ""

if [[ $REPLY =~ ^[Yy]$ ]]; then
    NETWORK_RULES_CONFIGURED="true"
    configure_network_rules
else
    echo "Using default configuration (no emulation)..."
    cat > network_emulator/network_rules.conf << 'EOF'
# Network Emulation Rules - Default (no emulation)
BROKERS=none
PRODUCERS=none
CONSUMERS_G1=none
CONSUMERS_G2=none
EOF
fi

echo "============================================="
echo "Network emulation setup complete!"
echo "============================================="

# ============================================================
# CONFIGURAZIONE DURATA ESPERIMENTO
# ============================================================
echo "============================================="
echo "Experiment Duration Configuration"
echo "============================================="

read -p "Set experiment duration in seconds [default: 60]: " EXPERIMENT_DURATION
EXPERIMENT_DURATION=${EXPERIMENT_DURATION:-60}

echo ">> Experiment will run for ${EXPERIMENT_DURATION} seconds"
echo ""

# Esporta come variabile d'ambiente per docker-compose
export EXPERIMENT_DURATION

# ============================================================
# CONFIGURAZIONE TOPIC E PARAMETRI ESPERIMENTO
# ============================================================
echo "============================================="
echo "Experiment Configuration"
echo "============================================="

# Scelta topic (numero partizioni)
echo ""
echo "Select Kafka topic (by number of partitions):"
echo "  1) 3 partitions"
echo "  2) 6 partitions"
echo "  3) 9 partitions"
echo "  4) 12 partitions"
read -n 1 -p "Choice (1-4) [default: 1]: " topic_choice
echo ""  
topic_choice=${topic_choice:-1}

case $topic_choice in
    1)
        PARTITION_COUNT=3
        TOPIC_NAME="test_3_partitions"
        ;;
    2)
        PARTITION_COUNT=6
        TOPIC_NAME="test_6_partitions"
        ;;
    3)
        PARTITION_COUNT=9
        TOPIC_NAME="test_9_partitions"
        ;;
    4)
        PARTITION_COUNT=12
        TOPIC_NAME="test_12_partitions"
        ;;
    *)
        echo "Invalid choice, using default (3 partitions)"
        PARTITION_COUNT=3
        TOPIC_NAME="test_3_partitions"
        ;;
esac

echo ">> Selected topic: $TOPIC_NAME with $PARTITION_COUNT partitions"
echo ""

# Payload size
read -p "Payload size in bytes [default: 200]: " PAYLOAD_SIZE_BYTES
PAYLOAD_SIZE_BYTES=${PAYLOAD_SIZE_BYTES:-200}
echo ">> Payload size: ${PAYLOAD_SIZE_BYTES} bytes"
echo ""

# Batch size
read -p "Number of messages per batch [default: 50]: " NB_MESSAGES_PER_BATCH
NB_MESSAGES_PER_BATCH=${NB_MESSAGES_PER_BATCH:-50}
echo ">> Batch size: ${NB_MESSAGES_PER_BATCH} messages"
echo ""

# Componi RUN_ID
RUN_ID="${TOPIC_NAME}_NB_${NB_MESSAGES_PER_BATCH}_PS_${PAYLOAD_SIZE_BYTES}"
echo ">> Run ID: ${RUN_ID}"
echo ""

# Esporta tutte le variabili per docker-compose
export TOPIC_NAME
export PARTITION_COUNT
export RUN_ID
export PAYLOAD_SIZE_BYTES
export NB_MESSAGES_PER_BATCH

echo "============================================="
echo "Configuration Summary"
echo "============================================="
echo "Topic: $TOPIC_NAME"
echo "Partitions: $PARTITION_COUNT"
echo "Payload size: $PAYLOAD_SIZE_BYTES bytes"
echo "Batch size: $NB_MESSAGES_PER_BATCH messages"
echo "Run ID: $RUN_ID"
echo "Experiment duration: $EXPERIMENT_DURATION seconds"
echo "============================================="
echo ""


# ============================================================
# BUILD DELLE IMMAGINI 
# ============================================================
echo "============================================="
echo "Build Docker Images"
echo "============================================="

# Per il momento in fase di testing, il network_emulator viene sempre rebuilddato per garantire 
# che il file network_rules.conf sia sempre aggiornato nell'immagine
if [ "$NETWORK_RULES_CONFIGURED" = "true" ] || [ "$NETWORK_RULES_CONFIGURED" = "false" ]; then
    echo ">> Network rules configured, rebuilding network_emulator automatically..."
    docker compose build --quiet network_emulator
    echo ">> Network emulator rebuilt!"
    echo ""
    
    # Chiedi per le altre immagini
    read -p ">> Rebuild other images (brokers, producers, consumers)? (y/n): " -n 1 -r
    echo ""
    
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        read -p ">> Show build logs? (y/n): " -n 1 -r
        echo ""
        echo ">> Starting build process..."
        if [[ $REPLY =~ ^[Yy]$ ]]; then
            # Build con log
            echo ">> Building images (with logs)..."
            docker compose build broker_1 broker_2 broker_3 producer_1 producer_2 consumer_1_g1 consumer_2_g1 consumer_1_g2 message_generator kafka_ui
        else
            # Build senza log (quiet)
            echo ">> Building images (quiet mode)..."
            docker compose build --quiet broker_1 broker_2 broker_3 producer_1 producer_2 consumer_1_g1 consumer_2_g1 consumer_1_g2 message_generator kafka_ui
        fi
        echo ">> Build completed!"
        echo ""
    else
        echo ">> Skipping build for other services..."
        echo ""
    fi
else
    # Comportamento normale se non ci sono regole configurate
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
fi

echo "============================================="
echo "Build process finished!"
echo "============================================="


# ============================================================
# AVVIO DEI SERVIZI DOCKER 
# ============================================================
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

docker exec -it broker_1 kafka-topics \
  --bootstrap-server broker_1:29092 \
  --create \
  --topic test_3_partitions \
  --partitions 3 --replication-factor 3 \
  2>&1 | grep -q "Created topic" && echo "   ✓ Created topic: test_3_partitions" || echo "   X Topic already exists: test_3_partitions"

docker exec -it broker_1 kafka-topics \
  --bootstrap-server broker_1:29092 \
  --create \
  --topic test_6_partitions \
  --partitions 6 --replication-factor 3 \
  2>&1 | grep -q "Created topic" && echo "   ✓ Created topic: test_6_partitions" || echo "   X Topic already exists: test_6_partitions"

docker exec -it broker_1 kafka-topics \
  --bootstrap-server broker_1:29092 \
  --create \
  --topic test_9_partitions \
  --partitions 9 --replication-factor 3 \
  2>&1 | grep -q "Created topic" && echo "   ✓ Created topic: test_9_partitions" || echo "   X Topic already exists: test_9_partitions"

docker exec -it broker_1 kafka-topics \
  --bootstrap-server broker_1:29092 \
  --create \
  --topic test_12_partitions \
  --partitions 12 --replication-factor 3 \
  2>&1 | grep -q "Created topic" && echo "   ✓ Created topic: test_12_partitions" || echo "   X Topic already exists: test_12_partitions"

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


# ============================================================
# AVVIO TIMER ESPERIMENTO
# ============================================================
TOTAL_EXPERIMENT_TIME=$((EXPERIMENT_DURATION + 60))  # +60 for producer warmup
experiment_timer $TOTAL_EXPERIMENT_TIME