#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."


# ============================================================
# CONFIGURAZIONE INTERATTIVA NETWORK EMULATION
# ============================================================

# Funzione per configurare una singola rete (FUORI da configure_network_rules)
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
# Chiedi se configurare network emulation
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
  --topic test_partitions_3 \
  --partitions 3 --replication-factor 3 \
  2>&1 | grep -q "Created topic" && echo "   ✓ Created topic: test_partitions_3" || echo "   X Topic already exists: test_partitions_3"

docker exec -it broker_1 kafka-topics \
  --bootstrap-server broker_1:29092 \
  --create \
  --topic test_partitions_6 \
  --partitions 6 --replication-factor 3 \
  2>&1 | grep -q "Created topic" && echo "   ✓ Created topic: test_partitions_6" || echo "   X Topic already exists: test_partitions_6"

docker exec -it broker_1 kafka-topics \
  --bootstrap-server broker_1:29092 \
  --create \
  --topic test_partitions_9 \
  --partitions 9 --replication-factor 3 \
  2>&1 | grep -q "Created topic" && echo "   ✓ Created topic: test_partitions_9" || echo "   X Topic already exists: test_partitions_9"

docker exec -it broker_1 kafka-topics \
  --bootstrap-server broker_1:29092 \
  --create \
  --topic test_partitions_12 \
  --partitions 12 --replication-factor 3 \
  2>&1 | grep -q "Created topic" && echo "   ✓ Created topic: test_partitions_12" || echo "   X Topic already exists: test_partitions_12"

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


