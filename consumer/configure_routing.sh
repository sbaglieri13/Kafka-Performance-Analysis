#!/bin/bash

set -e # "exit on error"

# ============================================================
# CONFIGURAZIONE ROUTE VERSO I BROKER (GROUP 1)
# ============================================================ù

if [ "$GROUP_ID" = "group_1" ]; then
    # Aggiunge una route statica:
    #   - Destinazione: 172.20.10.0/24 (rete dei broker)
    #   - Gateway: 172.20.30.254 (IP del network emulator nella rete consumer G1)
    # 
    # NOTA: per raggiungere i broker, passa dal network emulator
    
    ip route add 172.20.10.0/24 via 172.20.30.254
    
    echo "Consumer Group 1 routing configured"
fi

# ============================================================
# CONFIGURAZIONE ROUTE VERSO I BROKER (GROUP 2)
# ============================================================

if [ "$GROUP_ID" = "group_2" ]; then
    # Aggiunge una route statica:
    #   - Destinazione: 172.20.10.0/24 (rete dei broker)
    #   - Gateway: 172.20.40.254 (IP del network emulator nella rete consumer G2)
    # 
    # NOTA: i due gruppi usano gateway DIVERSI (172.20.30.254 vs 172.20.40.254)
    # Questo permette di applicare condizioni di rete DIVERSE per ogni gruppo
    #
    # NOTA: per raggiungere i broker, passa dal network emulator
    
    ip route add 172.20.10.0/24 via 172.20.40.254
    
    echo "Consumer Group 2 routing configured"
fi

# ============================================================
# VERIFICA ROUTE CONFIGURATE (DEBUG)
# ============================================================
ip route show

# Output atteso per consumer group 1:
# default via 172.20.30.1 dev eth0
# 172.20.10.0/24 via 172.20.30.254 dev eth0   
# 172.20.30.0/24 dev eth0 proto kernel scope link src 172.20.30.11

# Output atteso per consumer group 2:
# default via 172.20.40.1 dev eth0
# 172.20.10.0/24 via 172.20.40.254 dev eth0   
# 172.20.40.0/24 dev eth0 proto kernel scope link src 172.20.40.11

# ============================================================
# AVVIO CONSUMER
# ============================================================
exec python /app/consumer.py