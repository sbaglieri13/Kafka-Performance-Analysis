#!/bin/bash

set -e # "exit on error"

# ============================================================
# CONFIGURAZIONE ROUTE VERSO I BROKER
# ============================================================
# I producer vivono nella rete 172.20.20.0/24
# I broker vivono nella rete 172.20.10.0/24 (subnet diversa)
# 
# IMPORTANTE: i producer hanno DUE reti:
#   1. net_producers (172.20.20.0/24) - per comunicare con i broker
#   2. net_msggen (172.20.50.0/24) - per comunicare con message_generator
# 
# Solo il traffico verso i BROKER deve passare dal network emulator
# Il traffico verso message_generator deve essere DIRETTO (nessun emulazione)

# Route verso i BROKER attraverso il network emulator
# - Destinazione: 172.20.10.0/24 (rete dei broker)
# - Gateway: 172.20.20.254 (IP del network emulator nella rete producer)
ip route add 172.20.10.0/24 via 172.20.20.254


# NOTA: la rete net_msggen (172.20.50.0/24) NON ha route verso il router
# 
# Questo perché emuliamo solo il traffico Kafka, non la chiamata API al message generator

# ============================================================
# VERIFICA ROUTE CONFIGURATE 
# ============================================================
echo "Producer routing configured"

# Mostra tutte le route attualmente configurate nel container
ip route show

# Output atteso:
# default via 172.20.50.1 dev eth1   
# 172.20.10.0/24 via 172.20.20.254 dev eth0   
# 172.20.20.0/24 via 172.20.20.254 dev eth0   
# 172.20.20.0/24 dev eth0 proto kernel scope link src 172.20.20.11
# 172.20.50.0/24 dev eth1 proto kernel scope link src 172.20.50.11

# ============================================================
# AVVIO PRODUCER 
# ============================================================
exec python /app/producer.py
