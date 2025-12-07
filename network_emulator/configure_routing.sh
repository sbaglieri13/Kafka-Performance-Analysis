#!/bin/bash

set -e # "exit on error"

echo "=== Network Emulator Starting ==="

# ============================================================
# STEP 1: ABILITA IP FORWARDING
# ============================================================
# IP forwarding permette al container di inoltrare pacchetti 
# da una rete all'altra (comportarsi come un router)
sysctl -w net.ipv4.ip_forward=1

# ============================================================
# STEP 2: CONFIGURA NAT (Network Address Translation)
# ============================================================
# NAT permette a reti diverse di comunicare tra loro
# MASQUERADE nasconde l'IP sorgente sostituendolo con quello del router
# Serve perché i container in reti diverse non si vedrebbero altrimenti

echo "=== Configuring NAT rules ==="

# Permetti comunicazione: Producers (172.20.20.0/24) <-> Brokers (172.20.10.0/24)
iptables -t nat -A POSTROUTING -s 172.20.20.0/24 -d 172.20.10.0/24 -j MASQUERADE
iptables -t nat -A POSTROUTING -s 172.20.10.0/24 -d 172.20.20.0/24 -j MASQUERADE

# Permetti comunicazione: Consumers G1 (172.20.30.0/24) <-> Brokers (172.20.10.0/24)
iptables -t nat -A POSTROUTING -s 172.20.30.0/24 -d 172.20.10.0/24 -j MASQUERADE
iptables -t nat -A POSTROUTING -s 172.20.10.0/24 -d 172.20.30.0/24 -j MASQUERADE

# Permetti comunicazione: Consumers G2 (172.20.40.0/24) <-> Brokers (172.20.10.0/24)
iptables -t nat -A POSTROUTING -s 172.20.40.0/24 -d 172.20.10.0/24 -j MASQUERADE
iptables -t nat -A POSTROUTING -s 172.20.10.0/24 -d 172.20.40.0/24 -j MASQUERADE

# ============================================================
# STEP 3: PERMETTI FORWARDING DEI PACCHETTI
# ============================================================
# Questa regola esegue il comando: "accetta TUTTI i pacchetti che devono essere 
# inoltrati da una rete all'altra"
iptables -A FORWARD -j ACCEPT

# ============================================================
# STEP 4: MOSTRA INFORMAZIONI 
# ============================================================
echo ""
echo "=== Network Emulator Ready (BASELINE MODE) ==="
echo "IP Forwarding: ENABLED"
echo "NAT: CONFIGURED"
echo "Network Emulation (tc/netem): DISABLED (baseline performance)"
echo ""
echo "All traffic is routed but no delays/losses are applied"

# ============================================================
# STEP 5: MANTIENI IL CONTAINER ATTIVO
# ============================================================
exec "$@"
