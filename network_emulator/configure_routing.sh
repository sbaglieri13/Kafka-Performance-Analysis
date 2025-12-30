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
# STEP 4: PULISCI REGOLE tc ESISTENTI
# ============================================================
# Prima di applicare nuove regole, rimuovi quelle vecchie
# Questo evita conflitti e accumulo di regole da esecuzioni precedenti

echo "=== Cleaning old tc rules ==="

for iface in $(ip -o link show | awk -F': ' '{print $2}' | grep -v '^lo$'); do
    tc qdisc del dev $iface root 2>/dev/null || true
    echo "  ✓ Cleaned $iface"
done

# ============================================================
# STEP 5: APPLICA EMULAZIONE DI RETE (tc/netem)
# ============================================================

echo "=== Configuring network emulation ==="

# Carica configurazione
source /network_rules.conf 2>/dev/null || {
    echo "No configuration file found, using baseline mode"
    BROKERS="none"
    PRODUCERS="none"
    CONSUMERS_G1="none"
    CONSUMERS_G2="none"
}

# Funzione per applicare regole tc
apply_tc_rules() {
    local interface=$1
    local rules=$2
    local network_name=$3
    
    if [ "$rules" = "none" ] || [ -z "$rules" ]; then
        echo "  $network_name: no emulation"
        return
    fi
    
    if [ -z "$interface" ]; then
        echo "  $network_name: interface not found"
        return
    fi
    
    # Parse rules (formato: "delay:200ms:50ms" o "loss:30%" o "delay:200ms:50ms,loss:10%")
    local netem_cmd="tc qdisc add dev $interface root netem"
    
    # Sostituisci ',' con spazio per separare multiple regole
    rules=$(echo "$rules" | tr ',' ' ')
    
    for rule in $rules; do
        local rule_type=$(echo "$rule" | cut -d':' -f1)
        
        case $rule_type in
            delay)
                local delay_val=$(echo "$rule" | cut -d':' -f2)
                local jitter_val=$(echo "$rule" | cut -d':' -f3)
                netem_cmd="$netem_cmd delay $delay_val $jitter_val"
                ;;
        esac
    done
    
    # Applica il comando
    eval "$netem_cmd"
    echo "  ✓ $network_name: $rules applied"
}

# Identifica interfacce
IFACE_BROKERS=$(ip -o addr show | grep "172.20.10.254" | awk '{print $2}')
IFACE_PRODUCERS=$(ip -o addr show | grep "172.20.20.254" | awk '{print $2}')
IFACE_CONSUMERS_G1=$(ip -o addr show | grep "172.20.30.254" | awk '{print $2}')
IFACE_CONSUMERS_G2=$(ip -o addr show | grep "172.20.40.254" | awk '{print $2}')

# Applica regole
apply_tc_rules "$IFACE_BROKERS" "$BROKERS" "BROKERS"
apply_tc_rules "$IFACE_PRODUCERS" "$PRODUCERS" "PRODUCERS"
apply_tc_rules "$IFACE_CONSUMERS_G1" "$CONSUMERS_G1" "CONSUMERS_G1"
apply_tc_rules "$IFACE_CONSUMERS_G2" "$CONSUMERS_G2" "CONSUMERS_G2"

echo ""
echo "=== Network Emulator Ready ==="


# ============================================================
# STEP 6: MOSTRA INFORMAZIONI 
# ============================================================

echo ""
echo "=== Network Emulator Configuration Summary ==="
echo ""
echo "Infrastructure:"
echo "  ✓ IP Forwarding: ENABLED"
echo "  ✓ NAT: CONFIGURED"
echo "  ✓ Packet Forwarding: ENABLED"
echo ""
echo "Network Emulation Rules:"
echo "  - BROKERS:      ${BROKERS:-none}"
echo "  - PRODUCERS:    ${PRODUCERS:-none}"
echo "  - CONSUMERS_G1: ${CONSUMERS_G1:-none}"
echo "  - CONSUMERS_G2: ${CONSUMERS_G2:-none}"
echo ""

# Controlla se almeno una regola è attiva
if [ "$BROKERS" != "none" ] || [ "$PRODUCERS" != "none" ] || \
   [ "$CONSUMERS_G1" != "none" ] || [ "$CONSUMERS_G2" != "none" ]; then
    echo "Status: ACTIVE EMULATION"
    echo "Traffic conditions are being applied"
else
    echo "Status: BASELINE MODE"
    echo "All traffic routed without emulation"
fi

echo ""
echo "=== Network Emulator is Ready! ==="
echo ""


# ============================================================
# STEP 7: MANTIENI IL CONTAINER ATTIVO
# ============================================================
exec "$@"
