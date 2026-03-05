#!/bin/bash

set -e # "exit on error"

echo "=== Network Emulator Starting ==="

# STEP 1: ENABLE IP FORWARDING
# IP forwarding allows the container to forward packets
# from one network to another (i.e., behave like a router).
sysctl -w net.ipv4.ip_forward=1

# STEP 2: CONFIGURE NAT (Network Address Translation)
# NAT allows different networks to communicate with each other.
# MASQUERADE hides the source IP by replacing it with the router's IP.
# This is required because containers on different networks would otherwise not see each other.

echo "=== Configuring NAT rules ==="

# Allow communication: Producers (172.20.20.0/24) <-> Brokers (172.20.10.0/24)
iptables -t nat -A POSTROUTING -s 172.20.20.0/24 -d 172.20.10.0/24 -j MASQUERADE
iptables -t nat -A POSTROUTING -s 172.20.10.0/24 -d 172.20.20.0/24 -j MASQUERADE

# Allow communication: Consumers G1 (172.20.30.0/24) <-> Brokers (172.20.10.0/24)
iptables -t nat -A POSTROUTING -s 172.20.30.0/24 -d 172.20.10.0/24 -j MASQUERADE
iptables -t nat -A POSTROUTING -s 172.20.10.0/24 -d 172.20.30.0/24 -j MASQUERADE

# Allow communication: Consumers G2 (172.20.40.0/24) <-> Brokers (172.20.10.0/24)
iptables -t nat -A POSTROUTING -s 172.20.40.0/24 -d 172.20.10.0/24 -j MASQUERADE
iptables -t nat -A POSTROUTING -s 172.20.10.0/24 -d 172.20.40.0/24 -j MASQUERADE

# STEP 3: ALLOW PACKET FORWARDING
# This rule executes: "accept ALL packets that must be
# forwarded from one network to another."
iptables -A FORWARD -j ACCEPT

# STEP 4: CLEAN EXISTING tc RULES
# Before applying new rules, remove the old ones.
# This prevents conflicts and the accumulation of rules from previous executions.

echo "=== Cleaning old tc rules ==="

for iface in $(ip -o link show | awk -F': ' '{print $2}' | grep -v '^lo$'); do
    tc qdisc del dev $iface root 2>/dev/null || true
    echo "  ✓ Cleaned $iface"
done

# STEP 5: APPLY NETWORK EMULATION (tc/netem)

echo "=== Configuring network emulation ==="

# Load configuration from file (if exists)
source /network_rules.conf 2>/dev/null || {
    echo "No configuration file found, using baseline mode"
    BROKERS="none"
    PRODUCERS="none"
    CONSUMERS_G1="none"
    CONSUMERS_G2="none"
}

# Function to apply tc rules based on configuration
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

    # Parse rules (formato: "delay:200ms:50ms")
    local netem_cmd="tc qdisc add dev $interface root netem"
    
    # Replace commas with spaces to handle multiple rules (es. "delay:200ms:50ms,loss:10%")
    rules=$(echo "$rules" | tr ',' ' ')

    for rule in $rules; do
        local rule_type=$(echo "$rule" | cut -d':' -f1)

        case $rule_type in
            delay)
                local delay_val=$(echo "$rule" | cut -d':' -f2)
                local jitter_val=$(echo "$rule" | cut -d':' -f3)
                local dist_val=$(echo "$rule" | cut -d':' -f4)
                netem_cmd="$netem_cmd delay $delay_val $jitter_val"
                # If distribution is specified (e.g., "normal"), add it to the command
                if [ -n "$dist_val" ]; then
                    netem_cmd="$netem_cmd distribution $dist_val"
                fi
                ;;
            loss)
                local loss_val=$(echo "$rule" | cut -d':' -f2)
                netem_cmd="$netem_cmd loss $loss_val"
                ;;
            dup)
                local dup_val=$(echo "$rule" | cut -d':' -f2)
                netem_cmd="$netem_cmd duplicate $dup_val"
                ;;
        esac
    done

    # Apply the tc command
    echo "DEBUG CMD: $netem_cmd"
    eval "$netem_cmd"
    echo "  ✓ $network_name: $rules applied"
}

# Identify interfaces for each network based on IP addresses
IFACE_BROKERS=$(ip -o addr show | grep "172.20.10.254" | awk '{print $2}')
IFACE_PRODUCERS=$(ip -o addr show | grep "172.20.20.254" | awk '{print $2}')
IFACE_CONSUMERS_G1=$(ip -o addr show | grep "172.20.30.254" | awk '{print $2}')
IFACE_CONSUMERS_G2=$(ip -o addr show | grep "172.20.40.254" | awk '{print $2}')

# Apply tc rules for each network based on configuration
apply_tc_rules "$IFACE_BROKERS" "$BROKERS" "BROKERS"
apply_tc_rules "$IFACE_PRODUCERS" "$PRODUCERS" "PRODUCERS"
apply_tc_rules "$IFACE_CONSUMERS_G1" "$CONSUMERS_G1" "CONSUMERS_G1"
apply_tc_rules "$IFACE_CONSUMERS_G2" "$CONSUMERS_G2" "CONSUMERS_G2"

echo ""
echo "=== Network Emulator Ready ==="


# STEP 6: DISPLAY INFORMATION

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

# Check if any emulation rules are active
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


# STEP 7: KEEP THE CONTAINER RUNNING
exec "$@"
