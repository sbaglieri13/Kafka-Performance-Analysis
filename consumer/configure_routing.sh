#!/bin/bash

set -e # "exit on error"

# ROUTE CONFIGURATION TOWARD BROKERS (GROUP 1)
if [ "$GROUP_ID" = "group_1" ]; then
    # Adds a static route:
    #   - Destination: 172.20.10.0/24 (broker network)
    #   - Gateway: 172.20.30.254 (network emulator IP in the Consumer G1 network)
    #
    # NOTE: to reach the brokers, traffic must go through the network emulator.
    ip route add 172.20.10.0/24 via 172.20.30.254
    echo "Consumer Group 1 routing configured"
fi

# ROUTE CONFIGURATION TOWARD BROKERS (GROUP 2)
if [ "$GROUP_ID" = "group_2" ]; then
    # Adds a static route:
    #   - Destination: 172.20.10.0/24 (broker network)
    #   - Gateway: 172.20.40.254 (network emulator IP in the Consumer G2 network)
    #
    # NOTE: the two groups use DIFFERENT gateways (172.20.30.254 vs 172.20.40.254).
    # This allows applying DIFFERENT network conditions for each group.
    #
    # NOTE: to reach the brokers, traffic must go through the network emulator.
    
    ip route add 172.20.10.0/24 via 172.20.40.254
    echo "Consumer Group 2 routing configured"
fi

# CONFIGURED ROUTES VERIFICATION (DEBUG)
ip route show

# Expected output for Consumer Group 1:
# default via 172.20.30.1 dev eth0
# 172.20.10.0/24 via 172.20.30.254 dev eth0
# 172.20.30.0/24 dev eth0 proto kernel scope link src 172.20.30.11

# Expected output for Consumer Group 2:
# default via 172.20.40.1 dev eth0
# 172.20.10.0/24 via 172.20.40.254 dev eth0
# 172.20.40.0/24 dev eth0 proto kernel scope link src 172.20.40.11

# CONSUMER STARTUP
exec python /app/consumer.py