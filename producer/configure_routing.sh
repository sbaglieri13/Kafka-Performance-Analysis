#!/bin/bash

set -e # "exit on error"

# ROUTE CONFIGURATION TOWARD BROKERS
# Producers reside in the 172.20.20.0/24 network.
# Brokers reside in the 172.20.10.0/24 network (different subnet).
#
# IMPORTANT: producers have TWO networks:
#   1. net_producers (172.20.20.0/24) - used to communicate with brokers
#   2. net_msggen (172.20.50.0/24) - used to communicate with the message_generator
#
# Only traffic toward the BROKERS must pass through the network emulator.
# Traffic toward the message_generator must be DIRECT (no emulation).
#
# Route toward the BROKERS through the network emulator:
# - Destination: 172.20.10.0/24 (broker network)
# - Gateway: 172.20.20.254 (network emulator IP in the producer network)
ip route add 172.20.10.0/24 via 172.20.20.254

# NOTE: the net_msggen network (172.20.50.0/24) does NOT have routes to the router.
#
# This is because we emulate only Kafka traffic, not the API call to the message generator.
#
# CONFIGURED ROUTES VERIFICATION
echo "Producer routing configured"

# Displays all routes currently configured in the container.
ip route show

# Expected output::
# default via 172.20.50.1 dev eth1   
# 172.20.10.0/24 via 172.20.20.254 dev eth0   
# 172.20.20.0/24 via 172.20.20.254 dev eth0   
# 172.20.20.0/24 dev eth0 proto kernel scope link src 172.20.20.11
# 172.20.50.0/24 dev eth1 proto kernel scope link src 172.20.50.11

# PRODUCER START
exec python /app/producer.py
