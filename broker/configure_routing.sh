#!/bin/bash

set -e # "exit on error"
echo "=== Configuring broker routing as ROOT ==="

# CHECK ROOT PRIVILEGES
if [ "$(id -u)" -eq 0 ]; then
    # id -u returns the UID (User ID) of the current user
    # root always has UID = 0
    # -eq 0 = numeric equality test
    
    echo "Running as root, configuring routes..."
    
    # ROUTE CONFIGURATION TOWARD THE NETWORK EMULATOR
    # The broker resides in the 172.20.10.0/24 network
    # It must communicate with:
    #   - Producer (network 172.20.20.0/24)
    #   - Consumer G1 (network 172.20.30.0/24)
    #   - Consumer G2 (network 172.20.40.0/24)
    #
    # These networks are isolated, therefore an intermediate router is required.
    # The network emulator (172.20.10.254) acts as the central router.
    #
    # Without these routes, the broker would not be able to communicate
    # with producers and consumers (they are on different subnets).
    
    # Route toward the PRODUCER network
    ip route add 172.20.20.0/24 via 172.20.10.254 2>/dev/null || echo "Route to producers already exists"
    
    # Route toward the CONSUMER GROUP 1 network
    ip route add 172.20.30.0/24 via 172.20.10.254 2>/dev/null || echo "Route to consumers G1 already exists"
    
    # Route toward the CONSUMER GROUP 2 network
    ip route add 172.20.40.0/24 via 172.20.10.254 2>/dev/null || echo "Route to consumers G2 already exists"
    
    # CONFIGURED ROUTES VERIFICATION
    # Displays all routes currently configured in the container
    echo "Routes configured:"
    ip route show
    # Expected output:
    # default via 172.20.10.1 dev eth0
    # 172.20.10.0/24 dev eth0 proto kernel scope link src 172.20.10.11
    # 172.20.20.0/24 via 172.20.10.254 dev eth0   
    # 172.20.30.0/24 via 172.20.10.254 dev eth0  
    # 172.20.40.0/24 via 172.20.10.254 dev eth0   
else
    # ERROR HANDLING: NOT RUNNING AS ROOT
    # If we are not root, we cannot modify routing tables.
    # Exit with an error to prevent a misconfigured broker.
    echo "ERROR: Not running as root, cannot configure routes"
    exit 1
fi

# KAFKA STARTUP
echo "=== Starting Kafka broker ==="
exec /etc/confluent/docker/run
