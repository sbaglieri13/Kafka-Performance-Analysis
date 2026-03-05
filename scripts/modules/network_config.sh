#!/usr/bin/env bash

# NETWORK EMULATION CONFIGURATION

configure_single_network() {
    local network_name=$1
    local network_var=$2
    local rule=""

    echo ""
    echo "--- Configure $network_name ---"

    # DELAY + OPTIONAL DISTRIBUTION
    read -p "Apply delay to $network_name? (y/n): " -n 1 -r; echo ""
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        read -p "  Delay (ms) [default: 200]: " delay; delay=${delay:-200}
        read -p "  Jitter (ms) [default: 50]: " jitter; jitter=${jitter:-50}

        read -p "  Apply distribution? (y/n): " -n 1 -r; echo ""
        local dist=""
        if [[ $REPLY =~ ^[Yy]$ ]]; then
            echo "  Available distributions: uniform | normal | pareto | paretonormal"
            read -p "  Distribution [default: normal]: " dist; dist=${dist:-normal}
        fi

        if [ -n "$dist" ]; then
            rule="${rule}delay:${delay}ms:${jitter}ms:${dist},"
        else
            rule="${rule}delay:${delay}ms:${jitter}ms,"
        fi
    fi

    # LOSS
    read -p "Apply packet loss to $network_name? (y/n): " -n 1 -r; echo ""
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        read -p "  Loss percentage [default: 1]: " loss; loss=${loss:-1}
        rule="${rule}loss:${loss}%,"
    fi

    # DUPLICATION
    read -p "Apply packet duplication to $network_name? (y/n): " -n 1 -r; echo ""
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        read -p "  Duplication percentage [default: 1]: " dup; dup=${dup:-1}
        rule="${rule}dup:${dup}%,"
    fi

    # Remove trailing comma
    rule=$(echo "$rule" | sed 's/,$//')
    [ -z "$rule" ] && rule="none"

    sed -i "s/^$network_var=.*/$network_var=$rule/" network_emulator/network_rules.conf
    echo "  ✓ $network_name configured: $rule"
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
    echo "Available rules"
    echo "  - delay         (latency + jitter, optional distribution)"
    echo "  - loss          (packet loss %)"
    echo "  - duplication   (packet duplication %)"
    echo "  - none          (no emulation)"
    echo ""
    
    # Initialize config file with default values
    cat > network_emulator/network_rules.conf << 'EOF'
# Network Emulation Rules - Auto-generated
BROKERS=none
PRODUCERS=none
CONSUMERS_G1=none
CONSUMERS_G2=none
EOF
    
    # Configure each network one by one
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

setup_network_emulation() {
    echo "============================================="
    echo "Setup Network Emulation"
    echo "============================================="
    
    # Ask user if they want to configure network emulation rules
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
    
    # Export a variable to indicate whether custom network rules were configured, so it can be used in docker_deploy.sh to decide whether to start the network emulator container
    export NETWORK_RULES_CONFIGURED
}
