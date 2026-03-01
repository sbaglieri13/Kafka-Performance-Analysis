#!/usr/bin/env bash

# BUILD IMAGES
build_docker_images() {
    echo "============================================="
    echo "Build Docker Images"
    echo "============================================="
    
    # The network_emulator is always rebuilt to ensure that the network_rules.conf file is consistently updated in the image.
    if [ "$NETWORK_RULES_CONFIGURED" = "true" ] || [ "$NETWORK_RULES_CONFIGURED" = "false" ]; then
        echo ">> Network rules configured, rebuilding network_emulator automatically..."
        docker compose build --quiet network_emulator
        echo ">> Network emulator rebuilt!"
        echo ""
        
        # Ask for other images
        read -p ">> Rebuild other images (brokers, producers, consumers)? (y/n): " -n 1 -r
        echo ""
        
        if [[ $REPLY =~ ^[Yy]$ ]]; then
            read -p ">> Show build logs? (y/n): " -n 1 -r
            echo ""
            echo ">> Starting build process..."
            if [[ $REPLY =~ ^[Yy]$ ]]; then
                # Build with log
                echo ">> Building images (with logs)..."
                docker compose build broker_1 broker_2 broker_3 producer_1 producer_2 consumer_1_g1 consumer_2_g1 consumer_1_g2 kafka_ui
            else
                # Build without log (quiet)
                echo ">> Building images (quiet mode)..."
                docker compose build --quiet broker_1 broker_2 broker_3 producer_1 producer_2 consumer_1_g1 consumer_2_g1 consumer_1_g2 kafka_ui
            fi
            echo ">> Build completed!"
            echo ""
        else
            echo ">> Skipping build for other services..."
            echo ""
        fi
    else
        # If the network rules are not configured, we ask the user if they want to rebuild all images (including the network_emulator) to ensure that they can set up the network rules before the build.
        read -p ">> Do you want to rebuild Docker images? (y/n): " -n 1 -r
        echo ""
        
        if [[ $REPLY =~ ^[Yy]$ ]]; then
            read -p ">> Show build logs? (y/n): " -n 1 -r
            echo ""
            echo ">> Starting build process..."
            if [[ $REPLY =~ ^[Yy]$ ]]; then
                # Build with log
                echo ">> Building images (with logs)..."
                docker compose build network_emulator broker_1 broker_2 broker_3 producer_1 producer_2 consumer_1_g1 consumer_2_g1 consumer_1_g2 message_generator kafka_ui
            else
                # Build without log (quiet)
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
}
