#!/usr/bin/env bash

# ============================================================
# BUILD DELLE IMMAGINI 
# ============================================================
build_docker_images() {
    echo "============================================="
    echo "Build Docker Images"
    echo "============================================="
    
    # Per il momento in fase di testing, il network_emulator viene sempre rebuilddato per garantire 
    # che il file network_rules.conf sia sempre aggiornato nell'immagine
    if [ "$NETWORK_RULES_CONFIGURED" = "true" ] || [ "$NETWORK_RULES_CONFIGURED" = "false" ]; then
        echo ">> Network rules configured, rebuilding network_emulator automatically..."
        docker compose build --quiet network_emulator
        echo ">> Network emulator rebuilt!"
        echo ""
        
        # Chiedi per le altre immagini
        read -p ">> Rebuild other images (brokers, producers, consumers)? (y/n): " -n 1 -r
        echo ""
        
        if [[ $REPLY =~ ^[Yy]$ ]]; then
            read -p ">> Show build logs? (y/n): " -n 1 -r
            echo ""
            echo ">> Starting build process..."
            if [[ $REPLY =~ ^[Yy]$ ]]; then
                # Build con log
                echo ">> Building images (with logs)..."
                docker compose build broker_1 broker_2 broker_3 producer_1 producer_2 consumer_1_g1 consumer_2_g1 consumer_1_g2 kafka_ui
            else
                # Build senza log (quiet)
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
        # Comportamento normale se non ci sono regole configurate
        read -p ">> Do you want to rebuild Docker images? (y/n): " -n 1 -r
        echo ""
        
        if [[ $REPLY =~ ^[Yy]$ ]]; then
            read -p ">> Show build logs? (y/n): " -n 1 -r
            echo ""
            echo ">> Starting build process..."
            if [[ $REPLY =~ ^[Yy]$ ]]; then
                # Build con log
                echo ">> Building images (with logs)..."
                docker compose build network_emulator broker_1 broker_2 broker_3 producer_1 producer_2 consumer_1_g1 consumer_2_g1 consumer_1_g2 message_generator kafka_ui
            else
                # Build senza log (quiet)
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
