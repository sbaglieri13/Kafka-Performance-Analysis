#!/usr/bin/env bash

# ============================================================
# CONFIGURAZIONE DURATA ESPERIMENTO
# ============================================================
configure_experiment_duration() {
    echo "============================================="
    echo "Experiment Duration Configuration"
    echo "============================================="
    
    if [[ "${EXPERIMENT_MODE:-1}" == "1" ]]; then
        # Time-based mode
        read -p "Set experiment duration in seconds [default: 60]: " EXPERIMENT_DURATION
        EXPERIMENT_DURATION=${EXPERIMENT_DURATION:-60}
        
        echo ">> Experiment will run for ${EXPERIMENT_DURATION} seconds"
        echo ""
        
        export EXPERIMENT_DURATION
        export TARGET_MESSAGES="999999999" # valore alto per non limitare in base ai messaggi
    else
        # Message-based mode
        read -p "Set target number of messages [default: 10000]: " TARGET_MESSAGES
        TARGET_MESSAGES=${TARGET_MESSAGES:-10000}
        
        echo ">> Experiment will process ${TARGET_MESSAGES} messages"
        echo ""
        
        export TARGET_MESSAGES
        export EXPERIMENT_DURATION="999999999"  # valore alto per non limitare in base al tempo
    fi
}

# ============================================================
# CONFIGURAZIONE TOPIC E PARAMETRI ESPERIMENTO
# ============================================================
configure_experiment_parameters() {
    echo "============================================="
    echo "Experiment Configuration"
    echo "============================================="
    
    # Scelta topic (numero partizioni)
    echo ""
    echo "Select Kafka topic (by number of partitions):"
    echo "  1) 3 partitions"
    echo "  2) 6 partitions"
    echo "  3) 9 partitions"
    echo "  4) 12 partitions"
    read -n 1 -p "Choice (1-4) [default: 1]: " topic_choice
    echo ""  
    topic_choice=${topic_choice:-1}
    
    case $topic_choice in
        1)
            PARTITION_COUNT=3
            TOPIC_NAME="test_3_partitions"
            ;;
        2)
            PARTITION_COUNT=6
            TOPIC_NAME="test_6_partitions"
            ;;
        3)
            PARTITION_COUNT=9
            TOPIC_NAME="test_9_partitions"
            ;;
        4)
            PARTITION_COUNT=12
            TOPIC_NAME="test_12_partitions"
            ;;
        *)
            echo "Invalid choice, using default (3 partitions)"
            PARTITION_COUNT=3
            TOPIC_NAME="test_3_partitions"
            ;;
    esac
    
    echo ">> Selected topic: $TOPIC_NAME with $PARTITION_COUNT partitions"
    echo ""
    
    # Payload size
    read -p "Payload size in bytes [default: 200]: " PAYLOAD_SIZE_BYTES
    PAYLOAD_SIZE_BYTES=${PAYLOAD_SIZE_BYTES:-200}
    echo ">> Payload size: ${PAYLOAD_SIZE_BYTES} bytes"
    echo ""
    
    # Batch size
    read -p "Number of messages per batch [default: 50]: " NB_MESSAGES_PER_BATCH
    NB_MESSAGES_PER_BATCH=${NB_MESSAGES_PER_BATCH:-50}
    echo ">> Batch size: ${NB_MESSAGES_PER_BATCH} messages"
    echo ""
    
    # Componi RUN_ID
    RUN_ID="${TOPIC_NAME}_NB_${NB_MESSAGES_PER_BATCH}_PS_${PAYLOAD_SIZE_BYTES}"
    echo ">> Run ID: ${RUN_ID}"
    echo ""
    
    # Esporta tutte le variabili per docker-compose
    export TOPIC_NAME
    export PARTITION_COUNT
    export RUN_ID
    export PAYLOAD_SIZE_BYTES
    export NB_MESSAGES_PER_BATCH
    
    echo "============================================="
    echo "Configuration Summary"
    echo "============================================="
    echo "Topic: $TOPIC_NAME"
    echo "Partitions: $PARTITION_COUNT"
    echo "Payload size: $PAYLOAD_SIZE_BYTES bytes"
    echo "Batch size: $NB_MESSAGES_PER_BATCH messages"
    echo "Run ID: $RUN_ID"

    if [[ "${EXPERIMENT_MODE:-1}" == "1" ]]; then
        echo "Mode: Time-based"
        echo "Experiment duration: $EXPERIMENT_DURATION seconds"
    else
        echo "Mode: Message-based"
        echo "Target messages: $TARGET_MESSAGES"
    fi

    echo "============================================="
    echo ""
}
