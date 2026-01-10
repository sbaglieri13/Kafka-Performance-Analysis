#!/usr/bin/env bash

# ============================================================
# FUNZIONE PROGRESS BAR ESPERIMENTO
# ============================================================
experiment_timer() {
    local total_duration=$1
    local warmup_duration=60  # Producer warmup time
    local experiment_duration=$((total_duration - warmup_duration))
    
    echo ""
    echo "============================================="
    echo "Experiment Started"
    echo "============================================="
    echo "Total duration: ${total_duration}s (${warmup_duration}s warmup + ${experiment_duration}s production)"
    echo ""
    
    local elapsed=0
    local interval=1  # Update ogni secondo
    local bar_length=50
    
    # Warmup phase
    echo ">> Warmup phase (producers initializing)..."
    while [ $elapsed -lt $warmup_duration ]; do
        sleep $interval
        elapsed=$((elapsed + interval))
        
        local remaining=$((warmup_duration - elapsed))
        local percentage=$((elapsed * 100 / warmup_duration))
        local filled=$((percentage * bar_length / 100))
        local empty=$((bar_length - filled))
        
        # Genera progress bar 
        local bar=$(printf "%${filled}s" | tr ' ' '#')
        bar="${bar}$(printf "%${empty}s")"
        
        printf "\r   [ %-${bar_length}s ] %3d%% - %ds remaining   " "$bar" "$percentage" "$remaining"
    done
    
    local full_bar=$(printf "%${bar_length}s" | tr ' ' '#')
    printf "\r   [ %-${bar_length}s ] 100%% - Warmup completed!     \n" "$full_bar"
    echo ""
    
    # Production phase
    echo ">> Production phase (active message flow)..."
    
    while [ $elapsed -lt $total_duration ]; do
        sleep $interval
        elapsed=$((elapsed + interval))
        
        local remaining=$((total_duration - elapsed))
        local production_elapsed=$((elapsed - warmup_duration))
        local percentage=$((production_elapsed * 100 / experiment_duration))
        local filled=$((percentage * bar_length / 100))
        local empty=$((bar_length - filled))
        
        # Genera progress bar
        local bar=$(printf "%${filled}s" | tr ' ' '#')
        bar="${bar}$(printf "%${empty}s")"
        
        printf "\r   [ %-${bar_length}s ] %3d%% - %ds remaining   " "$bar" "$percentage" "$remaining"
    done
    
    local full_bar=$(printf "%${bar_length}s" | tr ' ' '#')
    printf "\r   [ %-${bar_length}s ] 100%% - Production completed!     \n" "$full_bar"

    echo ""
    echo ">> Waiting consumers..."
    sleep 30  # Attende che i consumer completino l'elaborazione
    
    echo ""
    echo "============================================="
    echo "✓ Experiment Completed!"
    echo "============================================="
    echo ""
    echo "Total time: ${total_duration}s"
    echo "You can now stop the environment with: ./scripts/teardown_environment.sh"
    echo ""
}
