#!/bin/bash

set -e # "exit on error"
echo "=== Configuring broker routing as ROOT ==="

# ============================================================
# VERIFICA PRIVILEGI ROOT
# ============================================================

if [ "$(id -u)" -eq 0 ]; then
    # id -u = restituisce l'UID (User ID) dell'utente corrente
    # root ha sempre UID = 0
    # -eq 0 = test di uguaglianza numerica
    
    echo "Running as root, configuring routes..."
    
    # ============================================================
    # CONFIGURAZIONE ROUTE VERSO IL NETWORK EMULATOR
    # ============================================================
    # Il broker vive nella rete 172.20.10.0/24
    # Deve comunicare con:
    #   - Producer (rete 172.20.20.0/24)
    #   - Consumer G1 (rete 172.20.30.0/24)
    #   - Consumer G2 (rete 172.20.40.0/24)
    # 
    # Queste reti sono separate, quindi serve un router intermedio
    # Il network emulator (172.20.10.254) fa da router centrale
    # 
    # Senza queste route, il broker non potrebbe comunicare
    # con producer e consumer (sono in subnet diverse)
    
    # Route verso la rete dei PRODUCER
    ip route add 172.20.20.0/24 via 172.20.10.254 2>/dev/null || echo "Route to producers already exists"
    
    # Route verso la rete dei CONSUMER GRUPPO 1
    ip route add 172.20.30.0/24 via 172.20.10.254 2>/dev/null || echo "Route to consumers G1 already exists"
    
    # Route verso la rete dei CONSUMER GRUPPO 2
    ip route add 172.20.40.0/24 via 172.20.10.254 2>/dev/null || echo "Route to consumers G2 already exists"
    
    # ============================================================
    # VERIFICA ROUTE CONFIGURATE 
    # ============================================================
    # Mostra tutte le route attualmente configurate nel container
    echo "Routes configured:"
    ip route show
    # Output atteso:
    # default via 172.20.10.1 dev eth0
    # 172.20.10.0/24 dev eth0 proto kernel scope link src 172.20.10.11
    # 172.20.20.0/24 via 172.20.10.254 dev eth0   
    # 172.20.30.0/24 via 172.20.10.254 dev eth0  
    # 172.20.40.0/24 via 172.20.10.254 dev eth0   
    
else
    # ============================================================
    # GESTIONE ERRORE: NON SIAMO ROOT
    # ============================================================
    # Se non siamo root, non possiamo modificare le route
    # Terminiamo con errore per evitare un broker mal configurato
    echo "ERROR: Not running as root, cannot configure routes"
    exit 1
fi


# ============================================================
# AVVIO KAFKA
# ============================================================

echo "=== Starting Kafka broker ==="
exec /etc/confluent/docker/run
