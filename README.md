# Kafka Performance Analysis

Questo progetto implementa un ambiente Kafka distribuito con:
- **3 broker Kafka** in modalità KRaft
- **2 producer** che generano messaggi
- **3 consumer** suddivisi in 2 gruppi
- **Network Emulator** per simulare latenza e perdita di pacchetti
- **Message Generator** per la creazione controllata di messaggi
- **Statistical Analysis Tool** per l'analisi della latenza

Ognuno dei componenti sopra è stato implementato come servizio docker, ad eccezione del **Statistical Analysis Tool** che per il momento è un semplice file python da lanciare quando necessario.

L'infrastruttura misura e analizza la latenza end-to-end dei messaggi Kafka in diverse condizioni di rete configurabili tramite script al setup dell'ambiente.

## Architettura

### Docker Networks

```
┌─────────────────────────────────────────────────────────┐
│                                                         │
│  172.20.10.0/24 (BROKERS)                               │
│  ├─ broker_1 (172.20.10.11)                             │
│  ├─ broker_2 (172.20.10.12)                             │
│  ├─ broker_3 (172.20.10.13)                             │
│  ├─ kafka_ui (172.20.10.20)                             │
│  └─ network_emulator (172.20.10.254) [ROUTER]           │
│                                                         │
│  172.20.20.0/24 (PRODUCERS)                             │
│  ├─ producer_1 (172.20.20.11)                           │
│  ├─ producer_2 (172.20.20.12)                           │
│  └─ network_emulator (172.20.20.254) [ROUTER]           │
│                                                         │
│  172.20.30.0/24 (CONSUMERS GROUP 1)                     │
│  ├─ consumer_1_g1 (172.20.30.11)                        │
│  ├─ consumer_2_g1 (172.20.30.12)                        │
│  └─ network_emulator (172.20.30.254) [ROUTER]           │
│                                                         │
│  172.20.40.0/24 (CONSUMERS GROUP 2)                     │
│  ├─ consumer_1_g2 (172.20.40.11)                        │
│  └─ network_emulator (172.20.40.254) [ROUTER]           │
│                                                         │
│  172.20.50.0/24 (MESSAGE GENERATOR)                     │
│  └─ message_generator (172.20.50.10)                    │
│                                                         │
└─────────────────────────────────────────────────────────┘
```

### Flusso dati

```
message_generator → producer_1/producer_2 → [network_emulator] → brokers → [network_emulator] → consumers
                    ↓                                                                              ↓
                    └─ producer_*_messages.jsonl ←──────────────────── consumer_*_messages.jsonl ─┘
                                                                                ↓
                                              statistical_analysis_tool → latency_report.json
```

## Quick Start


### 1. Preparazione Ambiente Python

```bash
# Crea un ambiente virtuale
python -m venv venv

# Attiva l'ambiente virtuale
venv/bin/activate

# Installa i requisiti
pip install -r requirements.txt
```

### 2. Setup Iniziale

```bash
bash scripts/setup_environment.sh
```

Questo script:
- Configura le regole di emulazione di rete (delay, packet loss)
- Imposta la durata dell'esperimento
- Effettua il build delle immagini Docker
- Avvia tutti i servizi

### 3. Esecuzione Esperimento

L'esperimento inizia automaticamente dopo il setup. Durante l'esecuzione:
- **Minuti 0-1**: Fase di inizializzazione dei servizi
- **Minuti 1+**: Produzione e consumo di messaggi

### 4. Analisi dei Risultati
Al termine dell'esperimento è possibie effettuare l'analisi della latency con il comando: 

```bash
python statistical_analysis_tool/statistical_analysis_tool.py
```

Questo genera `data/latency_report.json` con le statistiche di latenza.

### 5. Cleanup

```bash
bash scripts/teardown_environment.sh
```

Ferma i container e pulisce i file (con prompt interattivo).

## Struttura del Progetto

```
.
├── broker/                          # Configurazione broker Kafka
│   ├── Dockerfile
│   └── configure_routing.sh
├── producer/                        # Producer Kafka
│   ├── Dockerfile
│   ├── producer.py
│   ├── configure_routing.sh
│   └── requirements.txt
├── consumer/                        # Consumer Kafka
│   ├── Dockerfile
│   ├── consumer.py
│   ├── configure_routing.sh
│   └── requirements.txt
├── message_generator/               # Generatore API REST di messaggi
│   ├── Dockerfile
│   ├── message_generator.py
│   └── requirements.txt
├── network_emulator/                # Emulatore condizioni di rete e funge da router centrale
│   ├── Dockerfile
│   ├── configure_routing.sh
│   └── network_rules.conf
├── statistical_analysis_tool/       # Analisi della latenza end-to-end
│   └── statistical_analysis_tool.py
├── scripts/
|   └── modules                      # Moduli di supporto a setup_environment.sh
│     ├── docker_build.sh         
│     ├── docker_deploy.sh       
│     ├── experiment_config.sh        
│     ├── experiment_timer.sh        
│     └── network_config.sh      
|   ├── setup_environment.sh         # Setup iniziale
│   └── teardown_environment.sh      # Cleanup
├── data/                            # Output (messaggi e report)
├── docker-compose.yml               # Orchestrazione servizi
├── README.md
└── requirements.txt                 # Pacchetti necessari alla preparazione dell'ambiente python
```

## Configurazione Network Emulation

Durante il setup, è possibile configurare le regole di emulazione per ogni rete:

### Tipi di Regole Disponibili

1. **Delay**: Aggiunge latenza
   ```
   delay:200ms:50ms  # delay + jitter
   ```

## Output e Analisi

### File di Output

- **`data/producer_*_messages.jsonl`**: Record di invio messaggi
  ```json
  {
    "key": "123",
    "run_id": "test_p3",
    "topic": "test_partitions_3",
    "message": "xxxx...",
    "message_size_bytes": 256,
    "send_timestamp_ms": 1704067200000.5,
    "write_timestamp_ms": 1704067200010.2,
    "status": "OK",
    "send_batch_id": 1
  }
  ```

- **`data/consumer_*_messages.jsonl`**: Record di ricezione messaggi
  ```json
  {
    "key": "123",
    "run_id": "test_p3",
    "topic": "test_partitions_3",
    "partition": 0,
    "offset": 42,
    "message": "xxxx...",
    "receive_timestamp_ms": 1704067200050.8,
    "producer_send_timestamp_ms": 1704067200000.5
  }
  ```

- **`data/latency_report.json`**: Report di analisi
  ```json
  {
    "run_id": "test_p3",
    "by_consumer": [
      {
        "consumer": "consumer_1_g1",
        "latency_min_ms": 2.5,
        "latency_max_ms": 150.8,
        "latency_mean_ms": 45.3,
        "message_count": 5420
      }
    ],
    "by_group": [
      {
        "consumer_group": "group_1",
        "latency_min_ms": 2.1,
        "latency_max_ms": 200.5,
        "latency_mean_ms": 48.7,
        "message_count": 10840
      }
    ]
  }
  ```

## Componenti Principali

### producer.py

Produce messaggi a Kafka con:
- Timestamp di invio
- ID batch per tracciamento
- Payload configurabile
- ....

### consumer.py

Consuma messaggi da Kafka e registra:
- Timestamp di ricezione
- Partition e offset
- Timestamp originale del producer (per calcolo latenza)
- ....

### message_generator.py

FastAPI endpoint `/next-message` che fornisce:
- ID messaggio incrementale
- Payload di dimensione configurabile
- Timestamp di generazione (al momento non utilizzato)

### network_emulator/configure_routing.sh

Configura l'infrastruttura di rete:
- IP forwarding
- NAT per comunicazione cross-subnet
- Regole `tc` (traffic control) per emulazione latenza/loss

### statistical_analysis_tool.py

Analizza latenza end-to-end:
- Calcola latenza per consumer
- Aggrega per consumer group

## Docker Compose

Il file `docker-compose.yml` orchestra:

- **3 Broker Kafka** con KRaft 
- **Kafka UI** (port 8080) per visualizzazione cluster
- **2 Producer** su rete dedicata
- **3 Consumer** su 2 reti distinte (per test emulazione separata)
- **Message Generator** (port 8000) su rete dedicata
- **Network Emulator** su tutte le reti

## Variabili d'Ambiente

| Variabile | Descrizione | Default |
|-----------|-------------|---------|
| `EXPERIMENT_DURATION` | Durata esperimento (secondi) | 60 |
| `TOPIC_NAME` | Topic Kafka | test_partitions_3 |
| `RUN_ID` | ID della run per tracciamento | test_p3 |
| `PAYLOAD_SIZE_BYTES` | Dimensione payload messaggi | 200 |
| `NB_MESSAGES_PER_BATCH` | Messaggi per batch | 10 |
| `PARTITION_COUNT` | Numero di partizioni | 3 |
| `BOOTSTRAP_SERVERS` | Server Kafka bootstrap | broker_*:29092 |

