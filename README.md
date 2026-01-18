# Kafka Performance Analysis

## 📖 Panoramica

Questo progetto implementa un ambiente Kafka distribuito per l'analisi delle performance in condizioni di rete controllate. Prevede:
- **3 broker Kafka** in modalità KRaft
- **2 producer** che generano messaggi
- **3 consumer** suddivisi in 2 gruppi
- **Network Emulator** per simulare delay
- **Message Generator** per la creazione controllata di messaggi

Ognuno di questi componenti è stato implementato come servizio docker.

## 🏗️ Architettura

### Componenti principali

#### Kafka brokers (3 istanze)

- Modalità KRaft 
- Porte: 9092, 9093, 9094 (host), 29092 (inter-broker)
- Porte JMX: 9999, 10000, 10001 per metriche
- Rete: `net_brokers` (172.20.10.0/24)
- Replication Factor: 3
- Min ISR: 2 (numero minimo di repliche “in-sync”)

#### Producers (2 istanze)

- Recuperano messaggi dal *Message Generator* tramite una chiamate API GET
- Inoltrano verso Kafka con batching configurabile
- Scrivono file `data/stats/*_stats.json` con:
  - metriche di invio
  - overhead e throughput misurati dai broker via JMX
- Rete: `net_producers` (172.20.20.0/24) + `net_msggen`

#### Consumers (3 istanze)

- 2 consumer in `group_1`, 1 consumer in `group_2`
- Tracciano timestamp di ricezione per la latency analysis
- Salvano il recap dei messaggi processati
- Reti: `net_consumers_g1` (172.20.30.0/24), `net_consumers_g2` (172.20.40.0/24)

#### Message Generator

- Servizio FastAPI con endpoint `/next-message`
- Genera payload con chiave incrementale e metadata temporali
- Rete: `net_msggen` (172.20.50.0/24)

#### Network Emulator

- Router centrale collegato a tutte le subnet
- Applica regole `tc/netem` per simulare delay

#### Kafka UI

- Interfaccia web per monitorare il cluster su porta 8080


## 📁 Struttura del Progetto

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
├── experiments/
│   ├── latency_analysis/
│   |   ├── reports/
│   |   |   └── latency_report.json       # Output dell'esprimento
│   │   └── latency_analysis.py 
│   └── network_bandwidth_evaluation/
│       ├── plots/
│       |   └── network_bandwidth_evaluation.png      # Output dell'esprimento
│       ├── network_bandwidth_evaluation_launch.sh 
│       └── plot_network_bandwidth_evaluation.py 
├── scripts/
|   └── modules/                      # Moduli di supporto
│     ├── docker_build.sh         
│     ├── docker_deploy.sh       
│     ├── experiment_config.sh        
│     ├── experiment_timer.sh        
│     └── network_config.sh      
|   ├── custom_setup_environment.sh  # Avvio dell'ambiente 
│   ├── teardown_environment.sh      # Cleanup
│   └── update_stats_with_jmx.py     # Raccoglie le metriche con JMX
├── data/                            # Output (messaggi, recap e statistiche)
|   └── stats/ 
├── docker-compose.yml               # Orchestrazione servizi
├── setup_project.sh                 # Script per setup iniziale del progetto
├── .gitignore
├── README.md                        # Documentazione
└── requirements.txt                 # Pacchetti necessari alla preparazione dell'ambiente python
```

## 🚀 Quick Start


### Preparazione Ambiente Python

```bash
# Setup automatico (crea venv, installa dipendenze, crea directory data e data/stats)
./setup_project.sh

# Attiva l'ambiente virtuale
# Linux/Mac:
source venv/bin/activate

# Windows (Git Bash):
source venv/Scripts/activate
```

## 🧪 Esperimenti

### 1. Esperimento Singolo

```bash
./scripts/custom_setup_environment.sh
```

Parametri richiesti:
- Modalità: Time-based (1) o Message-based (2)
- Network Emulation: Configura delay/jitter per ogni subnet
- Topic: Scegli numero di partizioni (3, 6, 9, 12)
- Payload Size: Dimensione messaggi in bytes
- Batch Size: Messaggi per batch
- Duration/Target: Secondi o numero messaggi

Questo script:
- Configura le regole di emulazione di rete (delay, packet loss)
- Imposta la durata dell'esperimento o il target dei messaggi da generare 
- Regola payload size, numero di messaggi per batch, ...
- Effettua il build delle immagini Docker
- Avvia tutti i servizi

Durante l'esecuzione:
- **Minuti 0-1**: Fase di inizializzazione dei servizi
- **Minuti 1+**: Produzione e consumo di messaggi

Al termine dell'esperimento è saranno disponibili nella folder `data` il recap dei messaggi generati, e nella folder `data\stats` le statistiche varie, tra cui throughput e overhead.

### 2. Network Bandwidth Evaluation

```bash
./experiments/network_bandwidth_evaluation/network_bandwidth_evaluation_launch.sh
```
Esegue 120 esperimenti (4 partizioni × 30 batch sizes):
- PARTITION_COUNTS=(3 6 9 12)
- PAYLOAD_SIZE_BYTES=1000
- TARGET_MESSAGES=50000
- BATCH_SIZES=(1 2 3 4 5 6 7 8 9 10 12 13 15 16 18 20 21 23 24 26 29 30 32 35 37 40 41 45 49 50)

Per generare il plot in experiments/network_bandwidth_evaluation/plots/ con l'andamento del throughput e overhead per ogni esecuzione:
```bash
python ./experiments/network_bandwidth_evaluation/plot_network_bandwidth_evaluation.py
```

Il plot mostra 4 subplot (uno per configurazione di partizioni) con:
- Asse Y sinistro (blu): Throughput (KB/s) con prediction curve
- Asse Y destro (rosso): Overhead (%) con prediction curve
- Marker blu (+): Risultati sperimentali throughput
- Marker rosso (○): Risultati sperimentali overhead

Modelli di fitting:
- **Throughput (saturazione esponenziale):**
  $$
  T(x) = c - a \cdot e^{-b x}
  $$

- **Overhead (decadimento esponenziale):**
  $$
  O(x) = a \cdot e^{-b x} + c
  $$


### 3. Analisi Latency
Lanciare un esperimento singolo come al punto 1. Successivamente:
```bash
python ./experiments/latency_analysis/latency_analysis.py
```
Output: `reports/latency_report.json`

### 🧹 Cleanup

```bash
bash scripts/teardown_environment.sh
```

Ferma i container e pulisce i file (con prompt interattivo).


## 📊 Metriche e Output

### File di Output

- **`data/producer_*_messages.jsonl`**: Record di invio messaggi
  ```json
  {
    "key": "8", 
    "run_id": "test_3_partitions_NB_50_PS_200", 
    "topic": "test_3_partitions", 
    "message": ".........", 
    "message_size_bytes": 411, 
    "send_timestamp_ms": 1768665395468.925, 
    "write_timestamp_ms": 1768665395623.809, 
    "status": "OK", 
    "send_batch_id": 1
  }
  ```

- **`data/consumer_*_messages.jsonl`**: Record di ricezione messaggi
  ```json
  {
    "key": "1", 
    "run_id": "test_3_partitions_NB_50_PS_200", 
    "topic": "test_3_partitions", 
    "partition": 2, 
    "offset": 0, 
    "message": "..........", 
    "receive_timestamp_ms": 1768665395603.1628, 
    "producer_send_timestamp_ms": 1768665395455.4229
  }
  ```

- **`data/stats/producer_*_stats.json`**: Statistiche del producer
  ```json
  {
    "run_id": "test_3_partitions_NB_50_PS_200",
    "producer_name": "producer_1",
    "batch_size": 50,
    "num_messages": 30850,
    "num_batches": 617,
    "payload_bytes": 12993955,
    "transmission_time_seconds": 120.05,
    "kafka_overhead_bytes": 969945,
    "kafka_total_bytes": 26978709,
    "throughput_kbps": 219.17,
    "overhead_percentage": 3.6
  }
  ```

- **`experiments/latency_analysis/reports/latency_report.json`**: Latency Report
  ```json
  {
    "run_id": "test_3_partitions_NB_50_PS_200",
    "by_consumer": [
      {
        "consumer": "consumer_1_g1",
        "latency_min_ms": 2.593994140625,
        "latency_max_ms": 184.5341796875,
        "latency_mean_ms": 58.01090192773885,
        "message_count": 41038
      },
      {
        "consumer": "consumer_1_g2",
        "latency_min_ms": 2.374267578125,
        "latency_max_ms": 183.764404296875,
        "latency_mean_ms": 58.00994950736968,
        "message_count": 61750
      },
      {
        "consumer": "consumer_2_g1",
        "latency_min_ms": 2.220703125,
        "latency_max_ms": 147.739990234375,
        "latency_mean_ms": 57.999966842044316,
        "message_count": 20712
      }
    ],
    "by_group": [
      {
        "consumer_group": "group_1",
        "latency_min_ms": 2.220703125,
        "latency_max_ms": 184.5341796875,
        "latency_mean_ms": 58.007234114056175,
        "message_count": 61750
      },
      {
        "consumer_group": "group_2",
        "latency_min_ms": 2.374267578125,
        "latency_max_ms": 183.764404296875,
        "latency_mean_ms": 58.00994950736968,
        "message_count": 61750
      }
    ]
  }
  ```

- **`experiments/network_bandwidth_evaluation/plots/network_bandwidth_evaluation.png`**: Network Bandwidth Evaluation
![Network Bandwidth Evaluation](./experiements/network_bandwidth_evaluation/plots/network_bandwidth_evaluation.png)


## 🐳 Docker Compose

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
| `TARGET_MESSAGES` | Numero di messaggi da generare | 10K |
| `TOPIC_NAME` | Topic Kafka | test_partitions_3 |
| `RUN_ID` | ID della run per tracciamento | test_p3 |
| `PAYLOAD_SIZE_BYTES` | Dimensione payload messaggi | 200 |
| `NB_MESSAGES_PER_BATCH` | Messaggi per batch | 10 |
| `PARTITION_COUNT` | Numero di partizioni | 3 |
| `BOOTSTRAP_SERVERS` | Server Kafka bootstrap | broker_*:29092 |

