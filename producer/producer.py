import math
import os
import time
import json
import socket
import itertools
import random
import string
from pathlib import Path
from confluent_kafka import Producer, Consumer
from typing import Dict
import numpy as np

bootstrap = os.getenv("BOOTSTRAP_SERVERS")
topic = os.getenv("TOPIC_NAME")
run_id = os.getenv("RUN_ID")
payload_size_bytes = int(os.getenv("PAYLOAD_SIZE_BYTES"))
nb_messages_per_batch = int(os.getenv("NB_MESSAGES_PER_BATCH"))
producer_name = os.getenv("CONTAINER_NAME", socket.gethostname())
partition_count = int(os.getenv("PARTITION_COUNT"))
lambda_rate = float(os.getenv("LAMBDA_RATE", "50.0"))  # msg/s, default Medium

BATCH_HEADER_SIZE = 61  # bytes, formato Kafka fisso
COMPRESSION_RATIO = 1.0  # nessuna compressione

key_counter = itertools.count(start=1) # contatore per generare chiavi uniche incrementali
def make_message() -> dict:
    """Genera payload e chiave incrementale"""
    key     = str(next(key_counter)) # chiave incrementale unica per ogni messaggio
    message = ''.join(random.choices(string.ascii_lowercase, k=payload_size_bytes)) # payload casuale di lettere minuscole della lunghezza specificata
    return {"key": key, "message": message}

# Funzione per simulare arrivi Poisson
def poisson_sleep():
    """Attende un intervallo esponenziale con media 1/lambda_rate secondio per simulare un processo di arrivo Poisson"""
    interval = np.random.exponential(1.0 / lambda_rate)
    time.sleep(interval)
    
linger_ms = 30000 # per non inviare batch incompleti troppo presto, ma lasciare tempo sufficiente per completare i batch anche a lambda alti

current_batch_id = 0  # serve per tracciare l'ID del batch corrente cosi da testare se effettivamente in un batch ci sono i messaggi attesi
messages_in_current_batch = 0 # serve per contare quanti messaggi sono stati inviati nel batch corrente, cosi da sapere quando incrementare l'ID del batch
delivery_records = [] 
batch_tracking_data = [] 

# Tracking per batch+broker (per gestire N partizioni su M broker)
batch_broker_first_delivery = {}  # Traccia il primo delivery per ogni batch+broker

# Variabile globale per tracciare l'ultimo invio GLOBALE
last_global_send_per_broker = {}  # {broker_id: timestamp_ms}
batches_sent_since_last_record = {}  # {broker_id: counter}

data_dir = Path("/app/data")
data_dir.mkdir(parents=True, exist_ok=True)

stats_dir = Path("/app/data/stats")
stats_dir.mkdir(parents=True, exist_ok=True)

# File per salvare i record dei messaggi inviati
jsonl_path = data_dir / f"{producer_name}_{run_id}_messages_recap.jsonl"

def get_partition_leader_mapping() -> Dict[int, int]:
    """
    Ottiene il mapping Partizione -> Broker Leader usando Admin API
    Ritorna: {partition_id: broker_id}
    """
    consumer = Consumer({
        'bootstrap.servers': bootstrap,
        'group.id': 'metadata_query_temp'
    })
    
    try:
        metadata = consumer.list_topics(topic=topic, timeout=10)
        topic_metadata = metadata.topics[topic]
        
        partition_leaders = {}
        for partition_id, partition_metadata in topic_metadata.partitions.items():
            leader_broker_id = partition_metadata.leader
            partition_leaders[partition_id] = leader_broker_id
            print(f"Partition {partition_id} -> Broker Leader {leader_broker_id}", flush=True)
        
        consumer.close()
        return partition_leaders
    except Exception as e:
        print(f"Error getting partition metadata: {e}", flush=True)
        consumer.close()
        return {}


# Ottieni mapping partizioni all'avvio del producer
partition_to_broker = get_partition_leader_mapping()

# Tracking per broker
broker_tracking = {
    broker_id: {
        'batch_count': 0,
        'partition_count': 0,  # inizializzato a 0, sarà calcolato dopo
    }
    for broker_id in set(partition_to_broker.values()) if partition_to_broker
}

# Conta quante partizioni ha ogni broker
if partition_to_broker and broker_tracking:
    for partition_id, broker_id in partition_to_broker.items():
        broker_tracking[broker_id]['partition_count'] += 1
    
    print(f"\nBroker distribution:", flush=True)
    for broker_id, info in broker_tracking.items():
        print(f"  Broker {broker_id}: {info['partition_count']} partitions", flush=True)

# stima preliminare della dimensione messaggio
def estimate_message_size() -> int:
    tmp_key = "0"
    tmp_payload = {
        "key": tmp_key,
        "run_id": run_id,
        "topic": topic,
        "message": "x" * payload_size_bytes,
        "send_timestamp_ms": 0.0,
        "status": "PENDING",
        "send_batch_id": 0,
    }
    value_bytes = json.dumps(tmp_payload).encode("utf-8")
    return len(tmp_key.encode("utf-8")) + len(value_bytes)


estimated_msg_size = estimate_message_size()
messages_per_partition = math.ceil(nb_messages_per_batch / partition_count) # numero di messaggi per partizione in ogni batch (arrotondato per eccesso)
batch_size_bytes = int(estimated_msg_size * messages_per_partition * 1.1) # margine del 10% per sicurezza

producer_conf = {
    "bootstrap.servers": bootstrap, # indirizzo del cluster Kafka
    "linger.ms": linger_ms,  # tempo massimo in ms per aspettare prima di inviare un batch incompleto
    "batch.size": batch_size_bytes, # dimensione massima del batch in byte (calcolata come stima dimensione messaggio * nb_messages_per_batch * margine)
}

p = Producer(producer_conf)
send_ts = {}

def delivery_report(err, msg):
    """
    Callback quando un messaggio è stato consegnato al broker.
    Scrive una riga JSONL con:
    - key
    - run_id
    - topic
    - partition
    - broker_id
    - message
    - message_size_bytes
    - send_timestamp_ms
    - status
    - send_batch_id
    """
    msg_key = msg.key().decode("utf-8")
    send_timestamp = send_ts.pop(msg_key, None)
    status = "OK" if err is None else f"ERROR:{err}"

    full_payload_bytes = msg.value()  # bytes
    full_payload = json.loads(full_payload_bytes.decode("utf-8"))
    message_text = full_payload.get("message")
    send_batch_id = full_payload.get("send_batch_id")
    run_id_from_payload = full_payload.get("run_id")

    # dimensione totale del messaggio inviato al broker (key + value)
    key_bytes = msg.key() or b""
    message_size = len(key_bytes) + len(full_payload_bytes)
    
    # partition e broker_id
    partition = msg.partition()
    broker_id = partition_to_broker.get(partition) if partition_to_broker else None
    

    record = {
        "key": msg_key,
        "run_id": run_id_from_payload,
        "topic": msg.topic(),
        "partition": partition,
        "broker_id": broker_id,
        "message": message_text,
        "message_size_bytes": message_size,
        "send_timestamp_ms": send_timestamp,
        "status": status,
        "send_batch_id": send_batch_id,
    }

    delivery_records.append(record)
    
    # Traccia per correlation analysis
    if partition_to_broker and broker_id and send_batch_id:
        batch_broker_key = f"{send_batch_id}_{broker_id}"
        
        # Se è il primo messaggio di questo batch verso questo broker
        if batch_broker_key not in batch_broker_first_delivery:
            time_ms = time.time_ns() / 1_000_000
            batch_broker_first_delivery[batch_broker_key] = {
                'timestamp_ms': time_ms,  # timestamp del primo delivery in ms
                'partition': partition,
                'message_size': message_size,
                'batch_id': send_batch_id,
                'broker_id': broker_id
            }
            
            # Registra per correlation analysis
            record_batch_delivery(broker_id, send_batch_id, partition, time_ms, message_size)

def record_batch_delivery(broker_id: int, batch_id: int, partition: int, 
                          timestamp_ms: float, message_size: float):
    """
    Registra il momento di primo delivery di un batch verso un broker.
    Questa funzione viene chiamata dal delivery_report quando il primo 
    messaggio di un batch raggiunge un broker specifico.
    """
    if broker_id not in broker_tracking:
        return
    
    # Calcola Sb (batch size in bytes)
    Sm = message_size  # Dimensione del messaggio serializzato
    Sb = (Sm * nb_messages_per_batch + BATCH_HEADER_SIZE) / COMPRESSION_RATIO
    
    # P = numero di partizioni gestite da questo broker (partizioni leader)
    P = broker_tracking[broker_id]['partition_count']
    
    # Inizializza contatori per questo broker
    if broker_id not in batches_sent_since_last_record:
        batches_sent_since_last_record[broker_id] = 0
        last_global_send_per_broker[broker_id] = None
        
    batches_sent_since_last_record[broker_id] += 1

    # Registra Gs solo dopo aver inviato N = partition_count totali batch
    # Questo garantisce un "ciclo completo"
    if batches_sent_since_last_record[broker_id] >= P:
        # Calcola Gs: intervallo dall'ultimo gruppo completo
        Gs = None
        
        if last_global_send_per_broker[broker_id] is not None:
            Gs = timestamp_ms - last_global_send_per_broker[broker_id]
        
        # Aggiorna timestamp e reset contatore
        last_global_send_per_broker[broker_id] = timestamp_ms
        batches_sent_since_last_record[broker_id] = 0
        
        # Salva i dati solo dopo aver completato il gruppo
        if Gs is not None:
            batch_tracking_data.append({
                'broker_id': broker_id,
                'partition': partition,
                'batch_id': batch_id,
                'timestamp_ms': timestamp_ms,
                'Sm': round(Sm, 2),
                'Nb': nb_messages_per_batch,
                'Sb': round(Sb, 2),
                'P': P,
                'Sr': None,  # Calcolato dopo da update_stats_with_jmx
                'Gs': round(Gs, 2)  
            })
        
def write_all_producer_records():
    """Scrive tutti i record del producer alla fine dell'esperimento"""
    with jsonl_path.open("w", encoding="utf-8") as f:
        for record in delivery_records:
            f.write(json.dumps(record) + "\n")

def write_unified_stats():
    """
    Aggiorna il file stats con batch_metrics
    """
    
    if not batch_tracking_data:
        return
    
    # Estrae liste di Sm, Sb, Gs
    Sm_values = [item['Sm'] for item in batch_tracking_data]
    Sb_values = [item['Sb'] for item in batch_tracking_data]
    Gs_values = [item['Gs'] for item in batch_tracking_data]
    
    # Costanti 
    Nb = batch_tracking_data[0]['Nb']
    P = batch_tracking_data[0]['P']
    
    # Converte in numpy arrays
    Sm_array = np.array(Sm_values)
    Sb_array = np.array(Sb_values)
    Gs_array = np.array(Gs_values)
    
    # Calcola percentili P5 e P95 per Gs
    p5_gs, p95_gs = np.percentile(Gs_array, [5, 95])
    mask = (Gs_array >= p5_gs) & (Gs_array <= p95_gs)
    
    # Applica filtro
    Sm_filtered = Sm_array[mask]
    Sb_filtered = Sb_array[mask]
    Gs_filtered = Gs_array[mask]
    
    # Calcola batch_count per ogni broker
    broker_batch_counts = {}
    for item in batch_tracking_data:
        broker_id = item['broker_id']
        broker_batch_counts[broker_id] = broker_batch_counts.get(broker_id, 0) + 1
    
    # Carica stats esistenti
    stats_path = stats_dir / f"{producer_name}_{run_id}_stats.json"
    
    if stats_path.exists():
        with stats_path.open('r') as f:
            stats = json.load(f)
    else:
        # Fallback se stats non esiste ancora (non dovrebbe succedere)
        stats = {
            "run_id": run_id,
            "producer_name": producer_name
        }
    
    # Aggiungi experiment_config
    stats['experiment_config'] = {
        'partition_count': partition_count,
        'nb_messages_per_batch': nb_messages_per_batch,
        'batch_header_size_bytes': BATCH_HEADER_SIZE,
        'compression_ratio': COMPRESSION_RATIO,
        'partition_to_broker': partition_to_broker,
        'lambda_rate': lambda_rate,
        'linger_ms': linger_ms,
    }
    
    # Aggiungi broker_tracking_summary
    stats['broker_tracking_summary'] = {
        str(broker_id): {
            'batch_count': broker_batch_counts.get(broker_id, 0),
            'partition_count': info['partition_count']
        }
        for broker_id, info in broker_tracking.items()
    }
    
    # Aggiungi batch_metrics
    stats['batch_metrics'] = {
        'Nb': int(Nb),
        'P': int(P),
        'Sm_mean_bytes': round(float(np.mean(Sm_filtered)), 2),
        'Sb_mean_bytes': round(float(np.mean(Sb_filtered)), 2),
        'Gs_mean_ms': round(float(np.mean(Gs_filtered)), 2),
        'Sr_mean_bytes': None  # Calcolato dopo da update_stats_with_jmx
    }
    
    # Riscrivi file stats
    with stats_path.open('w') as f:
        json.dump(stats, f, indent=2)


def send_message():
    """
    Chiede un messaggio al message_generator, aggiunge i campi Kafka e lo invia al broker
    """
    global current_batch_id, messages_in_current_batch

    if messages_in_current_batch == 0:
        current_batch_id += 1

    # 1) genera messaggio
    gen_msg = make_message()
    key = gen_msg["key"]
    message_text = gen_msg["message"]

    # 2) timestamp di invio dal punto di vista del producer
    send_timestamp = time.time_ns() / 1_000_000
    send_ts[key] = send_timestamp

    message = {
        "key": key,
        "run_id": run_id,
        "topic": topic,
        "message": message_text,
        "send_timestamp_ms": send_timestamp, # timestamp (in ms) generato dal producer nell'istante esatto in cui il messaggio viene creato e “messo in coda” per l’invio
        "status": "PENDING",
        "send_batch_id": current_batch_id,
    }

    p.produce(
        topic=topic,
        key=key,
        value=json.dumps(message).encode("utf-8"),
        callback=delivery_report,
    )
    p.poll(0)

    messages_in_current_batch += 1
    if messages_in_current_batch >= nb_messages_per_batch:
        messages_in_current_batch = 0 # resetto il contatore dei messaggi nel batch


def main():
    target_messages = int(os.getenv("TARGET_MESSAGES")) # numero totale di messaggi da inviare
    experiment_duration = int(os.getenv("EXPERIMENT_DURATION")) # durata massima dell'esperimento in secondi
    
    time.sleep(60)  # attendo che tutti i consumer siano pronti
    
    transmission_start = time.time()
    end_time = transmission_start + experiment_duration
    
    try:
        messages_sent = 0
        while (messages_sent < target_messages and time.time() < end_time) or messages_in_current_batch > 0:
            if messages_sent < target_messages and time.time() < end_time:
                poisson_sleep()  # attende un intervallo casuale per simulare arrivi Poisson
                send_message()
                messages_sent += 1
            elif messages_in_current_batch > 0:
                # Completa il batch corrente anche se tempo/target sono raggiunti
                send_message()
                messages_sent += 1
            else:
                break  
    finally:
        p.flush()
        transmission_end = time.time()
        
        # Invio segnale di fine trasmissione cosi che i consumer possano terminare
        for partition in range(partition_count):
            end_signal = json.dumps({
                "type": "END_SIGNAL",
                "producer": producer_name,
                "timestamp": time.time()
            })
            p.produce(topic, end_signal.encode('utf-8'), partition=partition)
        p.flush()
        
        time.sleep(2)
        
        write_all_producer_records()
        
        total_time = transmission_end - transmission_start
        num_messages = len(delivery_records)
        total_payload_bytes = sum(r['message_size_bytes'] for r in delivery_records)
        unique_batches = len(set(r['send_batch_id'] for r in delivery_records))
        
        stats = {
            "run_id": run_id,
            "producer_name": producer_name,
            "topic": topic,  
            
            "transmission_stats": {  
                "num_messages": num_messages,
                "num_batches": unique_batches,
                "payload_bytes": total_payload_bytes,
                "transmission_time_seconds": round(total_time, 2)
            },
            
            "kafka_metrics": {  
                "kafka_overhead_bytes": None,
                "kafka_total_bytes": None,
                "throughput_kbps": None,
                "overhead_percentage": None,
                "packet_header_size_bytes": None
            }
        }
        
        stats_path = stats_dir / f"{producer_name}_{run_id}_stats.json"
        with stats_path.open('w') as f:
            json.dump(stats, f, indent=2)
            
        write_unified_stats() # aggiorna batch_metrics nel file stats
    
    while True:
        time.sleep(60) # mantiene il container attivo


if __name__ == "__main__":
    main()
