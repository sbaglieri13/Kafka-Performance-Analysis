import os
import time
import json
import socket
from pathlib import Path
import requests
from confluent_kafka import Producer


MESSAGE_GENERATOR_URL = os.getenv("MESSAGE_GENERATOR_URL")

bootstrap = os.getenv("BOOTSTRAP_SERVERS")
topic = os.getenv("TOPIC_NAME")
run_id = os.getenv("RUN_ID")
payload_size_bytes = int(os.getenv("PAYLOAD_SIZE_BYTES"))
nb_messages_per_batch = int(os.getenv("NB_MESSAGES_PER_BATCH"))
producer_name = os.getenv("CONTAINER_NAME", socket.gethostname())
partition_count = int(os.getenv("PARTITION_COUNT"))

current_batch_id = 0  # serve per tracciare l'ID del batch corrente cosi da testare se effettivamente in un batch ci sono i messaggi attesi
messages_in_current_batch = 0
delivery_records = []

data_dir = Path("/app/data")
data_dir.mkdir(parents=True, exist_ok=True)

stats_dir = Path("/app/data/stats")
stats_dir.mkdir(parents=True, exist_ok=True)

# File per salvare i record dei messaggi inviati
jsonl_path = data_dir / f"{producer_name}_{run_id}_messages_recap.jsonl"

# stima preliminare della dimensione messaggio
def estimate_message_size() -> int:
    tmp_key = "0"
    tmp_payload = {
        "key": tmp_key,
        "run_id": run_id,
        "topic": topic,
        "message": "x" * payload_size_bytes,
        "send_timestamp_ms": 0.0,
        "write_timestamp_ms": None,
        "status": "PENDING",
        "send_batch_id": 0,
    }
    value_bytes = json.dumps(tmp_payload).encode("utf-8")
    return len(tmp_key.encode("utf-8")) + len(value_bytes)


estimated_msg_size = estimate_message_size()
batch_size_bytes = int(estimated_msg_size * nb_messages_per_batch * 1.2) # margine del 20% per sicurezza

producer_conf = {
    "bootstrap.servers": bootstrap,
    "linger.ms": 100,  # imposto linger.ms a 100 ms per testare meglio il batching ed essere sicuro che venga riempito il batch
    "batch.size": batch_size_bytes,
    #"enable.idempotence": True,
}

p = Producer(producer_conf)
send_ts = {}


def fetch_generated_message() -> dict:
    """
    Chiede un nuovo messaggio al message_generator
    La risposta attesa è:
    {
        "key": "...",
        "run_id": "...",
        "message": "...",
        "generator_timestamp_ms": ...
    }
    """
    resp = requests.get(MESSAGE_GENERATOR_URL, timeout=5)
    resp.raise_for_status()
    return resp.json()


def delivery_report(err, msg):
    """
    Callback quando un messaggio è stato consegnato al broker.
    Scrive una riga JSONL con:
    - key
    - run_id
    - topic
    - message
    - message_size_bytes
    - send_timestamp_ms
    - write_timestamp_ms
    - status
    - send_batch_id
    """
    msg_key = msg.key().decode("utf-8")
    send_timestamp = send_ts.pop(msg_key, None)
    write_timestamp = time.time_ns() / 1_000_000  # converto in millisecondi
    status = "OK" if err is None else f"ERROR:{err}"

    full_payload_bytes = msg.value()  # bytes
    full_payload = json.loads(full_payload_bytes.decode("utf-8"))
    message_text = full_payload.get("message")
    send_batch_id = full_payload.get("send_batch_id")
    run_id_from_payload = full_payload.get("run_id")

    # dimensione totale del messaggio inviato al broker (key + value)
    key_bytes = msg.key() or b""
    message_size = len(key_bytes) + len(full_payload_bytes)

    record = {
        "key": msg_key,
        "run_id": run_id_from_payload,
        "topic": msg.topic(),
        "message": message_text,
        "message_size_bytes": message_size,
        "send_timestamp_ms": send_timestamp,
        "write_timestamp_ms": write_timestamp,
        "status": status,
        "send_batch_id": send_batch_id,
    }

    delivery_records.append(record)


def write_all_producer_records():
    """Scrive tutti i record del producer alla fine dell'esperimento"""
    with jsonl_path.open("w", encoding="utf-8") as f:
        for record in delivery_records:
            f.write(json.dumps(record) + "\n")


def send_message():
    """
    Chiede un messaggio al message_generator, aggiunge i campi Kafka e lo invia al broker
    """
    global current_batch_id, messages_in_current_batch

    if messages_in_current_batch == 0:
        current_batch_id += 1

    # 1) prendo il messaggio dal generator
    gen_msg = fetch_generated_message()
    key = gen_msg["key"]
    run_id_from_gen = gen_msg["run_id"]
    message_text = gen_msg["message"]

    # 2) timestamp di invio dal punto di vista del producer
    send_timestamp = time.time_ns() / 1_000_000
    send_ts[key] = send_timestamp

    message = {
        "key": key,
        "run_id": run_id_from_gen,
        "topic": topic,
        "message": message_text,
        "send_timestamp_ms": send_timestamp, # timestamp (in ms) generato dal producer nell'istante esatto in cui il messaggio viene creato e “messo in coda” per l’invio
        "write_timestamp_ms": None,
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
        messages_in_current_batch = 0


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
            "batch_size": nb_messages_per_batch,
            "num_messages": num_messages,
            "num_batches": unique_batches,
            "payload_bytes": total_payload_bytes,
            "transmission_time_seconds": round(total_time, 2),
            # Overhead sarà aggiunto dopo da script esterno
            "kafka_overhead_bytes": None,
            "kafka_total_bytes": None,
            "throughput_kbps": None,
            "overhead_percentage": None
        }
        
        stats_path = stats_dir / f"{producer_name}_{run_id}_stats.json"
        with stats_path.open('w') as f:
            json.dump(stats, f, indent=2)
    
    while True:
        time.sleep(60) # mantiene il container attivo


if __name__ == "__main__":
    main()
