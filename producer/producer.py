import os
import time
import json
from pathlib import Path
from confluent_kafka import Producer

bootstrap = os.getenv("BOOTSTRAP_SERVERS") # Kafka bootstrap servers
topic = os.getenv("TOPIC_NAME") # Kafka topic dove il producer invia i messaggi
run_id = os.getenv("RUN_ID")
payload_size_bytes = int(os.getenv("PAYLOAD_SIZE_BYTES"))  # dimensione del payload di ogni messaggio in byte
partition_count = int(os.getenv("PARTITION_COUNT")) # numero di partizioni del topic
out_file = Path("/app/data") / f"producer_metrics_{run_id}.jsonl"

producer_conf = {
    "bootstrap.servers": bootstrap, # Kafka bootstrap servers
    "linger.ms": 10, # tempo massimo (in millisecondi) che il producer aspetta per accumulare messaggi in un batch prima di inviarli
    "batch.size": 32768, # dimensione massima (in byte) del batch in memoria per ogni partizione -- 32 KB
    "enable.idempotence": True, # abilita l’idempotenza del producer (con sequence number interno), così in caso di retry non ottieni duplicati sul topic, garantisce “exactly‑once” lato broker
}

p = Producer(producer_conf)
send_ts = {}

def make_payload(msg_id: int) -> dict:
    """
    Costruisce un payload di dimensione controllata ~ payload_size_bytes
    """
    base = f"msg-{msg_id}-"
    # riempi con 'x' fino a raggiungere (approssimativamente) la dimensione richiesta
    padding_len = max(payload_size_bytes - len(base), 0)
    data_str = base + ("x" * padding_len)
    return {"id": msg_id, "data": data_str}

# Callback function chiamata quando un messaggio è stato consegnato (o si è verificato un errore)
def delivery_report(err, msg):
    msg_key = msg.key().decode("utf-8")
    sent_at = send_ts.pop(msg_key, None) # timestamp di invio del messaggio
    written_at = time.time_ns() # timestamp di scrittura del messaggio sul broker
    status = "OK" if err is None else f"ERROR:{err}"

    record = {
        "run_id": run_id,
        "topic": msg.topic(),
        "partition": msg.partition(),
        "offset": msg.offset(),
        "key": msg_key,
        "produce_call_ts_ns": sent_at,
        "written_ts_ns": written_at,
        "status": status,
        "payload_size_bytes": payload_size_bytes,
        "partition_count": partition_count,
    }
    with out_file.open("a", encoding="utf-8") as f:
        f.write(json.dumps(record) + "\n")

# Funzione per inviare un messaggio al topic Kafka
def send_message(msg_id: int, payload: dict):
    key = str(msg_id)
    value = json.dumps(payload)
    now = time.time_ns()
    send_ts[key] = now
    p.produce(topic=topic, key=key, value=value.encode("utf-8"), callback=delivery_report) # invia il messaggio in modo asincrono
    p.poll(0) # chiama le callback pendenti (come delivery_report)

# Generatore di messaggi che invia un messaggio ogni secondo
def message_generator():
    msg_id = 0
    while True:
        msg_id += 1
        payload = make_payload(msg_id)
        send_message(msg_id, payload)
        time.sleep(0.001)

def main():
    message_generator()

if __name__ == "__main__":
    main()
