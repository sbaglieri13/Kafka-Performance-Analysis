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

messages_per_batch = int(os.getenv("MESSAGES_PER_BATCH", "1"))
linger_ms = int(os.getenv("LINGER_MS", "0")) # tempo di attesa prima di inviare un batch in millisecondi
batch_size_bytes = int(os.getenv("BATCH_SIZE_BYTES", "32768")) # dimensione del batch in byte

role = os.getenv("CLIENT_ROLE", "prod")
out_file = Path("/app/data") / f"producer_metrics_{role}_{run_id}.jsonl"


producer_conf = {
    "bootstrap.servers": bootstrap, # Kafka bootstrap servers
    "linger.ms": linger_ms, # tempo massimo (in millisecondi) che il producer aspetta per accumulare messaggi in un batch prima di inviarli
    "batch.size": batch_size_bytes, # dimensione massima (in byte) del batch in memoria per ogni partizione
    "enable.idempotence": True, # abilita l’idempotenza del producer (con sequence number interno), così in caso di retry non ottieni duplicati sul topic, garantisce “exactly‑once” lato broker
}

p = Producer(producer_conf)
send_ts = {}
last_send_ts = None


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
    sent_at = send_ts.pop(msg_key, None)
    written_at = time.time_ns()
    status = "OK" if err is None else f"ERROR:{err}"

    delta_ms = None
    if msg.headers():
        for k, v in msg.headers():
            if k == "delta_produce_ms" and v is not None:
                delta_ms = float(v.decode("utf-8"))
                break

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
        "messages_per_batch": messages_per_batch,
        "linger_ms": linger_ms,
        "batch_size_bytes": batch_size_bytes,
        "delta_produce_ms": delta_ms,
    }
    with out_file.open("a", encoding="utf-8") as f:
        f.write(json.dumps(record) + "\n")


# Invia un messaggio al topic
def send_message(msg_id: int, payload: dict):
    global last_send_ts

    key = str(msg_id)
    value = json.dumps(payload)

    now = time.time_ns()
    send_ts[key] = now

    # intervallo dal precedente invio (ms)
    delta_ms = None
    if last_send_ts is not None:
        delta_ms = (now - last_send_ts) / 1e6
    last_send_ts = now

    headers = []
    if delta_ms is not None:
        headers.append(("delta_produce_ms", str(delta_ms).encode("utf-8")))

    p.produce(
        topic=topic,
        key=key,
        value=value.encode("utf-8"),
        headers=headers,
        callback=delivery_report,
    )
    p.poll(0) # chiama le callback di consegna dei messaggi in sospeso

# Generatore di messaggi
def message_generator():
    msg_id = 0
    while True:
        for _ in range(messages_per_batch):
            msg_id += 1
            payload = make_payload(msg_id)
            send_message(msg_id, payload)



def main():
    message_generator()

if __name__ == "__main__":
    main()
