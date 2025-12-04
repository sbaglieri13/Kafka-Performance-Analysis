import os
import time
import json
import socket
from pathlib import Path

from confluent_kafka import Producer

bootstrap = os.getenv("BOOTSTRAP_SERVERS")
topic = os.getenv("TOPIC_NAME")
run_id = os.getenv("RUN_ID")
payload_size_bytes = int(os.getenv("PAYLOAD_SIZE_BYTES"))
nb_messages_per_batch = int(os.getenv("NB_MESSAGES_PER_BATCH"))
producer_name = os.getenv("CONTAINER_NAME", socket.gethostname())

current_batch_id = 0 # serve per tracciare l'ID del batch corrente cosi da testare se effettivamente in un batch ci sono i messaggi attesi
messages_in_current_batch = 0

data_dir = Path("/app/data")
data_dir.mkdir(parents=True, exist_ok=True)

# File per salvare i record dei messaggi inviati
jsonl_path = data_dir / f"{producer_name}_{run_id}_messages.jsonl"

def make_message_string() -> str:
    '''Crea una stringa di lunghezza esatta payload_size_bytes'''
    base = "xxxx"
    padding_len = max(payload_size_bytes - len(base), 0)
    # sempre base + tante 'x' (tagliate se serve)
    s = base + ("x" * padding_len)
    return s[:payload_size_bytes]

# stima preliminare della dimensione messaggio
def estimate_message_size() -> int:
    tmp_key = "0"
    tmp_message_text = make_message_string()
    tmp_payload = {
        "key": tmp_key,
        "run_id": producer_name+"_"+run_id,
        "topic": topic,
        "message": tmp_message_text,
        "send_timestamp_ms": 0,
        "write_timestamp_ms": None,
        "status": "PENDING",
    }
    value_bytes = json.dumps(tmp_payload).encode("utf-8")
    return len(tmp_key.encode("utf-8")) + len(value_bytes)

estimated_msg_size = estimate_message_size()
batch_size_bytes = int(estimated_msg_size * nb_messages_per_batch * 1.2) # margine del 20% per sicurezza

producer_conf = {
    "bootstrap.servers": bootstrap,
    "linger.ms": 100, # imposto linger.ms a 100 ms per testare meglio il batching ed assiumere che venga riempito il batch
    "batch.size": batch_size_bytes,
    "enable.idempotence": True,
}

p = Producer(producer_conf)
send_ts = {}


def delivery_report(err, msg):
    msg_key = msg.key().decode("utf-8")
    send_timestamp = send_ts.pop(msg_key, None)
    write_timestamp = time.time_ns() / 1_000_000 # converto in millisecondi
    status = "OK" if err is None else f"ERROR:{err}"

    full_payload_bytes = msg.value()              # bytes
    full_payload = json.loads(full_payload_bytes.decode("utf-8"))
    message_text = full_payload.get("message")
    send_batch_id = full_payload.get("send_batch_id")

    # dimensione totale del messaggio inviato al broker (key + value)
    key_bytes = msg.key() or b""
    message_size = len(key_bytes) + len(full_payload_bytes)

    record = {
        "key": msg_key,
        "run_id": producer_name+"_"+run_id,
        "topic": msg.topic(),
        "message": message_text,
        "message_size_bytes": message_size,
        "send_timestamp_ms": send_timestamp,
        "write_timestamp_ms": write_timestamp,
        "status": status,
        "send_batch_id": send_batch_id,
    }

    with jsonl_path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(record) + "\n")




def send_message(msg_id: int):
    global current_batch_id, messages_in_current_batch

    if messages_in_current_batch == 0:
        current_batch_id += 1

    key = str(msg_id)
    message_text = make_message_string()

    send_timestamp = time.time_ns() / 1_000_000 # converto in millisecondi
    send_ts[key] = send_timestamp

    message = {
        "key": key,
        "run_id": producer_name+"_"+run_id,
        "topic": topic,
        "message": message_text,
        "send_timestamp_ms": send_timestamp, # timestamp (in nanosecondi) generato dal producer nell'istante esatto in cui il messaggio viene creato e “messo in coda” per l’invio
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


def message_generator():
    msg_id = 0
    while True:
        msg_id += 1
        send_message(msg_id)
        time.sleep(0.001)


def main():
    # attendo 20 secondi prima di iniziare a produrre
    time.sleep(20)

    try:
        message_generator()
    finally:
        p.flush()


if __name__ == "__main__":
    main()
