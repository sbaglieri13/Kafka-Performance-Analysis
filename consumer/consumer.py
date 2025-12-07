import json
import os
import sys
import time
import socket
from pathlib import Path
from confluent_kafka import Consumer

bootstrap = os.getenv("BOOTSTRAP_SERVERS")      # Kafka bootstrap servers
topic = os.getenv("TOPIC_NAME")                 # Topic da cui leggere
run_id = os.getenv("RUN_ID")
group_id = os.getenv("GROUP_ID")
container_name = os.getenv("CONTAINER_NAME", socket.gethostname())

data_dir = Path("/app/data")
data_dir.mkdir(parents=True, exist_ok=True)

# <CONTAINER_NAME>_<RUN_ID>_messages.jsonl
out_file = data_dir / f"{container_name}_{run_id}_messages.jsonl"

# Configurazione del consumer Kafka
consumer_conf = {
    "bootstrap.servers": bootstrap,
    "group.id": group_id,
    "auto.offset.reset": "earliest",
}

c = Consumer(consumer_conf)
c.subscribe([topic])

print(f"Subscribed to topic: {topic}") 
sys.stdout.flush()

def process_message(msg):
    """
    Estrae i dati dal messaggio Kafka e scrive una riga JSONL nel file di output
    Campi estratti:
    - key
    - run_id
    - topic 
    - partition 
    - offset
    - message (dal payload)
    - receive_timestamp_ms (momento di lettura)
    - latency (differenza tra receive_timestamp_ms e send_timestamp_ms dal payload)
    """

    receive_ts = time.time_ns() / 1_000_000  # converto in millisecondi

    key = msg.key().decode("utf-8") if msg.key() else None
    raw_value = msg.value().decode("utf-8") if msg.value() else None

    try:
        payload = json.loads(raw_value) if raw_value is not None else {}
    except json.JSONDecodeError:
        payload = {}

    message_text = payload.get("message")
    send_timestamp_ms = payload.get("send_timestamp_ms")
    run_id_payload = payload.get("run_id")

    delta_receive_minus_send = (
        receive_ts - send_timestamp_ms
        if isinstance(send_timestamp_ms, (int, float))
        else None
    )

    record = {
        "key": key,
        "run_id": run_id_payload,
        "topic": msg.topic(),
        "partition": msg.partition(),
        "offset": msg.offset(),
        "message": message_text,
        "receive_timestamp_ms": receive_ts,
        "latency_ms": delta_receive_minus_send,
    }

    with out_file.open("a", encoding="utf-8") as f:
        f.write(json.dumps(record) + "\n")


def main():
    try:
        while True:
            msg = c.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                continue
            process_message(msg)
    finally:
        c.close()


if __name__ == "__main__":
    main()
