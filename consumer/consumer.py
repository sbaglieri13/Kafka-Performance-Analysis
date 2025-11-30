import time
import json
from pathlib import Path
import os
from confluent_kafka import Consumer

bootstrap = os.getenv("BOOTSTRAP_SERVERS")  # Kafka bootstrap servers
topic = os.getenv("TOPIC_NAME") # Kafka topic da cui il consumer legge i messaggi
run_id = os.getenv("RUN_ID")
group_id = os.getenv("GROUP_ID")
role = os.getenv("CLIENT_ROLE")
out_file = Path("/app/data") / f"consumer_metrics_{role}_{run_id}.jsonl"

consumer_conf = {
    "bootstrap.servers": bootstrap, # Kafka bootstrap servers
    "group.id": group_id, # ID del gruppo consumer
    "auto.offset.reset": "earliest", # inizia a leggere dal primo offset se non ci sono offset salvati per questo gruppo
}

c = Consumer(consumer_conf)
c.subscribe([topic]) # iscrive il consumer al topic

def main():
    with out_file.open("a", encoding="utf-8") as f:
        while True:
            msg = c.poll(1.0) # attende fino a 1 secondo per un messaggio
            # se entro 1 secondo non arriva nessun messaggio, msg sarà None, altrimenti sarà un oggetto Message
            if msg is None:
                continue
            if msg.error():
                continue

            receive_ts = time.time_ns() # timestamp di ricezione del messaggio
            key = msg.key().decode("utf-8") if msg.key() else None # decodifica la chiave del messaggio
            value = msg.value().decode("utf-8") if msg.value() else None # decodifica il valore del messaggio

            # crea un record con le informazioni del messaggio e il timestamp di ricezione
            record = {
                "run_id": run_id,
                "topic": msg.topic(),
                "partition": msg.partition(),
                "offset": msg.offset(),
                "key": key,
                "receive_ts_ns": receive_ts,
                "value": value,
            }
            f.write(json.dumps(record) + "\n")


if __name__ == "__main__":
    try:
        main()
    finally:
        c.close()
