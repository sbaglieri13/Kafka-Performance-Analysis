import time
import json
from confluent_kafka import Consumer
import os

bootstrap = os.getenv("BOOTSTRAP_SERVERS", "localhost:9092")  # Kafka bootstrap servers
consumer_conf = {
    "bootstrap.servers": bootstrap, # Kafka bootstrap servers
    "group.id": "latency-test-group", # ID del gruppo consumer
    "auto.offset.reset": "earliest", # inizia a leggere dal primo offset se non ci sono offset salvati per questo gruppo
}
out_file = "/app/data/consumer_metrics.csv"
topic = os.getenv("TOPIC_NAME", "test-topic-3") # Kafka topic da cui il consumer legge i messaggi

c = Consumer(consumer_conf)
c.subscribe([topic]) # iscrive il consumer al topic

def main():
    with open(out_file, "a", encoding="utf-8") as f:
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
                "key": key,
                "receive_ts_ns": receive_ts,
                "topic": msg.topic(),
                "partition": msg.partition(),
                "offset": msg.offset(),
                "value": value,
            }
            f.write(json.dumps(record) + "\n")


if __name__ == "__main__":
    try:
        main()
    finally:
        c.close()
