import json
import os
import sys
import time
import socket
from pathlib import Path
from confluent_kafka import Consumer

bootstrap = os.getenv("BOOTSTRAP_SERVERS") # Kafka bootstrap servers
topic = os.getenv("TOPIC_NAME") # Topic to read from
run_id = os.getenv("RUN_ID") # unique identifier for this experiment run
group_id = os.getenv("GROUP_ID") # Kafka consumer group id
container_name = os.getenv("CONTAINER_NAME", socket.gethostname()) # container name (default to hostname if not set)

# Create data directory if it doesn't exist
data_dir = Path("./data") 
data_dir.mkdir(parents=True, exist_ok=True)

# <CONTAINER_NAME>_<RUN_ID>_messages.jsonl
out_file = data_dir / f"{container_name}_{run_id}_messages_recap.jsonl"

# Kafka consumer configuration
consumer_conf = {
    "bootstrap.servers": bootstrap,
    "group.id": group_id,
    "auto.offset.reset": "earliest",
}

c = Consumer(consumer_conf)
c.subscribe([topic]) # Subscribe to the topic
message_records = []

print(f"Subscribed to topic: {topic}") 
sys.stdout.flush()

def process_message(msg):
    """
    Extracts data from the Kafka message and writes a JSONL row to the output file.
    Extracted fields:
    - key
    - run_id
    - topic
    - partition
    - offset
    - message (from the payload)
    - receive_timestamp_ms (time of consumption)
    - producer_send_timestamp_ms (timestamp sent by the producer)
    """

    receive_ts = time.time_ns() / 1_000_000  # Convert to milliseconds

    key = msg.key().decode("utf-8") if msg.key() else None # Decode the key if it exists, otherwise set to None
    raw_value = msg.value().decode("utf-8") if msg.value() else None # Decode the value if it exists, otherwise set to None

    # Try to parse the value as JSON, if it fails, use an empty dict
    try:
        payload = json.loads(raw_value) if raw_value is not None else {}
    except json.JSONDecodeError:
        payload = {}

    message_text = payload.get("message") # Extract the "message" field from the payload
    producer_send_timestamp = payload.get("send_timestamp_ms") # Extract the "send_timestamp_ms" field from the payload, which is the timestamp sent by the producer
    run_id_payload = payload.get("run_id") # Extract the "run_id" field from the payload, which is the run_id sent by the producer (if it exists)

    # Create a record with all the extracted information
    record = {
        "key": key,
        "run_id": run_id_payload,
        "topic": msg.topic(),
        "partition": msg.partition(),
        "offset": msg.offset(),
        "message": message_text,
        "receive_timestamp_ms": receive_ts,
        "producer_send_timestamp_ms": producer_send_timestamp,
    }

    message_records.append(record)
    
def write_all_records():
    """Writes all records at the end of the experiment."""
    with out_file.open("w", encoding="utf-8") as f:
        for record in message_records:
            f.write(json.dumps(record) + "\n")

def main():
    
    experiment_duration = int(os.getenv("EXPERIMENT_DURATION")) # seconds to run the experiment before shutting down, starting from the first message received
    
    buffer_time = 30  # seconds to wait after receiving the last END_SIGNAL before shutting down, to ensure all messages are processed
    
    # counters and timers
    messages_received = 0
    start_time = None
    end_time = None
    
    # Set to track which producers have sent the END_SIGNAL
    producers_finished = set()
    expected_producers = {"producer_1", "producer_2"}
    
    try: 
        while True:
            
            # If we have received the first message and the end time is set, check if we should stop consuming
            if end_time is not None and time.time() >= end_time:
                break
            
            msg = c.poll(1.0) # Poll for messages with a timeout of 1 second
            
            if msg is None:
                continue
            
            if msg.error():
                continue
            
            # Set the start time and end time when we receive the first message
            if start_time is None:
                start_time = time.time()
                end_time = start_time + experiment_duration + buffer_time
                
            try:
                data = json.loads(msg.value().decode('utf-8')) # Try to decode the message value as JSON
                
                # Check if the message is an END_SIGNAL from a producer
                if data.get("type") == "END_SIGNAL":
                    producer_name = data.get("producer")
                    producers_finished.add(producer_name)
                    
                    # If we have received END_SIGNAL from all expected producers, we can set the end time to be sooner (current time + buffer_time) to finish the experiment earlier
                    if producers_finished >= expected_producers:
                        end_time = time.time() + buffer_time
                    continue  # Skip processing this message as a normal message since it's an END_SIGNAL
                    
            except (json.JSONDecodeError, UnicodeDecodeError, KeyError):
                pass  # If the message value is not valid JSON or doesn't have the expected fields, we just ignore the error and process it as a normal message
            
            process_message(msg)
            messages_received += 1
    finally:
        # Write all records to the output file at the end of the experiment
        write_all_records()
        c.close()
    while True:
        # Keep the container alive for a while after finishing the experiment to allow time for logs to be collected and for any manual inspection if needed
        time.sleep(60)

if __name__ == "__main__":
    main()
