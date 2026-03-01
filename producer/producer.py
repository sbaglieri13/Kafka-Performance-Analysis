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

bootstrap = os.getenv("BOOTSTRAP_SERVERS") # kafka bootstrap servers
topic = os.getenv("TOPIC_NAME") # topic name
run_id = os.getenv("RUN_ID") # unique identifier
payload_size_bytes = int(os.getenv("PAYLOAD_SIZE_BYTES")) # payload size 
nb_messages_per_batch = int(os.getenv("NB_MESSAGES_PER_BATCH")) # number of messages per batch (N)
producer_name = os.getenv("CONTAINER_NAME", socket.gethostname()) # producer name if provided, otherwise use hostname
partition_count = int(os.getenv("PARTITION_COUNT")) # number of partitions in the topic
lambda_rate = float(os.getenv("LAMBDA_RATE")) # poison arrival rate (messages per second)
send_mode = os.getenv("SEND_MODE") # mode of sending messages: "poisson" or "fixed_rate"

BATCH_HEADER_SIZE = 61  # bytes, fixed Kafka format
COMPRESSION_RATIO = 1.0  # no compression

key_counter = itertools.count(start=1) # counter for generating unique incremental keys for messages
def make_message() -> dict:
    """Generates the payload and an incremental key."""
    key     = str(next(key_counter)) # incremental key as string
    message = ''.join(random.choices(string.ascii_lowercase, k=payload_size_bytes)) # random string of specified byte size

# Function to simulate Poisson arrivals
def poisson_sleep():
    """Waits for an exponential inter-arrival time with mean 1/lambda_rate seconds to simulate a Poisson arrival process."""
    interval = np.random.exponential(1.0 / lambda_rate)
    time.sleep(interval)
    
linger_ms = 30000 # To avoid sending incomplete batches too early, while allowing sufficient time to complete batches even at high lambda values.

current_batch_id = 0  # Used to track the current batch ID in order to verify that each batch contains the expected number of messages.
messages_in_current_batch = 0 # Used to count how many messages have been sent in the current batch, so we know when to increment the batch ID.
delivery_records = [] 
batch_tracking_data = [] 

# Tracking per batch and per broker (to handle N partitions across M brokers)
batch_broker_first_delivery = {}  # Tracks the first delivery for each batch and broker combination

# Global variable used to track the last GLOBAL send event
last_global_send_per_broker = {}  # {broker_id: timestamp_ms}
batches_sent_since_last_record = {}  # {broker_id: counter}

# Create directories for data and stats if they don't exist
data_dir = Path("./data")
data_dir.mkdir(parents=True, exist_ok=True)

# Create directory for stats if it doesn't exist
stats_dir = Path("./data/stats")
stats_dir.mkdir(parents=True, exist_ok=True)

# File path for producer's JSONL recap of deliveries
jsonl_path = data_dir / f"{producer_name}_{run_id}_messages_recap.jsonl"

def get_partition_leader_mapping() -> Dict[int, int]:
    """
    Retrieves the Partition -> Broker Leader mapping using the Admin API.
    Returns: {partition_id: broker_id}
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


# Retrieve the partition mapping at producer startup
partition_to_broker = get_partition_leader_mapping()

# Tracking per broker: batch count and partition count 
broker_tracking = {
    broker_id: {
        'batch_count': 0,
        'partition_count': 0,  # Initialized to 0; it will be computed later
    }
    for broker_id in set(partition_to_broker.values()) if partition_to_broker
}

# Counts how many partitions each broker has
if partition_to_broker and broker_tracking:
    for partition_id, broker_id in partition_to_broker.items():
        broker_tracking[broker_id]['partition_count'] += 1
    
    print(f"\nBroker distribution:", flush=True)
    for broker_id, info in broker_tracking.items():
        print(f"  Broker {broker_id}: {info['partition_count']} partitions", flush=True)

# Preliminary estimate of the message size
def estimate_message_size() -> int:
    '''Estimates the size of a message in bytes'''
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


estimated_msg_size = estimate_message_size() # estimated size of a single message in bytes
batch_size_bytes = int(estimated_msg_size * nb_messages_per_batch * 1.2) # 20% safety margin

producer_conf = {
    "bootstrap.servers": bootstrap, # Kafka cluster address
    "linger.ms": linger_ms,  # Maximum time (in ms) to wait before sending an incomplete batch
    "batch.size": batch_size_bytes, # Maximum batch size in bytes (computed as estimated message size * nb_messages_per_batch * safety margin)
}

p = Producer(producer_conf)
send_ts = {}

def delivery_report(err, msg):
    """
    Callback triggered when a message has been delivered to the broker.
    Writes a JSONL row containing:
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

    # Total size of the message sent to the broker (key + value)
    key_bytes = msg.key() or b""
    message_size = len(key_bytes) + len(full_payload_bytes)
    
    # Partition and broker_id
    partition = msg.partition()
    broker_id = partition_to_broker.get(partition) if partition_to_broker else None
    
    # json record to be written in the producer's JSONL recap file
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
    
    if partition_to_broker and broker_id and send_batch_id:
        batch_broker_key = f"{send_batch_id}_{broker_id}"
        
        # If this is the first message of this batch sent to this broker
        if batch_broker_key not in batch_broker_first_delivery:
            time_ms = time.time_ns() / 1_000_000
            batch_broker_first_delivery[batch_broker_key] = {
                'timestamp_ms': time_ms,  # first delivery timestamp for this batch and broker
                'partition': partition,
                'message_size': message_size,
                'batch_id': send_batch_id,
                'broker_id': broker_id
            }
            
            record_batch_delivery(broker_id, send_batch_id, partition, time_ms, message_size)

def record_batch_delivery(broker_id: int, batch_id: int, partition: int, timestamp_ms: float, message_size: float):
    """
    Records the timestamp of the first delivery of a batch to a broker.
    This function is called by delivery_report when the first
    message of a batch reaches a specific broker.
    """
    if broker_id not in broker_tracking:
        return
    
    # # Computes Sb (batch size in bytes)
    Sm = message_size  # size of serialized message
    Sb = (Sm * nb_messages_per_batch + BATCH_HEADER_SIZE) / COMPRESSION_RATIO
    
    # P = number of partitions managed by this broker (leader partitions)
    P = broker_tracking[broker_id]['partition_count']
    
    # Initializes counters for this broker
    if broker_id not in batches_sent_since_last_record:
        batches_sent_since_last_record[broker_id] = 0
        last_global_send_per_broker[broker_id] = None
        
    batches_sent_since_last_record[broker_id] += 1

    # Record Gs only after sending N = partition_count total batches.
    # This ensures a "complete cycle."
    if batches_sent_since_last_record[broker_id] >= P:
        # Computes Gs: interval since the last complete group
        Gs = None
        
        if last_global_send_per_broker[broker_id] is not None:
            Gs = timestamp_ms - last_global_send_per_broker[broker_id]
        
        # Updates the timestamp and resets the counter
        last_global_send_per_broker[broker_id] = timestamp_ms
        batches_sent_since_last_record[broker_id] = 0
        
        # Save the data only after completing the group
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
                'Sr': None,  # calculated later from update_stats_with_jmx
                'Gs': round(Gs, 2)  
            })
        
def write_all_producer_records():
    """Writes all producer records at the end of the experiment."""
    with jsonl_path.open("w", encoding="utf-8") as f:
        for record in delivery_records:
            f.write(json.dumps(record) + "\n")

def write_unified_stats():
    """Updates the stats file with batch_metrics"""
    
    if not batch_tracking_data:
        return
    
    # Extract Sm, Sb, Gs values 
    Sm_values = [item['Sm'] for item in batch_tracking_data]
    Sb_values = [item['Sb'] for item in batch_tracking_data]
    Gs_values = [item['Gs'] for item in batch_tracking_data]
    
    # Constants 
    Nb = batch_tracking_data[0]['Nb']
    P = batch_tracking_data[0]['P']
    
    # Convert to numpy arrays 
    Sm_array = np.array(Sm_values)
    Sb_array = np.array(Sb_values)
    Gs_array = np.array(Gs_values)
    
    # Computes the P5 and P95 percentiles for Gs
    p5_gs, p95_gs = np.percentile(Gs_array, [5, 95])
    mask = (Gs_array >= p5_gs) & (Gs_array <= p95_gs)
    
    # Apply filter
    Sm_filtered = Sm_array[mask]
    Sb_filtered = Sb_array[mask]
    Gs_filtered = Gs_array[mask]
    
    # Calculate batch counts per broker
    broker_batch_counts = {}
    for item in batch_tracking_data:
        broker_id = item['broker_id']
        broker_batch_counts[broker_id] = broker_batch_counts.get(broker_id, 0) + 1
    
    # Upload the stats to the producer's stats file
    stats_path = stats_dir / f"{producer_name}_{run_id}_stats.json"
    
    if stats_path.exists():
        with stats_path.open('r') as f:
            stats = json.load(f)
    else:
        # Fallback if the stats file does not exist yet (should not happen)
        stats = {
            "run_id": run_id,
            "producer_name": producer_name
        }
    
    # Add experiment_config to the stats
    stats['experiment_config'] = {
        'partition_count': partition_count,
        'nb_messages_per_batch': nb_messages_per_batch,
        'batch_header_size_bytes': BATCH_HEADER_SIZE,
        'compression_ratio': COMPRESSION_RATIO,
        'partition_to_broker': partition_to_broker,
        'lambda_rate': lambda_rate,
        'linger_ms': linger_ms,
    }
    
    # Add broker_tracking_summary to the stats
    stats['broker_tracking_summary'] = {
        str(broker_id): {
            'batch_count': broker_batch_counts.get(broker_id, 0),
            'partition_count': info['partition_count']
        }
        for broker_id, info in broker_tracking.items()
    }
    
    # Add batch_metrics to the stats
    stats['batch_metrics'] = {
        'Nb': int(Nb),
        'P': int(P),
        'Sm_mean_bytes': round(float(np.mean(Sm_filtered)), 2),
        'Sb_mean_bytes': round(float(np.mean(Sb_filtered)), 2),
        'Gs_mean_ms': round(float(np.mean(Gs_filtered)), 2),
        'Sr_mean_bytes': None  # calculated later from update_stats_with_jmx
    }
    
    # Rewrite the stats file
    with stats_path.open('w') as f:
        json.dump(stats, f, indent=2)


def send_message():
    """Requests a message from the message_generator, adds the Kafka fields,and sends it to the broker."""
    global current_batch_id, messages_in_current_batch

    if messages_in_current_batch == 0:
        current_batch_id += 1

    # 1) generate the message and the key using the message generator
    gen_msg = make_message()
    key = gen_msg["key"]
    message_text = gen_msg["message"]

    # 2) Send timestamp from the producer's perspective
    send_timestamp = time.time_ns() / 1_000_000
    send_ts[key] = send_timestamp

    message = {
        "key": key,
        "run_id": run_id,
        "topic": topic,
        "message": message_text,
        "send_timestamp_ms": send_timestamp, # Timestamp (in ms) generated by the producer at the exact moment the message is created and "queued" for sending
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
        messages_in_current_batch = 0 # reset the counter to start a new batch


def main():
    target_messages = int(os.getenv("TARGET_MESSAGES")) # total number of messages to send in the experiment
    experiment_duration = int(os.getenv("EXPERIMENT_DURATION")) # maximum duration of the experiment in seconds
    
    time.sleep(60)  # wait for 60 seconds to ensure that all consumers are up and running before starting the transmission of messages. 
    
    transmission_start = time.time()
    end_time = transmission_start + experiment_duration
    
    try:
        messages_sent = 0
        while (messages_sent < target_messages and time.time() < end_time) or messages_in_current_batch > 0:
            if messages_sent < target_messages and time.time() < end_time:
                if send_mode == "poisson": # of the mode is poisson
                    poisson_sleep()  # waits for a random interval drawn from an exponential distribution to simulate Poisson arrivals
                send_message()
                messages_sent += 1
            elif messages_in_current_batch > 0:
                # If we have sent all target messages or reached the time limit, but we are still in the middle of a batch, we continue sending messages without waiting (to complete the batch) until messages_in_current_batch resets to 0.
                send_message()
                messages_sent += 1
            else:
                break  
    finally:
        p.flush()
        transmission_end = time.time()
        
        # After finishing sending all messages (or reaching the time limit), we send an "END_SIGNAL" message to each partition to indicate to the consumers that no more messages will be sent. This allows the consumers to know when they have received all messages and can finalize their processing.
        for partition in range(partition_count):
            end_signal = json.dumps({
                "type": "END_SIGNAL",
                "producer": producer_name,
                "timestamp": time.time()
            })
            p.produce(topic, end_signal.encode('utf-8'), partition=partition)
        p.flush()
        
        time.sleep(2)
        
        write_all_producer_records() # writes the JSONL recap of all deliveries for this producer
        
        # Computes the transmission stats
        total_time = transmission_end - transmission_start
        num_messages = len(delivery_records)
        total_payload_bytes = num_messages * payload_size_bytes
        unique_batches = len(set(r['send_batch_id'] for r in delivery_records))
        
        # Upload the stats to the producer's stats file
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
                "throughput_mbps": None,
                "overhead_percentage": None,
                "packet_header_size_bytes": None
            }
        }
        
        stats_path = stats_dir / f"{producer_name}_{run_id}_stats.json"
        with stats_path.open('w') as f:
            json.dump(stats, f, indent=2)
            
        write_unified_stats() # updates the stats file
    
    while True:
        time.sleep(60) # keep the producer running

if __name__ == "__main__":
    main()
