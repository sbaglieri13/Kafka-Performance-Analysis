import sys
import json
from pathlib import Path
from jmxquery import JMXConnection, JMXQuery


def collect_jmx_metrics(topic_name):
    """Raccoglie byte totali IN dai broker per il topic"""
    brokers = [
        {"host": "localhost", "port": 9999}, # Broker 1
        {"host": "localhost", "port": 10000}, # Broker 2
        {"host": "localhost", "port": 10001}, # Broker 3
    ]
    
    total_bytes_in = 0 
    
    for broker in brokers:
        try:
            # Connessione JMX
            jmx_url = f"service:jmx:rmi:///jndi/rmi://{broker['host']}:{broker['port']}/jmxrmi"
            jmx_conn = JMXConnection(jmx_url)
            
            query = JMXQuery(
                f"kafka.server:type=BrokerTopicMetrics,name=BytesInPerSec,topic={topic_name}",
                attribute="Count"
            )
            
            metrics = jmx_conn.query([query])
            
            if metrics and metrics[0].value:
                total_bytes_in += float(metrics[0].value)
                
        except Exception as e:
            print(f"Errore nella raccolta JMX dal broker {broker['host']}:{broker['port']}: {e}")
            pass
    
    return int(total_bytes_in)

def calculate_packet_header_size(stats_data, kafka_total_bytes):
    """
    Calcola il packet header size medio
    
    packet_header_size = (kafka_total_bytes - (Sb * num_batches)) / num_batches
    dove Sb = (Sm * Nb + batch_header_size) / compression_ratio
    """
    total_batch_size_bytes = 0
    total_batches = 0
    
    for data in stats_data:
        transmission_stats = data['transmission_stats']
        experiment_config = data['experiment_config']
            
        num_batches = transmission_stats['num_batches']
        messages_per_batch = experiment_config['nb_messages_per_batch']
        batch_header_size = experiment_config['batch_header_size_bytes']
        compression_ratio = experiment_config['compression_ratio']
            
        if 'batch_metrics' in data and data['batch_metrics']['Sm_mean_bytes'] is not None:
            avg_message_size = data['batch_metrics']['Sm_mean_bytes']
        else:
            # Fallback: calcola da payload_bytes (non filtrato)
            num_messages = transmission_stats['num_messages']
            avg_message_size = transmission_stats['payload_bytes'] / num_messages
        
        # Sb = (Sm * Nb + batch_header_size) / compression_ratio
        batch_size_bytes = (avg_message_size * messages_per_batch + batch_header_size) / compression_ratio
        
        total_batch_size_bytes += batch_size_bytes * num_batches
        total_batches += num_batches
    
    if total_batches == 0:
        return None
    
    # packet_header_size = (kafka_total_bytes - total_batch_size_bytes) / total_batches
    packet_header_size = (kafka_total_bytes - total_batch_size_bytes) / total_batches
    
    return round(packet_header_size, 2)

def update_producer_stats(run_id, topic_name):
    """Aggiorna i file stats dei producer"""
    stats_dir = Path("./data/stats")
    producer_files = list(stats_dir.glob(f"*_{run_id}_stats.json"))
    
    if not producer_files:
        return False
    
    # Raccoglie byte totali dai broker
    total_bytes_from_brokers = collect_jmx_metrics(topic_name)
    
    if total_bytes_from_brokers == 0:
        return False
    
    # Raccoglie payload totale dai producer e dati per calcolo
    total_payload_bytes = 0
    total_transmission_time = 0
    stats_data = []
    
    for pf in producer_files:
        try:
            with open(pf) as f:
                data = json.load(f)
                stats_data.append(data)
                total_payload_bytes += data['transmission_stats']['payload_bytes']
                # Usa il tempo massimo tra i producer
                total_transmission_time = max(
                    total_transmission_time,
                    data['transmission_stats']['transmission_time_seconds']
                )
        except Exception:
            print("Errore nella lettura del file:", pf)
            pass
    
    # Calcola overhead e throughput
    kafka_overhead_bytes = total_bytes_from_brokers - total_payload_bytes
    kafka_total_bytes = total_bytes_from_brokers
    overhead_percentage = (kafka_overhead_bytes / kafka_total_bytes * 100) if kafka_total_bytes > 0 else 0
    throughput_mbps = (kafka_total_bytes / (1024 * 1024)) / total_transmission_time if total_transmission_time > 0 else 0
    
    # Calcola packet header size
    packet_header_size = calculate_packet_header_size(stats_data, kafka_total_bytes)
    
    # Aggiorna ogni file producer stats
    for pf in producer_files:
        try:
            pf = Path(pf)
            temp_path = pf.with_suffix(pf.suffix + ".tmp")
            with pf.open('r') as f:
                data = json.load(f)
            
            data['kafka_metrics']['kafka_overhead_bytes'] = kafka_overhead_bytes
            data['kafka_metrics']['kafka_total_bytes'] = kafka_total_bytes
            data['kafka_metrics']['throughput_mbps'] = round(throughput_mbps, 2)
            data['kafka_metrics']['overhead_percentage'] = round(overhead_percentage, 2)
            data['kafka_metrics']['packet_header_size_bytes'] = packet_header_size
            
            if 'batch_metrics' in data and packet_header_size is not None:
                batch_metrics = data['batch_metrics']
                Sb_mean = batch_metrics['Sb_mean_bytes']
                P = batch_metrics.get('P', 1)
                
                if Sb_mean is not None:
                    Sr_mean = (Sb_mean * P) + packet_header_size
                    batch_metrics['Sr_mean_bytes'] = round(Sr_mean, 2)
            
            with temp_path.open('w') as f:
                json.dump(data, f, indent=2)
            temp_path.replace(pf)
                
        except Exception:
            print("Errore nell'aggiornamento del file:", pf)
            pass
    
    return True

if __name__ == "__main__":
    # Esegue aggiornamento se forniti run_id e topic_name
    # Es: python scripts/update_stats_with_jmx_overhead.py "$RUN_ID" "$TOPIC_NAME" 
    if len(sys.argv) == 3:
        update_producer_stats(sys.argv[1], sys.argv[2])
    
