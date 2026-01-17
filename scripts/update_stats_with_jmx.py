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
            pass
    
    return int(total_bytes_in)


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
    
    # Raccoglie payload totale dai producer
    total_payload_bytes = 0
    total_transmission_time = 0
    
    for pf in producer_files:
        try:
            with open(pf) as f:
                data = json.load(f)
                total_payload_bytes += data.get("payload_bytes", 0)
                # Usa il tempo massimo tra i producer
                total_transmission_time = max(
                    total_transmission_time,
                    data.get("transmission_time_seconds", 0)
                )
        except Exception:
            pass
    
    # Calcola overhead e throughput
    kafka_overhead_bytes = total_bytes_from_brokers - total_payload_bytes
    kafka_total_bytes = total_bytes_from_brokers
    overhead_percentage = (kafka_overhead_bytes / kafka_total_bytes * 100) if kafka_total_bytes > 0 else 0
    throughput_kbps = (kafka_total_bytes / 1024) / total_transmission_time if total_transmission_time > 0 else 0
    
    # Aggiorna ogni file producer stats
    for pf in producer_files:
        try:
            with open(pf, 'r') as f:
                data = json.load(f)
            
            data["kafka_overhead_bytes"] = kafka_overhead_bytes
            data["kafka_total_bytes"] = kafka_total_bytes
            data["throughput_kbps"] = round(throughput_kbps, 2)
            data["overhead_percentage"] = round(overhead_percentage, 2)
            
            with open(pf, 'w') as f:
                json.dump(data, f, indent=2)
                
        except Exception:
            pass
    
    return True


if __name__ == "__main__":
    # Esegue aggiornamento se forniti run_id e topic_name
    # Es: python scripts/update_stats_with_jmx_overhead.py "$RUN_ID" "$TOPIC_NAME" 
    if len(sys.argv) == 3:
        update_producer_stats(sys.argv[1], sys.argv[2])
    
