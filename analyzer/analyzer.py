import json
from pathlib import Path
import statistics

data_dir = Path("data")

producer_info = {}
min_produce_ts = None
max_produce_ts = None
payload_size_bytes = None
run_id = None
partition_count = None
topic = None

# Legge TUTTI i file di metriche dei producer
for prod_path in data_dir.glob("producer_metrics_*.jsonl"):
    with prod_path.open(encoding="utf-8") as f:
        for line in f:
            rec = json.loads(line)
            key = rec["key"]
            producer_info[key] = rec

            ts = rec["produce_call_ts_ns"]
            if min_produce_ts is None or ts < min_produce_ts:
                min_produce_ts = ts
            if max_produce_ts is None or ts > max_produce_ts:
                max_produce_ts = ts

            payload_size_bytes = rec.get("payload_size_bytes", payload_size_bytes)
            run_id = rec.get("run_id", run_id)
            partition_count = rec.get("partition_count", partition_count)
            topic = rec.get("topic", topic)

latencies_l1 = []
latencies_e2e = []
max_receive_ts = None

consumer_info = {}

# Legge TUTTI i file di metriche dei consumer
for cons_path in data_dir.glob("consumer_metrics_*.jsonl"):
    with cons_path.open(encoding="utf-8") as f:
        for line in f:
            rec = json.loads(line)
            key = rec["key"]
            consumer_info[key] = rec

# Join producer/consumer per chiave e calcolo latenze
for key, rec in consumer_info.items():
    recv_ts = rec["receive_ts_ns"]
    p = producer_info.get(key)
    if not p:
        continue

    l1 = (p["written_ts_ns"] - p["produce_call_ts_ns"]) / 1e6  # ms
    e2e = (recv_ts - p["produce_call_ts_ns"]) / 1e6

    latencies_l1.append(l1)
    latencies_e2e.append(e2e)

    if max_receive_ts is None or recv_ts > max_receive_ts:
        max_receive_ts = recv_ts


def summarize(values):
    return {
        "count": len(values),
        "min": min(values) if values else None,
        "max": max(values) if values else None,
        "mean": statistics.fmean(values) if values else None,
        "median": statistics.median(values) if values else None,
    }


print("Run ID:", run_id)
print("Topic:", topic)
print("Partition count:", partition_count)
print("Payload size (bytes):", payload_size_bytes)
print("Produce latency l1 (ms):", summarize(latencies_l1))
print("End-to-end latency (ms):", summarize(latencies_e2e))

summary_file = data_dir / "experiment_summary.csv"

if min_produce_ts is not None and max_receive_ts is not None and payload_size_bytes is not None:
    duration_sec = (max_receive_ts - min_produce_ts) / 1e9
    count = len(latencies_e2e)
    total_bytes = count * payload_size_bytes
    throughput_MBps = (total_bytes / (1024 * 1024)) / duration_sec if duration_sec > 0 else None
    send_interval_ms = (duration_sec * 1000 / count) if count > 0 else None

    print("Run duration (s):", duration_sec)
    print("Messages:", count)
    print("Throughput (MB/s):", throughput_MBps)
    print("Avg send interval (ms):", send_interval_ms)

    # header CSV
    header = (
        "run_id,topic,partition_count,payload_size_bytes,"
        "messages,duration_sec,throughput_MBps,send_interval_ms,"
        "l1_mean_ms,e2e_mean_ms\n"
    )

    row = (
        f"{run_id},{topic},{partition_count},{payload_size_bytes},"
        f"{count},{duration_sec},{throughput_MBps},{send_interval_ms},"
        f"{summarize(latencies_l1)['mean']},{summarize(latencies_e2e)['mean']}\n"
    )

    if not summary_file.exists():
        with summary_file.open("w", encoding="utf-8") as f:
            f.write(header)
            f.write(row)
    else:
        with summary_file.open("a", encoding="utf-8") as f:
            f.write(row)
