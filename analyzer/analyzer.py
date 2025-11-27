import json
from pathlib import Path
import statistics

data_dir = Path("data")
prod_file = data_dir / "producer_metrics.csv"
cons_file = data_dir / "consumer_metrics.csv"

# Carica metriche producer: key -> written_ts, produce_call_ts
producer_info = {}
with prod_file.open(encoding="utf-8") as f:
    for line in f:
        rec = json.loads(line)
        key = rec["key"]
        producer_info[key] = rec

latencies_l1 = []
latencies_e2e = []

with cons_file.open(encoding="utf-8") as f:
    for line in f:
        rec = json.loads(line)
        key = rec["key"]
        recv_ts = rec["receive_ts_ns"]
        p = producer_info.get(key)
        if not p:
            continue
        l1 = (p["written_ts_ns"] - p["produce_call_ts_ns"]) / 1e6  # ms
        e2e = (recv_ts - p["produce_call_ts_ns"]) / 1e6
        latencies_l1.append(l1)
        latencies_e2e.append(e2e)

def summarize(values):
    return {
        "count": len(values),
        "min": min(values) if values else None,
        "max": max(values) if values else None,
        "mean": statistics.fmean(values) if values else None,
        "median": statistics.median(values) if values else None,
    }

print("Produce latency l1 (ms):", summarize(latencies_l1))
print("End-to-end latency (ms):", summarize(latencies_e2e))
