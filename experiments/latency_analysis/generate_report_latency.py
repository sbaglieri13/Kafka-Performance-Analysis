import json
import argparse
import re
import numpy as np
import pandas as pd
from pathlib import Path
from typing import Dict, Optional


DATA_DIR = Path(__file__).parent.parent.parent / "data"
REPORTS_DIR = Path(__file__).parent / "reports"


def parse_run_id(run_id: str) -> Dict:
    '''Extracts metadata from run_id string.'''
    # Poisson: lam{lambda}_nb{nb}_part{partitions}_r{run}
    match_poisson = re.fullmatch(r"lam(\d+)_nb(\d+)_part(\d+)_r(\d+)", run_id)
    if match_poisson:
        return {
            "mode": "poisson",
            "lambda": int(match_poisson.group(1)),
            "nb": int(match_poisson.group(2)),
            "partitions": int(match_poisson.group(3)),
            "run_number": int(match_poisson.group(4)),
        }

    # Continuous: nb{nb}_part{partitions}_r{run}
    match_continuous = re.fullmatch(r"nb(\d+)_part(\d+)_r(\d+)", run_id)
    if match_continuous:
        return {
            "mode": "continuous",
            "lambda": None,
            "nb": int(match_continuous.group(1)),
            "partitions": int(match_continuous.group(2)),
            "run_number": int(match_continuous.group(3)),
        }


def load_consumer_data(run_id: str, data_dir: Optional[Path] = None) -> pd.DataFrame:
    '''Loads consumer data for the given run_id. Expects files named like consumer_*_{run_id}_messages_recap.jsonl'''
    files = list((data_dir or DATA_DIR).glob(f"consumer_*_{run_id}_messages_recap.jsonl"))
    if not files:
        return pd.DataFrame()

    rows = []
    for f in files:
        container = f.stem.replace(f"_{run_id}_messages_recap", "")
        for line in f.read_text().splitlines():
            if line.strip():
                r = json.loads(line)
                r["container_name"] = container
                rows.append(r)

    return pd.DataFrame(rows) if rows else pd.DataFrame()


def calculate_latency(df: pd.DataFrame) -> pd.DataFrame:
    '''Calculates latency in milliseconds and filters out negative values.'''
    df = df.copy()
    df["latency_ms"] = df["receive_timestamp_ms"] - df["producer_send_timestamp_ms"]
    return df[df["latency_ms"] >= 0].reset_index(drop=True)

def load_throughput(run_id: str, data_dir: Optional[Path] = None) -> Optional[float]:
    '''Loads Kafka throughput in Mbps from stats file for the given run_id. Expects files named like *_run_id_stats.json'''
    stats_dir = (data_dir or DATA_DIR) / "stats"
    files = list(stats_dir.glob(f"*_{run_id}_stats.json"))
    if not files:
        return None
    stats = json.loads(files[0].read_text())
    return stats.get("kafka_metrics", {}).get("throughput_mbps")

def aggregate_global(df: pd.DataFrame) -> Dict:
    '''Aggregates latency metrics globally across all messages.'''
    lat = df["latency_ms"]
    p25 = float(np.percentile(lat, 25))
    p75 = float(np.percentile(lat, 75))
    return {
        "latency_min_ms": round(float(lat.min()), 3),
        "latency_max_ms": round(float(lat.max()), 3),
        "latency_mean_ms": round(float(lat.mean()), 3),
        "latency_std_ms": round(float(lat.std()), 3),
        "latency_p25_ms": round(p25, 3),
        "latency_p50_ms": round(float(np.percentile(lat, 50)), 3),
        "latency_p75_ms": round(p75, 3),
        "latency_p95_ms": round(float(np.percentile(lat, 95)), 3),
        "latency_p99_ms": round(float(np.percentile(lat, 99)), 3),
        "latency_iqr_ms": round(p75 - p25, 3),
        "message_count": int(len(lat)),
    }


def aggregate_by_consumer(df: pd.DataFrame) -> list:
    '''Aggregates latency metrics by consumer (container_name).'''
    return (
        df.groupby("container_name")["latency_ms"]
        .agg(
            latency_min_ms="min",
            latency_max_ms="max",
            latency_mean_ms="mean",
            latency_std_ms="std",
            latency_p25_ms=lambda x: float(np.percentile(x, 25)),
            latency_p50_ms=lambda x: float(np.percentile(x, 50)),
            latency_p75_ms=lambda x: float(np.percentile(x, 75)),
            latency_p95_ms=lambda x: float(np.percentile(x, 95)),
            latency_p99_ms=lambda x: float(np.percentile(x, 99)),
            latency_iqr_ms=lambda x: float(np.percentile(x, 75) - np.percentile(x, 25)),
            message_count="count",
        )
        .reset_index()
        .rename(columns={"container_name": "consumer"})
        .round(3)
        .to_dict("records")
    )


def generate_report(run_id: str) -> Dict:
    '''Generates a latency report for the given run_id.'''
    meta = parse_run_id(run_id)
    df = load_consumer_data(run_id)
    if df.empty:
        return {"error": f"Nessun dato trovato per run_id={run_id}"}
    df = calculate_latency(df)
    return {
        "run_id": run_id,
        "mode": meta["mode"],
        "lambda": meta["lambda"],
        "nb": meta["nb"],
        "partitions": meta["partitions"],
        "run_number": meta["run_number"],
        "throughput_mbps": load_throughput(run_id),
        "global": aggregate_global(df),
        "by_consumer": aggregate_by_consumer(df),
    }


def save_report(report: Dict, run_id: str):
    '''Saves the report as a JSON file in the reports directory.'''
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    output_file = REPORTS_DIR / f"latency_report_{run_id}.json"
    with open(output_file, "w") as f:
        json.dump(report, f, indent=2)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--run_id", required=True)
    args = parser.parse_args()

    report = generate_report(args.run_id)
    save_report(report, args.run_id)
    print(f"  ✓ Report saved: latency_report_{args.run_id}.json")



if __name__ == "__main__":
    main()
