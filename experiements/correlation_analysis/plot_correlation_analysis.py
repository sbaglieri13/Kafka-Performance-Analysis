import json
import sys
import numpy as np
import matplotlib.pyplot as plt
from pathlib import Path
from scipy.optimize import curve_fit
from typing import Dict, List, Tuple
from collections import defaultdict

sys.path.append(str(Path(__file__).parent.parent.parent))
from utilities.tukey_iqr import tukey_iqr_filter
from utilities.confidence_interval import compute_ci

def load_batch_metrics_data() -> Tuple[Dict[int, List[Dict]]]: 
    """
    Carica i file stats, raggruppa per (partition_count, batch_size),
    applica IQR su Sr e Gs, poi IC al 95%.
    Restituisce per ogni partition_count una lista di dict con:
        - batch_size (int)
        - Sr_mean (float): dimensione media pacchetto post-IQR+IC [KB]
        - Gs_mean (float): intervallo medio di invio post-IQR+IC [ms]
        - Sr_n (int):  numero campioni validi dopo IQR
        - Gs_n (int):  numero campioni validi dopo IQR
    """
    stats_dir = Path(__file__).parent.parent.parent / "data/stats"
    stats_files = list(stats_dir.glob("*_stats.json"))
    
    if not stats_files:
        print(f"No stats files found in {stats_dir}")
        return {}
    
    print(f"Found {len(stats_files)} stats files")

    # raggruppa i dati per (partition_count, batch_size)
    groups: Dict[Tuple[int, int], Dict[str, List[float]]] = defaultdict(
        lambda: {"Sr": [], "Gs": []}
    )

    # carica i dati da ogni file e popola i gruppi
    for stats_file in stats_files:
        with open(stats_file, "r") as f:
            stats_data = json.load(f)

        partition_count = stats_data["experiment_config"]["partition_count"]
        batch_size = stats_data["experiment_config"]["nb_messages_per_batch"]
        batch_metrics = stats_data["batch_metrics"]

        Sr_kb = batch_metrics["Sr_mean_bytes"] / 1024 
        Gs_ms = batch_metrics["Gs_mean_ms"]

        key = (partition_count, batch_size)
        groups[key]["Sr"].append(Sr_kb)
        groups[key]["Gs"].append(Gs_ms)

    data_by_partitions: Dict[int, List[Dict]] = defaultdict(list)

    # applica IQR e IC per ogni gruppo e organizza i dati per partition_count
    for (partition_count, batch_size), values in groups.items():
        Sr_arr = np.array(values["Sr"])
        Gs_arr = np.array(values["Gs"])

        Sr_filtered, _ = tukey_iqr_filter(Sr_arr)
        Gs_filtered, _ = tukey_iqr_filter(Gs_arr)
        Sr_ci = compute_ci(Sr_filtered)
        Gs_ci = compute_ci(Gs_filtered)

        # aggiorna il dizionario organizzato per partition_count
        data_by_partitions[partition_count].append({
            "batch_size": batch_size,
            "Sr_mean":    Sr_ci["mean"],
            "Gs_mean":    Gs_ci["mean"],
            "Sr_n":       Sr_ci["n"],
            "Gs_n":       Gs_ci["n"],
        })

    # ordina i dati per batch_size all'interno di ogni partition_count
    for partition_count in data_by_partitions:
        data_by_partitions[partition_count].sort(key=lambda d: d["Sr_mean"])

    return dict(data_by_partitions)


def plot_correlation_analysis(data_by_partitions: Dict[int, List[Dict]], output_dir: Path):
    """Genera 4 subplot orizzontali per ogni partition_count, con scatter dei valori medi (post IQR+IC)"""

    colors = ['#1f77b4', '#ff7f0e', '#2ca02c', '#d62728']
    markers = ['s', '^', 'o', 'D']

    partition_counts = sorted(data_by_partitions.keys())

    all_Sr, all_Gs = [], []
    for pc in partition_counts:
        for d in data_by_partitions[pc]:
            all_Sr.append(d["Sr_mean"])
            all_Gs.append(d["Gs_mean"])

    x_limits = (min(all_Sr) * 0.97, max(all_Sr) * 1.03)
    y_limits = (min(all_Gs) * 0.97, max(all_Gs) * 1.03)

    fig, axes = plt.subplots(1, len(partition_counts), figsize=(5 * len(partition_counts), 5))
    if len(partition_counts) == 1:
        axes = [axes]

    fig.suptitle('Correlation Analysis: Packet Size vs Send Interval', fontsize=12, fontweight='bold')

    for idx, partition_count in enumerate(partition_counts):
        ax = axes[idx]
        data = data_by_partitions[partition_count]

        packet_sizes = np.array([d["Sr_mean"] for d in data])
        send_intervals = np.array([d["Gs_mean"] for d in data])

        color, marker = colors[idx % len(colors)], markers[idx % len(markers)]

        # Punto medio, valore rappresentativo post IQR+IC
        ax.scatter(
            packet_sizes, send_intervals,
            marker=marker,
            s=80,
            color=color,
            edgecolors="black",
            linewidths=0.8,
            zorder=3,
        )

        # Fit lineare sui valori medi
        def linear_model(x, a, b):
            return a * x + b

        params, _ = curve_fit(linear_model, packet_sizes, send_intervals)
        
        # Linea solo sui dati esistenti
        x_smooth = np.linspace(packet_sizes.min(), packet_sizes.max(), 100)
        y_pred = linear_model(x_smooth, *params)
        
        # R²
        residuals = send_intervals - linear_model(packet_sizes, *params)
        r_squared = 1 - np.sum(residuals**2) / np.sum((send_intervals - np.mean(send_intervals))**2)

        ax.plot(
            x_smooth, y_pred, "--",
            color=color, linewidth=2.2, alpha=0.85,
            label=f"partition numbers={partition_count}  R²={r_squared:.4f}",
        )    

        ax.set_xlim(x_limits)
        ax.set_ylim(y_limits)
        ax.set_xlabel('Packet Size (KB)', fontsize=11)
        ax.set_ylabel('Packet send interval (ms)', fontsize=11)
        ax.legend(loc='upper left', fontsize=9)
        ax.grid(True, alpha=0.3, linestyle='--')
    
    plt.tight_layout()
    output_file = output_dir / "correlation_analysis.png"
    plt.savefig(output_file, dpi=300, bbox_inches='tight')
    plt.close()

def main():
    print("Generating plot correlation analysis...")
    data_by_partitions = load_batch_metrics_data()

    if not data_by_partitions:
        print("No data found!")
        return 1
    
    plots_dir = Path(__file__).parent / "plots"
    plots_dir.mkdir(parents=True, exist_ok=True)

    plot_correlation_analysis(data_by_partitions, plots_dir)
    print("✓ Generated correlation_analysis.png")
    return 0

if __name__ == "__main__":
    main()
