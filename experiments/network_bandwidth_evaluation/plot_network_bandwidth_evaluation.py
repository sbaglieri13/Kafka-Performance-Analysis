import json
from pathlib import Path
import matplotlib.pyplot as plt
import numpy as np
from scipy.optimize import curve_fit
import sys
from collections import defaultdict
from typing import Dict, List, Tuple
from matplotlib.lines import Line2D

sys.path.append(str(Path(__file__).parent.parent.parent))
from utilities.tukey_iqr import tukey_iqr_filter
from utilities.confidence_interval import compute_ci


def load_bandwidth_data() -> Tuple[Dict[int, List[Dict]]]: 
    """
    Loads stats files, groups by (partition_count, batch_size),
    applies IQR on throughput and overhead, then 95% CI.
    Returns for each partition_count a list of dicts with:
        - batch_size (int)
        - throughput (float): mean throughput post-IQR+CI [MB/s]
        - overhead (float): mean overhead post-IQR+CI [%]
    """
    stats_dir = Path(__file__).parent.parent.parent / "data/stats"
    stats_files = list(stats_dir.glob("*_stats.json"))

    if not stats_files:
        print(f"No stats files found in {stats_dir}")
        return {}, {}

    print(f"Found {len(stats_files)} stats files")

    # group by (partition_count, batch_size) with lists of throughput and overhead
    groups: Dict[Tuple[int, int], Dict[str, List[float]]] = defaultdict(
        lambda: {"throughput": [], "overhead": []}
    )

    # load each stats file, extract partition_count, batch_size, throughput, overhead,
    for stats_file in stats_files:
        with open(stats_file) as f:
            data = json.load(f)

        partition_count = data["experiment_config"]["partition_count"]
        batch_size = data["experiment_config"]["nb_messages_per_batch"]
        throughput = data["kafka_metrics"]["throughput_mbps"]
        overhead = data["kafka_metrics"]["overhead_percentage"]
        
        # update group for this (partition_count, batch_size)
        key = (partition_count, batch_size)
        groups[key]["throughput"].append(throughput)
        groups[key]["overhead"].append(overhead)

    data_by_partitions: Dict[int, List[Dict]] = defaultdict(list)

    # for each (partition_count, batch_size) group, apply IQR filter and compute CI for throughput and overhead
    for (partition_count, batch_size), values in groups.items():
        t_filtered, _ = tukey_iqr_filter(values["throughput"])
        o_filtered, _ = tukey_iqr_filter(values["overhead"])
        
        # check if all values were removed by IQR filter, if so skip this group and print a warning
        if len(t_filtered) == 0:
            print(f"\nALL THROUGHPUT REMOVED by IQR")
            print(f"Partition={partition_count}, Batch={batch_size}")
            continue

        # check if all values were removed by IQR filter, if so skip this group and print a warning
        if len(o_filtered) == 0:
            print(f"\nALL OVERHEAD REMOVED by IQR")
            print(f"Partition={partition_count}, Batch={batch_size}")
            continue

        t_ci = compute_ci(t_filtered)
        o_ci = compute_ci(o_filtered)
        
        # check if CI returned None (can happen if only 1 value after IQR), if so skip this group and print a warning
        if t_ci["representative_point"] is None:
            print(f"\nCI returned None (Throughput)")
            print(f"Partition={partition_count}, Batch={batch_size}")
            continue

        # check if CI returned None (can happen if only 1 value after IQR), if so skip this group and print a warning
        if o_ci["representative_point"] is None:
            print(f"\nCI returned None (Overhead)")
            print(f"Partition={partition_count}, Batch={batch_size}")
            continue

        data_by_partitions[partition_count].append({
            "batch_size": batch_size,
            "throughput": t_ci["representative_point"],
            "overhead":   o_ci["representative_point"],
        })

    # sort by batch_size for each partition_count
    for pc in data_by_partitions:
        data_by_partitions[pc].sort(key=lambda d: d["batch_size"])

    return dict(data_by_partitions)


def plot_network_bandwidth_evaluation():
    '''Plots throughput and overhead vs batch size for each partition count, with theoretical curves.'''
    data_by_partitions = load_bandwidth_data()
    
    if not data_by_partitions:
        print("No data found!")
        return 1

    partition_counts = sorted(data_by_partitions.keys())
    fig, axes = plt.subplots(1, len(partition_counts), figsize=(5 * len(partition_counts), 5))
    
    if len(partition_counts) == 1:
        axes = [axes]
    
    fig.suptitle('Network Bandwidth Evaluation: Throughput vs Overhead', fontsize=12, fontweight='bold')

    for idx, partition_count in enumerate(partition_counts):
        ax1 = axes[idx]
        
        partition_data = data_by_partitions.get(partition_count, [])
        if not partition_data:
            print(f"No data found for partition {partition_count}")
            ax1.text(0.5, 0.5, 'No data', transform=ax1.transAxes, ha='center', va='center', fontsize=14, color='red')
            ax1.set_xlabel('Number of Messages per Batch', fontsize=12)
            ax1.text(0.5, 1.05, f'Partitions = {partition_count}', transform=ax1.transAxes, fontsize=11, ha='center')
            continue

        batch_sizes = np.array([d["batch_size"]  for d in partition_data])
        throughputs = np.array([d["throughput"]  for d in partition_data])
        overheads   = np.array([d["overhead"]    for d in partition_data])
        
        batch_smooth = np.linspace(1, max(batch_sizes), 300)
        
        # Throughput prediction: exponential saturation T(x) = c - a * exp(-b * x)
        def throughput_model(x, a, b, c):
            return c - a * np.exp(-b * x)
        
        # Fit on real data
        params_t, _ = curve_fit(throughput_model, batch_sizes, throughputs, p0=[40, 0.1, 135])
        throughput_theory = throughput_model(batch_smooth, *params_t)     
        
        # Overhead prediction: power-law decay O(x) = a * x^(-b) + c
        def overhead_model(x, a, b, c):
            return a * np.power(x, -b) + c
                
        # Fit on real data 
        params_o, _ = curve_fit(overhead_model, batch_sizes, overheads, p0=[20, 0.1, 8])
        overhead_theory = overhead_model(batch_smooth, *params_o)

        # Plot
        # Axis X: Batch size
        ax1.set_xlabel('Number of Messages per Batch', fontsize=12)
        
        # Axis Y left: Throughput
        ax1.set_ylabel('Throughput of Packets (MB/s)', fontsize=12, color='blue')
        
        # Throughput prediction
        ax1.plot(batch_smooth, throughput_theory, '-', color='blue', linewidth=1.5, alpha=0.3, zorder=1, label='Throughput prediction')
        
        # Marker throughput 
        ax1.plot(batch_sizes, throughputs, '+', color='blue', markersize=10, markeredgewidth=2, zorder=3, label='Experiment result (Throughput)')
        
        ax1.tick_params(axis='y', labelcolor='blue')
        ax1.grid(True, alpha=0.3)
        
        # Asse Y right: Overhead
        ax2 = ax1.twinx()
        ax2.set_ylabel('Overhead (%)', fontsize=12, color='red')
        
        # Overhead prediction
        ax2.plot(batch_smooth, overhead_theory, '-', color='red', linewidth=1.5, alpha=0.3, zorder=1, label='Overhead prediction')
        
        # Marker overhead
        ax2.plot(batch_sizes, overheads, 'o', color='red', markersize=6, markerfacecolor='none', markeredgewidth=1.5, zorder=3, label='Experiment result (Overhead)')
        
        ax2.tick_params(axis='y', labelcolor='red')
        
        # Title with partition count
        ax1.text(0.5, 1.05, f'Partition Number = {partition_count}', transform=ax1.transAxes, fontsize=11, ha='center')

        if idx == 0:
            lines1, labels1 = ax1.get_legend_handles_labels()
            lines2, labels2 = ax2.get_legend_handles_labels()   
    
    fig.legend(lines1 + lines2, labels1 + labels2, loc='lower center', bbox_to_anchor=(0.5, -0.05), ncol=4, frameon=True, fontsize=10)
    
    plt.tight_layout()
    plt.subplots_adjust(bottom=0.18, wspace=0.5)  # add some space at the bottom for the legend and between subplots
    
    plots_dir = Path(__file__).parent / "plots"
    plots_dir.mkdir(parents=True, exist_ok=True)
    plt.savefig(plots_dir / "network_bandwidth_evaluation.png", dpi=300, bbox_inches='tight')
    
    print(f"\n✓ Plot saved!")
    
def main():
    plot_network_bandwidth_evaluation()

if __name__ == "__main__":
    main()
