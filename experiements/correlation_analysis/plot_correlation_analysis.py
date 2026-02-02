import json
import numpy as np
import matplotlib.pyplot as plt
from pathlib import Path
from scipy.optimize import curve_fit
from typing import Dict, List
from collections import defaultdict


def load_batch_metrics_data() -> Dict[int, List[Dict]]:
    """Carica i file stats e raggruppa i dati batch_metrics per partition_count"""
    stats_dir = Path(__file__).parent.parent.parent / "data/stats"
    stats_files = list(stats_dir.glob("*_stats.json"))
    
    if not stats_files:
        print(f"No stats files found in {stats_dir}")
        return {}
    
    print(f"Found {len(stats_files)} stats files")
    data_by_partitions = defaultdict(list)
    
    for stats_file in stats_files:
        with open(stats_file, 'r') as f:
            stats_data = json.load(f)
        
        partition_count = stats_data['experiment_config']['partition_count']
        batch_size = stats_data['experiment_config']['nb_messages_per_batch']
        batch_metrics = stats_data['batch_metrics']
        Sr_mean = batch_metrics['Sr_mean_bytes']
        Gs_mean = batch_metrics['Gs_mean_ms']
        
        # I dati sono GIÀ filtrati P5-P95 dal producer!
        data_by_partitions[partition_count].append({
            'batch_size': batch_size,
            'avg_packet_size': Sr_mean / 1024,  # KB
            'avg_send_interval': Gs_mean
        })
    
    return dict(data_by_partitions)

def aggregate_by_batch_size(data: List[Dict]) -> List[Dict]:
    """Aggrega dati per batch_size calcolando la media di Sr e Gs"""
    batch_size_groups = defaultdict(lambda: {'Sr': [], 'Gs': []}) # Raggruppa per batch_size
    
    for item in data:
        # Aggiunge Sr e Gs al gruppo corrispondente
        batch_size_groups[item['batch_size']]['Sr'].append(item['avg_packet_size'])
        batch_size_groups[item['batch_size']]['Gs'].append(item['avg_send_interval'])
    
    # Calcola la media per ogni batch_size
    return sorted([{
        'avg_packet_size': np.mean(values['Sr']),
        'avg_send_interval': np.mean(values['Gs'])
    } for values in batch_size_groups.values()], key=lambda x: x['avg_packet_size'])


def plot_correlation_analysis(data_by_partitions: Dict[int, List[Dict]], output_dir: Path):
    """Genera 4 subplot orizzontali"""
    
    colors = ['#1f77b4', '#ff7f0e', '#2ca02c', '#d62728']
    markers = ['s', '^', 'o', 'D'] 
    
    partition_counts = sorted(data_by_partitions.keys())
    
    # Calcola limiti globali per gli assi
    all_packet_sizes = []
    all_send_intervals = []
    for partition_count in partition_counts:
        aggregated = aggregate_by_batch_size(data_by_partitions[partition_count])
        all_packet_sizes.extend([d['avg_packet_size'] for d in aggregated])
        all_send_intervals.extend([d['avg_send_interval'] for d in aggregated])
    
    x_limits = (min(all_packet_sizes) * 0.98, max(all_packet_sizes) * 1.02)
    y_limits = (min(all_send_intervals) * 0.98, max(all_send_intervals) * 1.02)
    
    # Crea subplot
    fig, axes = plt.subplots(1, 4, figsize=(20, 5))
    
    fig.suptitle('Correlation Analysis: Packet Size vs Send Interval', fontsize=12, fontweight='bold')
                 
    for idx, partition_count in enumerate(partition_counts):
        ax = axes[idx]
        aggregated_data = aggregate_by_batch_size(data_by_partitions[partition_count])
        
        # Estrae dati
        packet_sizes = np.array([d['avg_packet_size'] for d in aggregated_data])
        send_intervals = np.array([d['avg_send_interval'] for d in aggregated_data])
        
        # Fit lineare
        def linear_model(x, a, b): return a * x + b
        params, _ = curve_fit(linear_model, packet_sizes, send_intervals)
        
        # Linea solo sui dati esistenti
        x_smooth = np.linspace(packet_sizes.min(), packet_sizes.max(), 100)
        y_pred = linear_model(x_smooth, *params)
        
        # R²
        residuals = send_intervals - linear_model(packet_sizes, *params)
        r_squared = 1 - np.sum(residuals**2) / np.sum((send_intervals - np.mean(send_intervals))**2)
        
        color, marker = colors[idx], markers[idx]
        
        # Plot con edgecolors solo per marker riempiti
        if marker != 'x':  # Solo per marker riempiti
            ax.scatter(packet_sizes, send_intervals, marker=marker, s=100, 
                      color=color, edgecolors='black', linewidths=0.8, zorder=3)
        else:
            ax.scatter(packet_sizes, send_intervals, marker=marker, s=100, 
                      color=color, zorder=3)
        
        ax.plot(x_smooth, y_pred, '--', color=color, linewidth=2.5, alpha=0.8,
                label=f'partition number = {partition_count} (R² = {r_squared:.4f})')
        
        # Configurazione assi e legenda
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


