import json
from pathlib import Path
import matplotlib.pyplot as plt
import numpy as np
from scipy.optimize import curve_fit

def main():
    stats_dir = Path(__file__).parent.parent.parent / "data/stats"
    partition_counts = [3, 6, 9, 12]
    fig, axes = plt.subplots(1, 4, figsize=(20, 5), sharey=False)
    
    fig.suptitle('Network Bandwidth Evaluation: Throughput vs Overhead', fontsize=12, fontweight='bold')

    for idx, partition_count in enumerate(partition_counts):
        ax1 = axes[idx]
        stats_files = list(stats_dir.glob(f"*_part_{partition_count}_stats.json"))
        
        if not stats_files:
            print(f"No data found for partition {partition_count}")
            ax1.text(0.5, 0.5, 'No data', transform=ax1.transAxes, 
                     ha='center', va='center', fontsize=14, color='red')
            ax1.set_xlabel('Number of Messages per Batch', fontsize=12)
            ax1.text(0.5, 1.05, f'Partition Number = {partition_count}', 
                     transform=ax1.transAxes, fontsize=11, ha='center')
            continue
        
        # Aggrega dati per batch size
        data_by_batch = {}
        
        for file_path in stats_files:
            with open(file_path) as f:
                data = json.load(f)
                batch_size = data['experiment_config']['nb_messages_per_batch']
                throughput = data['kafka_metrics']['throughput_mbps']
                overhead = data['kafka_metrics']['overhead_percentage']
                
                if batch_size not in data_by_batch:
                    data_by_batch[batch_size] = {
                        'throughput_mbps': [],
                        'overhead_percentage': []
                    }

                data_by_batch[batch_size]['throughput_mbps'].append(throughput)
                data_by_batch[batch_size]['overhead_percentage'].append(overhead)

        
        # Calcola throughput e overhead medi per batch size
        batch_sizes = []
        throughputs = []
        overheads = []
        
        print(f"\nPartition {partition_count} results:")
        for batch_size in sorted(data_by_batch.keys()):
            stats = data_by_batch[batch_size]
            
            # Throughput medio in MB/s
            throughput = sum(stats['throughput_mbps']) / len(stats['throughput_mbps'])
            
            # Overhead medio in %
            overhead = sum(stats['overhead_percentage']) / len(stats['overhead_percentage'])
            
            batch_sizes.append(batch_size)
            throughputs.append(throughput)
            overheads.append(overhead)
            
            print(f"  Batch {batch_size}: throughput {throughput:.1f} MB/s, overhead {overhead:.1f}%")
        
        batch_sizes = np.array(batch_sizes)
        throughputs = np.array(throughputs)
        overheads = np.array(overheads)
        
        batch_smooth = np.linspace(1, max(batch_sizes), 300)
        
        # Throughput prediction: funzione esponenziale saturante T(x) = c - a * exp(-b*x)
        def throughput_model(x, a, b, c):
            return c - a * np.exp(-b * x)
        
        # Fit sui dati reali
        params_t, _ = curve_fit(throughput_model, batch_sizes, throughputs, p0=[40, 0.1, 135])
        throughput_theory = throughput_model(batch_smooth, *params_t)     
        
        # Overhead prediction: funzione esponenziale negativa O(x) = a * exp(-b*x) + c
        def overhead_model(x, a, b, c):
            return a * np.exp(-b * x) + c
                
        # Fit sui dati reali       
        params_o, _ = curve_fit(overhead_model, batch_sizes, overheads, p0=[20, 0.1, 8])
        overhead_theory = overhead_model(batch_smooth, *params_o)

        # Plot
        # Asse X
        ax1.set_xlabel('Number of Messages per Batch', fontsize=12)
        
        # Asse Y sinistro: Throughput
        ax1.set_ylabel('Throughput of Packets (MB/s)', fontsize=12, color='blue')
        
        # Throughput prediction
        ax1.plot(batch_smooth, throughput_theory, '-', color='blue', linewidth=1.5, alpha=0.3, zorder=1, label='Throughput prediction')
        
        # Marker throughput 
        ax1.plot(batch_sizes, throughputs, '+', color='blue', markersize=10, markeredgewidth=2, zorder=3, label='Experiment result (Throughput)')
        
        ax1.tick_params(axis='y', labelcolor='blue')
        ax1.grid(True, alpha=0.3)
        
        # Asse Y destro: Overhead
        ax2 = ax1.twinx()
        ax2.set_ylabel('Overhead (%)', fontsize=12, color='red')
        
        # Overhead prediction
        ax2.plot(batch_smooth, overhead_theory, '-', color='red', linewidth=1.5, alpha=0.3, zorder=1, label='Overhead prediction')
        
        # Marker overhead
        ax2.plot(batch_sizes, overheads, 'o', color='red', markersize=6, markerfacecolor='none', markeredgewidth=1.5, zorder=3, label='Experiment result (Overhead)')
        
        ax2.tick_params(axis='y', labelcolor='red')
        
        # Info sul numero di partizioni
        ax1.text(0.5, 1.05, f'Partition Number = {partition_count}', transform=ax1.transAxes, fontsize=11, ha='center')

        if idx == 0:
            lines1, labels1 = ax1.get_legend_handles_labels()
            lines2, labels2 = ax2.get_legend_handles_labels()   
    
    fig.legend(lines1 + lines2, labels1 + labels2, loc='lower center', bbox_to_anchor=(0.5, -0.05), ncol=4, frameon=True, fontsize=10)
    
    plt.tight_layout()
    plt.subplots_adjust(bottom=0.18, wspace=0.3)  # wspace = spazio tra subplot
    
    plots_dir = Path(__file__).parent / "plots"
    plots_dir.mkdir(parents=True, exist_ok=True)
    plt.savefig(plots_dir / "network_bandwidth_evaluation.png", dpi=300, bbox_inches='tight')
    
    print(f"\n✓ Plot saved!")

if __name__ == "__main__":
    main()
