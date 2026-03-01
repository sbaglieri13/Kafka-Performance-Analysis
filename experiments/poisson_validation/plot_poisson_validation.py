import json
import numpy as np
import matplotlib.pyplot as plt
from pathlib import Path
from scipy import stats


def load_send_timestamps(data_dir: Path) -> dict:
    """
    Loads send_timestamp_ms from producer recap files.
    Returns {producer_name: [t1, t2, ...]} sorted.
    """
    producer_files = list(data_dir.glob("producer_*_messages_recap.jsonl"))
    result = {}
    for f in producer_files:
        producer_name = f.stem.split("_")[0] + "_" + f.stem.split("_")[1]
        timestamps = []
        with open(f) as fp:
            for line in fp:
                record = json.loads(line)
                ts = record.get("send_timestamp_ms")
                if ts is not None:
                    timestamps.append(ts)
        result[producer_name] = sorted(timestamps)
    return result

def load_lambda_rate_from_stats(data_dir: Path) -> float:
    """
    Reads lambda_rate from producer stats file
    """
    stats_dir = data_dir / "stats"
    stats_files = list(stats_dir.glob("producer_*_stats.json"))
    
    if stats_files:
        with open(stats_files[0]) as f:
            stats = json.load(f)
        lambda_rate = stats.get("experiment_config", {}).get("lambda_rate")
        if lambda_rate is not None:
            return float(lambda_rate)
        else:
            print("!!! 'lambda_rate' not found in stats file !!!")
    else:
        print("!!! No stats file found !!!")

def compute_interarrivals(timestamps: list) -> np.ndarray:
    """Computes inter-arrival times in seconds."""
    ts = np.array(timestamps) / 1000.0  # ms to s
    return np.diff(ts)


def plot_poisson_validation(data_dir: Path, lambda_rate: float, load_label: str, output_dir: Path):
    producers_ts = load_send_timestamps(data_dir)

    if not producers_ts:
        print("No producer recap files found")
        return

    fig, axes = plt.subplots(len(producers_ts), 3, figsize=(18, 5 * len(producers_ts)))
    if len(producers_ts) == 1:
        axes = [axes]

    fig.suptitle(
        f'Poisson Arrival Validation — {load_label} load  '
        f'(λ = {lambda_rate} msg/s,  theoretical inter-arrival = '
        f'{1000/lambda_rate:.1f}ms)',
        fontsize=13, fontweight='bold'
    )

    for row_idx, (producer_name, timestamps) in enumerate(producers_ts.items()):
        interarrivals_raw  = compute_interarrivals(timestamps)
        theoretical_mean = 1.0 / lambda_rate
        
        # Removes outliers based on theoretical Exp(lambda) distribution threshold:
        # removes inter-arrival times where P(X > x) < alpha, i.e., x > -ln(alpha)/lambda
        # These events don't belong to the Poisson process but to OS scheduler noise (sleep jitter)
        # and system effects
        alpha = 0.01 # Significance threshold for defining outliers
        x_thr = -np.log(alpha) / lambda_rate # threshold of inter-arrival time beyond which we consider an outlier
        interarrivals = interarrivals_raw[interarrivals_raw <= x_thr] # remove outliers

        outliers_removed = len(interarrivals_raw) - len(interarrivals)
        outlier_pct = outliers_removed / len(interarrivals_raw) * 100
        print(f"{producer_name}: {len(interarrivals_raw)} inter-arrivals, "
            f"{outliers_removed} outliers removed ({outlier_pct:.2f}%) "
            f"| x_thr = {x_thr*1000:.2f}ms (alpha={alpha})")
        
        window = 10.0 / lambda_rate # Time window for message counting (10x theoretical inter-arrival)
        ts_s = np.array(timestamps) / 1000.0 
        bins = np.arange(ts_s.min(), ts_s.max() + window, window)
        counts, _ = np.histogram(ts_s, bins=bins) # Message count per time window
        
        ax_hist, ax_qq, ax_count = axes[row_idx]

        # Plot 1 - Inter-arrival histogram + Exponential PDF
        ax_hist.set_title(f'{producer_name} — Inter-arrival Distribution', fontsize=10)
        ax_hist.hist(interarrivals, bins=50, density=True, alpha=0.6, color='steelblue', label='Empirical')
        x = np.linspace(0, interarrivals.max(), 300)
        ax_hist.plot(x, lambda_rate * np.exp(-lambda_rate * x), 'r-', linewidth=2,  label=f'Exp(λ={lambda_rate}) theoretical')
        ax_hist.set_xlabel('Inter-arrival time (s)')
        ax_hist.set_ylabel('Density')
        ax_hist.legend(fontsize=8)
        ax_hist.grid(True, alpha=0.3)
        
        # Plot 2 - Q-Q plot vs Exponential
        ax_qq.set_title(f'{producer_name} — Exponential Q-Q Plot', fontsize=10)
        
        # Theoretical quantiles for Exponential distribution with mean = 1/lambda
        theoretical_quantiles = stats.expon.ppf(
            np.linspace(0.01, 0.99, len(interarrivals)),
            scale=theoretical_mean
        )
        empirical_quantiles = np.sort(interarrivals)
        ax_qq.scatter(theoretical_quantiles, empirical_quantiles, s=8, alpha=0.5, color='steelblue')
        lim = max(theoretical_quantiles.max(), empirical_quantiles.max())
        ax_qq.plot([0, lim], [0, lim], 'r--', linewidth=1.5, label='y = x (ideal)')
        ax_qq.set_xlabel('Theoretical quantiles (Exponential)')
        ax_qq.set_ylabel('Empirical quantiles')
        ax_qq.legend(fontsize=8)
        ax_qq.grid(True, alpha=0.3)

        # Plot 3 - Count distribution per time window vs Poisson PMF
        window_ms = window * 1000 # to ms
        window_label = f"{window_ms:.0f}ms" if window_ms < 1000 else f"{window:.1f}s"

        unique, freq = np.unique(counts, return_counts=True)
        freq_norm = freq / freq.sum()
        ax_count.bar(unique, freq_norm, alpha=0.6, color='steelblue', label='Empirical count distribution')

        k_range = np.arange(0, counts.max() + 5) # Range of k for PMF (0 to max count + margin)
        poisson_pmf = stats.poisson.pmf(k_range, mu=lambda_rate * window) # Theoretical PMF for Poisson with mean λ*window
        ax_count.plot(k_range, poisson_pmf, 'ro-', markersize=4, linewidth=1.5, label=f'Poisson(μ={(lambda_rate * window):.1f}) PMF')
        ax_count.set_title(f'{producer_name} — Message Count per {window_label} Window', fontsize=10)
        ax_count.set_xlabel(f'Messages per {window_label} window')
        ax_count.set_ylabel('Frequency (normalized)')
        ax_count.legend(fontsize=8)
        ax_count.grid(True, alpha=0.3)

    plt.tight_layout()
    
    output_dir.mkdir(parents=True, exist_ok=True)
    out_file = output_dir / "poisson_validation.png"
    plt.savefig(out_file, dpi=300, bbox_inches='tight')
    plt.close()
    print(f"✓ Saved: {out_file}")

def main():
    data_dir = Path(__file__).parent.parent.parent / "data"
    output_dir = Path(__file__).parent / "plots"
    
    # Determine load category based on lambda_rate
    lambda_rate = load_lambda_rate_from_stats(data_dir)
    if lambda_rate <= 20:
        load_label = "Low"
    elif lambda_rate <= 100:
        load_label = "Medium"
    else:
        load_label = "High"
    print(f">> Load category: {load_label} (λ={lambda_rate} msg/s)")
    plot_poisson_validation(data_dir, lambda_rate, load_label, output_dir)

if __name__ == "__main__":
    main()
