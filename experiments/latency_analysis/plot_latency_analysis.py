import json
import sys
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
from scipy.optimize import curve_fit
from mpl_toolkits.axes_grid1.inset_locator import inset_axes
from pathlib import Path
import math

sys.path.append(str(Path(__file__).parent.parent.parent / "utilities"))
from tukey_iqr import tukey_iqr_filter
from confidence_interval import compute_ci

REPORTS_DIR = Path(__file__).parent / "reports"
PLOTS_DIR   = Path(__file__).parent / "plots"

def load_reports() -> pd.DataFrame:
    '''Loads all report JSON files, extract relevant fields, and return a DataFrame with one row per run.'''
    rows = []
    for f in REPORTS_DIR.glob("latency_report_*.json"):
        data = json.loads(f.read_text())
        if "error" in data:
            continue
        rows.append({
            "lambda": data["lambda"],
            "nb": data["nb"],
            "partitions": data["partitions"],
            "run_number": data["run_number"],
            "mean": data["global"]["latency_mean_ms"],
            "std": data["global"]["latency_std_ms"],
            "p25": data["global"]["latency_p25_ms"],
            "p50": data["global"]["latency_p50_ms"],
            "p75": data["global"]["latency_p75_ms"],
            "p95": data["global"]["latency_p95_ms"],
            "p99": data["global"]["latency_p99_ms"],
            "iqr": data["global"]["latency_iqr_ms"],
            "min": data["global"]["latency_min_ms"],
            "max": data["global"]["latency_max_ms"],
        })
    return pd.DataFrame(rows)

def aggregate(df: pd.DataFrame) -> pd.DataFrame:
    """
    For each (lambda, nb, partitions):
    - filters outlier runs using Tukey IQR on mean latency (anchor)
    - computes mean + CI with t-distribution on all metrics
    Produces a DataFrame with one row per configuration, compatible with all 4 plots.
    """
    metrics = ["mean", "std", "p25", "p50", "p75", "p95", "p99", "iqr", "min", "max"]
    results = []

    for (lam, nb, parts), group in df.groupby(["lambda", "nb", "partitions"]):

        # IQR on mean latency to filter out runs with anomalous mean latency (anchor)
        anchor = group["mean"].values
        _, mask = tukey_iqr_filter(anchor)
        if mask.sum() == 0:
            mask = np.ones(len(anchor), dtype=bool)  # if all are outliers, keep all 

        group_filtered = group[mask] # apply filter to all metrics

        row = {"lambda": lam, "nb": nb, "partitions": parts}

        for metric in metrics:
            vals = group_filtered[metric].values
            ci = compute_ci(vals)
            row[metric] = ci["mean"] # mean
            row[f"{metric}_ci_half"] = ci["ci_half"] # CI 95% t-distribution

        # alias for compatibility with plotting functions
        row["mean_latency_ms"] = row["mean"]
        row["ci_half"] = row["mean_ci_half"]
        row["std_latency_ms"] = row["std"]

        results.append(row)

    return pd.DataFrame(results)

def plot_latency_vs_nb(agg: pd.DataFrame):
    '''Plot with subplots for each partition count. X=Nb, Y=mean latency (s). Line + points. Linear fit per lambda.'''
    partition_counts = sorted(agg["partitions"].unique())
    lambda_values = sorted(agg["lambda"].unique())

    fig, axes = plt.subplots(1, len(partition_counts), figsize=(5 * len(partition_counts), 5))
    axes = axes.flatten()

    colors = plt.cm.tab10.colors

    # Simple linear model: latency = a * Nb + c
    def linear_model(x, a, c):
        return a * x + c

    for idx, parts in enumerate(partition_counts):
        ax = axes[idx]
        sub_df = agg[agg["partitions"] == parts]

        for lam_idx, lam in enumerate(lambda_values):
            lam_df = sub_df[sub_df["lambda"] == lam].sort_values("nb")
            if lam_df.empty:
                continue

            color = colors[lam_idx % len(colors)]
            x = lam_df["nb"].values.astype(float)
            y = lam_df["mean_latency_ms"].values / 1000 # convert to seconds 

            ax.plot(x, y, color=color)
            ax.scatter(x, y, s=30, color=color, edgecolors="black", linewidths=0.5, zorder=3)

            if len(x) >= 2:
                popt, _ = curve_fit(
                    linear_model, x, y,
                    p0=[y[-1] / x[-1], y[0]],
                    maxfev=5000,
                )
                x_fit = np.linspace(x.min(), x.max(), 400)
                y_fit = linear_model(x_fit, *popt)
                
                # Calculate R² for the linear fit
                residuals = y - linear_model(x, *popt)
                ss_res = np.sum(residuals**2)
                ss_tot = np.sum((y - np.mean(y))**2)
                r_squared = 1 - ss_res / ss_tot if ss_tot > 0 else 1.0
                ax.plot(x_fit, y_fit, color=color, linestyle='--', linewidth=2, alpha=0.5, label=f"λ={lam}  R²={r_squared:.3f}")
            else:
                ax.plot(x, y, color=color, linestyle='--', linewidth=2, alpha=0.5, label=f"λ={lam}")

        ax.set_title(f"Partition number = {parts}", fontsize=8, fontweight="bold")
        ax.set_xlabel("Number of Messages per Batch", fontsize=8)
        ax.set_ylabel("Latency (s)", fontsize=8)
        ax.tick_params(axis='both', labelsize=7)
        ax.legend(title="λ (msg/s)", fontsize=7, title_fontsize=8)
        ax.grid(True, alpha=0.3)

    for idx in range(len(partition_counts), len(axes)):
        axes[idx].set_visible(False)

    
    fig.suptitle('Latency Analysis: Latency vs Nb', fontsize=12, fontweight='bold')
    plt.tight_layout()
    PLOTS_DIR.mkdir(parents=True, exist_ok=True)
    out = PLOTS_DIR / "latency_vs_nb.png"
    plt.savefig(out, dpi=300, bbox_inches="tight")
    plt.close()
    print(f"  ✓ Plot Latency vs Nb saved!")

def plot_heatmap_of_latency_vs_nb(agg: pd.DataFrame):
    '''Heatmap with subplots for each partition count. X=Nb, Y=lambda, color=mean latency (s).'''
    partition_counts = sorted(agg["partitions"].unique())
    lambda_values = sorted(agg["lambda"].unique())
    nb_values = sorted(agg["nb"].unique())

    n_cols = len(partition_counts)
    fig, axes = plt.subplots(1, len(partition_counts), figsize=(5 * len(partition_counts), 5))
    if n_cols == 1:
        axes = [axes]

    agg = agg.copy()
    agg["mean_latency_s"] = agg["mean_latency_ms"] / 1000.0 # convert to seconds 

    for idx, parts in enumerate(partition_counts):
        ax = axes[idx]
        sub_df = agg[agg["partitions"] == parts]
        
        # Matrix with NaN for missing values, to ensure correct heatmap shape and color scaling
        matrix = np.full((len(lambda_values), len(nb_values)), np.nan)
        for i, lam in enumerate(lambda_values):
            for j, nb in enumerate(nb_values):
                val = sub_df[
                    (sub_df["lambda"] == lam) & (sub_df["nb"] == nb)
                ]["mean_latency_s"].values
                if len(val) > 0:
                    matrix[i, j] = val[0]

        local_min = np.nanmin(matrix)
        local_max = np.nanmax(matrix)

        im = ax.imshow(
            matrix,
            aspect="auto",
            origin="lower",
            cmap="RdYlGn_r",
            vmin=local_min, 
            vmax=local_max, 
        )

        ax.set_xticks(range(len(nb_values)))
        ax.set_xticklabels([str(n) for n in nb_values], ha="right", fontsize=8)
        ax.set_yticks(range(len(lambda_values)))
        ax.set_yticklabels([f"λ={l}" for l in lambda_values], fontsize=8)
        ax.set_xlabel("Number of Messages per Batch", fontsize=8)
        ax.set_ylabel("λ (msg/s)", fontsize=8)
        ax.set_title(f"Partition number = {parts}", fontsize=8, fontweight="bold")

        cbar = fig.colorbar(im, ax=ax, shrink=0.8, location="bottom", pad=0.15)
        cbar.set_label("Mean Latency (s)", fontsize=8)
        cbar.ax.tick_params(labelsize=8)
        
    plt.subplots_adjust(wspace=0.3)
    fig.suptitle('Latency Analysis: Heatmap of Latency vs Nb', fontsize=12, fontweight='bold')
    PLOTS_DIR.mkdir(parents=True, exist_ok=True)
    out = PLOTS_DIR / "latency_heatmap_of_latency_vs_nb.png"
    plt.savefig(out, dpi=300, bbox_inches="tight")
    plt.close()
    print(f"  ✓ Plot Heatmap of Latency vs Nb saved!")

def plot_latency_vs_lambda(agg: pd.DataFrame):
    '''Plot with subplots for each partition count. X=lambda (log scale), Y=mean latency (s). Line + points. Hyperbolic fit per Nb. Zoom su Nb=1,10,15.'''
    partition_counts = sorted(agg["partitions"].unique())
    lambda_values    = sorted(agg["lambda"].unique())

    nb_all = sorted(agg["nb"].unique())
    nb_sample_indices = np.round(np.linspace(0, len(nb_all) - 1, 6)).astype(int)
    nb_sample = [nb_all[i] for i in nb_sample_indices]

    fig, axes = plt.subplots(1, len(partition_counts), figsize=(6 * len(partition_counts), 6))
    if len(partition_counts) == 1:
        axes = [axes]

    colors = plt.cm.tab10.colors
    
    nb_color_map = {
        nb: colors[i % len(colors)]
        for i, nb in enumerate(nb_sample)
    }

    # hyperbolic model
    def hyperbolic(x, a, c):
        return a / x + c

    for idx, parts in enumerate(partition_counts):
        ax = axes[idx]
        sub_df = agg[agg["partitions"] == parts]

        for nb_idx, nb in enumerate(nb_sample):
            nb_df = sub_df[sub_df["nb"] == nb].sort_values("lambda")
            if nb_df.empty:
                continue

            color = nb_color_map[nb]
            x = nb_df["lambda"].values.astype(float) 
            y = nb_df["mean_latency_ms"].values / 1000 # convert to seconds

            ax.scatter(x, y, s=30, color=color, edgecolors="black", linewidths=0.5, zorder=3)

            if len(x) >= 4:
                popt, _ = curve_fit(
                    hyperbolic, x, y,  
                    p0=[y.mean() * x.mean(), y.min()], 
                    maxfev=5000,
                    bounds=([0, 0], [np.inf, np.inf]),
                )
                x_fit = np.linspace(x.min(), x.max(), 400)
                y_fit = hyperbolic(x_fit, *popt) 
                ax.plot(x_fit, y_fit, color=color, label=f"Nb={nb}")
            else:
                ax.plot(x, y, color=color, alpha=0.5, label=f"Nb={nb}")
                

        ax.set_title(f"Partition number = {parts}", fontsize=8, fontweight="bold")
        ax.set_xlabel("λ (msg/s)", fontsize=8)
        ax.set_ylabel("Latency (s)", fontsize=8)
        ax.set_xscale("log")
        ax.set_xticks(lambda_values)
        ax.tick_params(axis='both', labelsize=7)
        ax.get_xaxis().set_major_formatter(ticker.ScalarFormatter())
        ax.legend(title="Number of Messages per Batch", fontsize=7, loc="upper right", title_fontsize=8)
        ax.grid(True, alpha=0.3)
        
        # Zoom on Nb=1,10,15 
        nb_zoom = [1, 10, 15]
        axins = inset_axes(
            ax,
            width="40%",
            height="40%",
            loc="center right",
            borderpad=1.2
        )

        for nb_idx, nb in enumerate(nb_zoom):
            nb_df = sub_df[sub_df["nb"] == nb].sort_values("lambda")
            if nb_df.empty:
                continue

            if nb not in nb_color_map:
                continue
            color = nb_color_map[nb]
            x = nb_df["lambda"].values.astype(float)
            y = nb_df["mean_latency_ms"].values / 1000

            axins.scatter(x, y, s=18, color=color, edgecolors="black", linewidths=0.4, zorder=3)

            if len(x) >= 4:
                popt, _ = curve_fit(
                    hyperbolic,
                    x, y,
                    p0=[y.mean() * x.mean(), y.min()],
                    maxfev=5000,
                    bounds=([0, 0], [np.inf, np.inf]),
                )
                x_fit = np.linspace(x.min(), x.max(), 300)
                y_fit = hyperbolic(x_fit, *popt)
                axins.plot(x_fit, y_fit, color=color, linewidth=1.2)
            else:
                axins.plot(x, y, color=color, linewidth=1.2)

        axins.tick_params(axis='both', labelsize=6)
        
        axins.grid(True, alpha=0.3)
        
        axins.set_title("Zoom Nb=1,10,15", fontsize=6)

        for spine in axins.spines.values():
            spine.set_edgecolor("black")
            spine.set_linewidth(0.8)
            
        axins2 = inset_axes(
            axins,
            width="55%",
            height="55%",
            loc="upper right",
            borderpad=0.8
        )

        nb_focus = 1
        nb_df = sub_df[sub_df["nb"] == nb_focus].sort_values("lambda")

        if not nb_df.empty and nb_focus in nb_color_map:
            color = nb_color_map[nb_focus]
            x = nb_df["lambda"].values.astype(float)
            y = nb_df["mean_latency_ms"].values / 1000

            axins2.scatter(x, y, s=12, color=color, edgecolors="black", linewidths=0.3, zorder=3)

            if len(x) >= 4:
                popt, _ = curve_fit(
                    hyperbolic,
                    x, y,
                    p0=[y.mean() * x.mean(), y.min()],
                    maxfev=5000,
                    bounds=([0, 0], [np.inf, np.inf]),
                )
                x_fit = np.linspace(x.min(), x.max(), 200)
                y_fit = hyperbolic(x_fit, *popt)
                axins2.plot(x_fit, y_fit, color=color, linewidth=1.0)

        x_min, x_max = min(x), max(x)
        y_min, y_max = min(y), max(y)

        x_pad = 0.05 * (x_max - x_min)
        y_pad = 0.15 * (y_max - y_min)

        x_range = x_max - x_min if x_max != x_min else x_min * 0.1 or 1.0
        y_range = y_max - y_min if y_max != y_min else y_max * 0.1 or 0.1
        x_pad = 0.05 * x_range
        y_pad = 0.15 * y_range
        axins2.set_xlim(x_min - x_pad, x_max + x_pad)
        axins2.set_ylim(max(0, y_min - y_pad), y_max + y_pad)

        axins2.tick_params(axis='both', labelsize=5)
        axins2.grid(True, alpha=0.2)

        for spine in axins2.spines.values():
            spine.set_edgecolor("black")
            spine.set_linewidth(0.7)

    fig.suptitle('Latency Analysis: Latency vs λ', fontsize=12, fontweight='bold')

    out = PLOTS_DIR / "latency_vs_lambda.png"
    plt.savefig(out, dpi=300, bbox_inches="tight")
    plt.close()
    print(f"  ✓ plot Latency vs lambda saved!")

def plot_boxplot_of_latency_vs_nb(agg: pd.DataFrame):
    '''Boxplot with subplots for each partition count. X=Nb, Y=latency (s). Boxplot per lambda.'''
    partition_counts = sorted(agg["partitions"].unique())
    lambda_values = sorted(agg["lambda"].unique())

    nb_all = sorted(agg["nb"].unique())
    nb_sample = sorted(agg["nb"].unique())

    n_rows = len(partition_counts)
    n_cols = len(lambda_values)

    colors = plt.cm.tab10.colors

    fig, axes = plt.subplots(
        n_rows, n_cols,
        figsize=(5 * n_cols, 4 * n_rows),
        sharey=False, sharex=True,
    )
    if n_rows == 1: axes = [axes]
    if n_cols == 1: axes = [[ax] for ax in axes]

    for row, parts in enumerate(partition_counts):
        for col, lam in enumerate(lambda_values):
            ax = axes[row][col]

            sub_df = agg[
                (agg["partitions"] == parts) & (agg["lambda"] == lam)
            ]

            box_data  = []
            positions = []

            for nb_idx, nb in enumerate(nb_sample):
                vals = sub_df[sub_df["nb"] == nb]

                if vals.empty:
                    continue

                vals = vals.iloc[0]
                
                box_data.append({
                    "med": vals["p50"] / 1000.0,
                    "q1": vals["p25"] / 1000.0,   
                    "q3": vals["p75"] / 1000.0,   
                    "whislo": vals["min"] / 1000.0,
                    "whishi": vals["max"] / 1000.0,
                })
                positions.append(nb_idx)

            if not box_data:
                ax.set_visible(False)
                continue

            bp = ax.bxp(
                box_data,
                positions=positions,
                widths=0.5,
                showfliers=False,
                patch_artist=True,
            )
            
            color = colors[col % len(colors)] 
            for patch in bp["boxes"]:
                patch.set_facecolor(color)
                patch.set_alpha(0.6)
            for median in bp["medians"]:
                median.set_color("black")
                median.set_linewidth(1.5)
            for whisker in bp["whiskers"]:
                whisker.set_linestyle("--")
                whisker.set_linewidth(0.9)

            ax.set_xticks(range(len(nb_sample)))
            ax.set_xticklabels(
                [str(n) for n in nb_sample],
                fontsize=7, ha="right"
            )
            ax.tick_params(axis='y', labelsize=7)
            ax.grid(True, alpha=0.3, axis="y")

            if row == 0:
                ax.set_title(f"λ={lam}", fontsize=8)
            if col == 0:
                plt.rcParams["mathtext.fontset"] = "custom"
                plt.rcParams["mathtext.bf"] = "sans:bold"
                ax.set_ylabel(r"$\bf{Partition\ Number\ =\ " + str(parts) + r"}$" + "\n\nLatency (s)", fontsize=8)
            if row == n_rows - 1:
                ax.set_xlabel("Number of Messages per Batch", fontsize=8)

    fig.suptitle('Latency Analysis: Boxplot of Latency vs Nb', fontsize=12, fontweight='bold')
    plt.tight_layout()
    out = PLOTS_DIR / "latency_boxplot_of_latency_vs_nb.png"
    plt.savefig(out, dpi=300, bbox_inches="tight")
    plt.close()
    print(f"  ✓ Plot Boxplot of Latency vs Nb saved!")
   
def plot_latency_ratio_vs_partition_number(agg: pd.DataFrame):
    '''Table with subplots for each Nb. X=partition count, Y=lambda, cell=mean latency ratio vs P_BASE (s).'''
    
    partition_counts = sorted(agg["partitions"].unique())
    lambda_values = sorted(agg["lambda"].unique())
    P_BASE = partition_counts[0]
    nb_all = sorted(agg["nb"].unique())
    
    n_cols, n_rows = 3, math.ceil(len(nb_all) / 3)
    fig, axes = plt.subplots(n_rows, n_cols, figsize=(5.5*n_cols, 3.2*n_rows), facecolor='white')
    axes = axes.flatten()
    
    col_labels = [f"P={p}" for p in partition_counts]
    cmap = plt.cm.summer_r  
    
    for nb_idx, nb in enumerate(nb_all):
        ax = axes[nb_idx]
        ax.axis("off"); ax.set_facecolor("#f8f9fa")
        sub = agg[agg["nb"] == nb]
        
        ratios_matrix, cell_text = [], []
        for lam in lambda_values:
            if sub[sub["lambda"] == lam].empty: continue
            lam_df = sub[sub["lambda"] == lam].sort_values("partitions")
            base_val = lam_df[lam_df["partitions"] == P_BASE]["mean_latency_ms"].iloc[0]
            
            row_ratios, row_text = [], []
            for p in partition_counts:
                val = lam_df[lam_df["partitions"] == p]["mean_latency_ms"]
                row_ratios.append(np.nan if val.empty else val.iloc[0] / base_val)
                row_text.append("—" if val.empty else f"×{row_ratios[-1]:.2f}")
            
            ratios_matrix.append(row_ratios)
            cell_text.append(row_text)
        
        if not cell_text: 
            ax.set_visible(False); continue
        
        ratios_arr = np.array(ratios_matrix)
        row_labels = [f"λ={lam}" for lam in lambda_values if not sub[sub["lambda"] == lam].empty]
        
        cell_colors = [["#f8f9fa"] * len(partition_counts) for _ in cell_text]
        for j in range(len(partition_counts)):
            col_vals = ratios_arr[:, j]
            valid = ~np.isnan(col_vals)
            if not valid.any(): continue
            col_min, col_max = np.nanmin(col_vals), np.nanmax(col_vals)
            norm = lambda v: 0.5 if col_max == col_min else (v - col_min) / (col_max - col_min)
            for i in np.where(valid)[0]:
                cell_colors[i][j] = cmap(norm(ratios_arr[i, j]))
        
        table = ax.table(cellText=cell_text, cellColours=cell_colors, rowLabels=row_labels,
                        colLabels=col_labels, cellLoc="center", loc="center", bbox=[0.05, 0.05, 0.9, 0.9])
        table.auto_set_font_size(False); table.set_fontsize(8); table.scale(1.3, 1.5)
        
        for (r, c), cell in table.get_celld().items():
            cell.set_linewidth(0.5); cell.set_edgecolor("#dee2e6")
            if r == 0 or c == -1:
                cell.set_facecolor("#423A38"); cell.set_text_props(weight="bold", color="#f8fafc")
        
        ax.set_title(f"Number of Messages per Batch = {nb}", fontsize=9, fontweight="bold")
    
    for i in range(len(nb_all), len(axes)): axes[i].set_visible(False)
    fig.suptitle("Latency Ratio vs Partition Number", fontsize=12, fontweight="bold")
    plt.tight_layout()
    
    out = PLOTS_DIR / "latency_ratio_vs_partition_number.png"
    PLOTS_DIR.mkdir(parents=True, exist_ok=True)
    plt.savefig(out, dpi=300, bbox_inches="tight"); plt.close()
    print(f"  ✓ Plot Latency ratio vs partition number saved!")

  
def main():
    '''Main function to load data, aggregate, and generate all plots.'''
    df = load_reports()
    agg = aggregate(df)
    
    plot_latency_vs_nb(agg)
    plot_latency_vs_lambda(agg)
    plot_heatmap_of_latency_vs_nb(agg)
    plot_boxplot_of_latency_vs_nb(agg)   
    plot_latency_ratio_vs_partition_number(agg)
    
if __name__ == "__main__":
    main()
