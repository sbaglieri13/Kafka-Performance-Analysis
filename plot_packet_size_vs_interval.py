import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from pathlib import Path

data_path = Path("data") / "experiment_summary.csv"
df = pd.read_csv(data_path)

NB_LIST = [1, 10, 20, 50] # valori di messages_per_batch da plottare
PARTITIONS_TO_PLOT = [3,6,9,12] # valori di partition_count da plottare

markers = {3: "o", 6: "x", 9: "^", 12: "s"}
colors = {3: "tab:blue", 6: "tab:orange", 9: "tab:green", 12: "tab:red"}
labels = {
    3: "partition number = 3",
    6: "partition number = 6",
    9: "partition number = 9",
    12: "partition number = 12",
}

# crea figura con 4 subplot (2x2)
fig, axes = plt.subplots(2, 2, figsize=(10, 8), sharex=True, sharey=True)
axes = axes.flatten()

# ciclo sui subplot per ogni valore di messages_per_batch
for ax, NB in zip(axes, NB_LIST):
    sub_nb = df[df["messages_per_batch"] == NB]
    sub_nb = sub_nb[sub_nb["partition_count"].isin(PARTITIONS_TO_PLOT)]
    sub_nb = sub_nb.sort_values(["partition_count", "payload_size_bytes"])

    r2_texts = []  # raccoglie gli R2 per questo Nb

    for p in sorted(sub_nb["partition_count"].unique()):
        sub = sub_nb[sub_nb["partition_count"] == p]
        if sub.empty:
            continue

        x = sub["payload_size_bytes"].values
        y = sub["send_interval_ms"].values

        ax.plot(
            x, y,
            marker=markers.get(p, "o"),
            linestyle="-",
            color=colors.get(p, None),
            label=labels.get(p, f"partition number = {p}"),
        )

        # calcolo e plot della retta di regressione lineare
        a, b = np.polyfit(x, y, 1)
        y_fit = a * x + b

        ss_res = ((y - y_fit) ** 2).sum()
        ss_tot = ((y - y.mean()) ** 2).sum()
        r2 = 1 - ss_res / ss_tot if ss_tot > 0 else float("nan")
        r2_texts.append(f"p={p}: R²={r2:.3f}")

        ax.plot(
            x, y_fit,
            linestyle="--",
            color=colors.get(p, None),
            alpha=0.7,
        )

    # titolo del subplot con Nb e R2
    ax.set_title(f"Nb = {NB}\n" + " | ".join(r2_texts), fontsize=9)
    ax.grid(True, which="both", linestyle="--", alpha=0.3)

# assi condivisi
for ax in axes[2:]:
    ax.set_xlabel("Packet size (byte)")
for ax in axes[0::2]:
    ax.set_ylabel("Packet send interval (ms)")

for ax in axes:
    ax.set_xlim(0, 6500)
    ax.set_ylim(0, 50)

# legenda comune
handles, leg_labels = axes[0].get_legend_handles_labels()
fig.legend(handles, leg_labels, loc="upper right")

fig.tight_layout()
plt.show()
