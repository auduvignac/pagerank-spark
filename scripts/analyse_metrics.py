#!/usr/bin/env python3
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from pathlib import Path
import argparse

# === 1. Arguments ===
parser = argparse.ArgumentParser(description="Analyse PageRank Spark — opérations cœur avec et sans GC (valeurs internes)")
parser.add_argument("--input", required=True, help="Chemin vers le fichier CSV des métriques")
parser.add_argument("--output", default="benchmark_results", help="Répertoire de sortie des graphiques")
args = parser.parse_args()

input_path = Path(args.input)
if not input_path.exists():
    raise FileNotFoundError(f"Fichier introuvable : {input_path}")

out_dir = Path(args.output)
out_dir.mkdir(parents=True, exist_ok=True)

# === 2. Chargement ===
def load_metrics(path):
    df = pd.read_csv(path)
    for col in ["durationSec", "gcSec"]:
        df[col] = df[col].astype(float)
    return df

df = load_metrics(input_path)

# === 3. Déterminer le type d'implémentation ===
def get_type(job):
    if "RDD-Partitioned" in job:
        return "RDD partitionné"
    elif "RDD-" in job:
        return "RDD classique"
    elif "DF-Partitioned" in job:
        return "DataFrame partitionné"
    elif "DF-" in job:
        return "DataFrame"
    else:
        return "Autre"

df["Implémentation"] = df["jobGroup"].apply(get_type)

# === 4. Identifier le type d’opération ===
def op_type(name):
    if "flatMap" in name: return "flatMap"
    if "map" in name: return "map"
    if "sum" in name: return "sum"
    if "count" in name: return "count"
    return "autre"

df["op"] = df["name"].apply(op_type)

# === 5. Filtrer les opérations cœur ===
core_ops = ["flatMap", "map", "sum", "count"]
df_core = df[df["op"].isin(core_ops)].copy()

# === 6. Agrégation par implémentation et opération ===
op_summary = (
    df_core.groupby(["Implémentation", "op"], as_index=False)
    .agg({
        "durationSec": "sum",
        "gcSec": "sum"
    })
)

# === 7. Palette de couleurs ===
colors = {
    "RDD classique": "#1f77b4",
    "RDD partitionné": "#a02c94",
    "DataFrame": "#ff0e0e",
    "DataFrame partitionné": "#ff7f0e"
}

impls = ["RDD classique", "RDD partitionné", "DataFrame", "DataFrame partitionné"]
ops = core_ops
bar_width = 0.18
x = np.arange(len(ops))

# === 8. FIGURE 1 : Diagramme sans GC (valeurs dans la barre) ===
fig, ax = plt.subplots(figsize=(12, 6))
for i, impl in enumerate(impls):
    impl_data = op_summary[op_summary["Implémentation"] == impl]
    durations = [impl_data.loc[impl_data["op"] == op, "durationSec"].sum() for op in ops]
    xpos = x + i * bar_width - 1.5 * bar_width
    bars = ax.bar(
        xpos,
        durations,
        width=bar_width,
        color=colors.get(impl, "#999999"),
        label=impl
    )
    # Valeurs dans la barre (arrondies)
    for bar, val in zip(bars, durations):
        if val > 0:
            ax.text(
                bar.get_x() + bar.get_width()/2,
                bar.get_height()/2,
                f"{val:.0f}",
                ha="center", va="center",
                fontsize=8, color="white", fontweight="bold"
            )

ax.set_xticks(x)
ax.set_xticklabels(ops)
ax.set_ylabel("Temps d'exécution (secondes)")
ax.set_title("Durée cumulée par type d'opération (sans GC)")
ax.legend(title="Implémentation", loc="upper right")
ax.grid(axis="y", linestyle="--", alpha=0.6)
plt.tight_layout()
path_no_gc = out_dir / "ops_duration_core_nogc_labels.png"
plt.savefig(path_no_gc, dpi=150)
plt.close()
print(f"✅ Graphique (sans GC) : {path_no_gc.resolve()}")

# === 9. FIGURE 2 : Diagramme avec GC empilé (valeurs dans chaque partie) ===
fig, ax = plt.subplots(figsize=(12, 6))
for i, impl in enumerate(impls):
    impl_data = op_summary[op_summary["Implémentation"] == impl]
    durations = [impl_data.loc[impl_data["op"] == op, "durationSec"].sum() for op in ops]
    gc = [impl_data.loc[impl_data["op"] == op, "gcSec"].sum() for op in ops]
    xpos = x + i * bar_width - 1.5 * bar_width

    # Partie utile
    bars_main = ax.bar(
        xpos,
        durations,
        width=bar_width,
        color=colors.get(impl, "#999999"),
        label=impl
    )

    # Partie GC empilée
    bars_gc = ax.bar(
        xpos,
        gc,
        width=bar_width,
        bottom=durations,
        color="limegreen",
        alpha=0.6
    )

    # Valeurs dans la partie utile
    for bar, val in zip(bars_main, durations):
        if val > 0:
            ax.text(
                bar.get_x() + bar.get_width()/2,
                bar.get_y() + bar.get_height()/2,
                f"{val:.0f}",
                ha="center", va="center",
                fontsize=8, color="white", fontweight="bold"
            )

    # Valeurs dans la partie GC
    for bar, val, g in zip(bars_main, durations, gc):
        if g > 0:
            ax.text(
                bar.get_x() + bar.get_width()/2,
                bar.get_y() + bar.get_height() + g/2,
                f"{g:.0f}",
                ha="center", va="center",
                fontsize=8, color="black", fontweight="bold"
            )

ax.set_xticks(x)
ax.set_xticklabels(ops)
ax.set_ylabel("Temps total (secondes)")
ax.set_title("Durée par type d'opération (utile + GC empilé, valeurs affichées)")
ax.legend(title="Implémentation", loc="upper right")
ax.grid(axis="y", linestyle="--", alpha=0.6)
plt.tight_layout()
path_with_gc = out_dir / "ops_duration_core_with_gc_labels.png"
plt.savefig(path_with_gc, dpi=150)
plt.close()
print(f"✅ Graphique (avec GC) : {path_with_gc.resolve()}")