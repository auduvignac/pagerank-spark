#!/usr/bin/env python3
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from pathlib import Path
import argparse

def plot_benchmarks(csv_file: Path, output_dir: Path, x_axis: str = "nodes"):
    """
    Génère deux graphiques :
      1. Diagramme en barres avec valeurs arrondies dans chaque barre.
      2. Courbes d'évolution du temps (sans texte sur les points).

    Args:
        csv_file (Path): chemin vers benchmark.csv
        output_dir (Path): dossier de sortie
        x_axis (str): "nodes" ou "edges"
    """
    df = pd.read_csv(csv_file, sep=";")

    # Conversion explicite en numérique
    df["nodes"] = pd.to_numeric(df["nodes"], errors="coerce")
    df["edges"] = pd.to_numeric(df["edges"], errors="coerce")
    df["time"]  = pd.to_numeric(df["time"], errors="coerce")

    if x_axis not in ["nodes", "edges"]:
        raise ValueError("x_axis doit être 'nodes' ou 'edges'")

    # === Palette cohérente avec analyse_pagerank_metrics.py ===
    color_map = {
        "RDD": "#1f77b4",
        "RDD_Partitioned": "#a02c94",
        "DF": "#ff0e0e",
        "DF_Partitioned": "#ff7f0e"
    }

    label_map = {
        "RDD": "RDD classique",
        "RDD_Partitioned": "RDD partitionné",
        "DF": "DataFrame",
        "DF_Partitioned": "DataFrame partitionné"
    }

    # ✅ Ordre cohérent : RDD → RDD_Partitioned → DF → DF_Partitioned
    methods = ["RDD", "RDD_Partitioned", "DF", "DF_Partitioned"]

    # === 1. Diagramme en barres ===
    graphs = df["graph"].unique()
    bar_width = 0.2
    x = np.arange(len(graphs))

    plt.figure(figsize=(10, 6))
    for i, method in enumerate(methods):
        subset = df[df["method"] == method].sort_values(by=x_axis)
        if subset.empty:
            continue

        color = color_map.get(method, "#999999")
        bars = plt.bar(
            x + i * bar_width,
            subset["time"].to_numpy(),
            width=bar_width,
            color=color,
            label=label_map.get(method, method)
        )

        # ✅ Afficher les valeurs dans la barre (arrondies)
        for bar in bars:
            yval = bar.get_height()
            if yval > 0:
                plt.text(
                    bar.get_x() + bar.get_width() / 2,
                    yval / 2,  # centré verticalement dans la barre
                    f"{yval:.0f}",  # arrondi à l’unité
                    ha="center",
                    va="center",
                    fontsize=8,
                    color="white",       # contraste sur fond coloré
                    fontweight="bold"
                )

    plt.xticks(x + bar_width * (len(methods) / 2 - 0.5), graphs, rotation=45, ha="right")
    plt.xlabel("Graphes d'étude", fontsize=12)
    plt.ylabel("Temps d'exécution (secondes)", fontsize=12)
    plt.title("Comparaison des durées PageRank (diagramme en barres)")
    plt.grid(axis="y", linestyle="--", alpha=0.6)

    # ✅ Légende triée dans l'ordre RDD → DF
    handles, labels = plt.gca().get_legend_handles_labels()
    ordered_labels = [label_map[m] for m in methods if label_map[m] in labels]
    ordered_handles = [handles[labels.index(l)] for l in ordered_labels]
    plt.legend(ordered_handles, ordered_labels)

    plt.tight_layout()
    output_file_bar = output_dir / "benchmark_times_bar.png"
    plt.savefig(output_file_bar, dpi=150, bbox_inches="tight")
    plt.close()
    print(f"✅ Diagramme en barres enregistré : {output_file_bar.resolve()}")

    # === 2. Courbes d’évolution (sans texte sur les points) ===
    plt.figure(figsize=(10, 6))
    for method in methods:
        subset = df[df["method"] == method].sort_values(by=x_axis)
        if subset.empty:
            continue
        plt.plot(
            subset[x_axis].to_numpy(),
            subset["time"].to_numpy(),
            marker="o",
            linestyle="--",
            color=color_map.get(method, "#999999"),
            label=label_map.get(method, method)
        )

    plt.xlabel("Nombre de nœuds" if x_axis == "nodes" else "Nombre d'arêtes", fontsize=12)
    plt.ylabel("Temps d'exécution (secondes)", fontsize=12)
    plt.title("Évolution du temps d'exécution selon la taille du graphe")
    plt.grid(True, linestyle="--", alpha=0.6)

    handles, labels = plt.gca().get_legend_handles_labels()
    ordered_labels = [label_map[m] for m in methods if label_map[m] in labels]
    ordered_handles = [handles[labels.index(l)] for l in ordered_labels]
    plt.legend(ordered_handles, ordered_labels)

    plt.tight_layout()
    output_file_line = output_dir / "benchmark_times_evolution.png"
    plt.savefig(output_file_line, dpi=150, bbox_inches="tight")
    plt.close()
    print(f"✅ Graphique d'évolution enregistré : {output_file_line.resolve()}")


# === Entrée principale ===
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Génère les graphiques comparatifs des temps d'exécution PageRank Spark")
    parser.add_argument("--input", required=True, help="Chemin vers le fichier benchmark.csv")
    parser.add_argument("--output", default="benchmark_results", help="Dossier de sortie pour les graphiques")
    parser.add_argument("--xaxis", default="nodes", choices=["nodes", "edges"], help="Abscisse du graphe : 'nodes' ou 'edges'")
    args = parser.parse_args()

    input_path = Path(args.input)
    if not input_path.exists():
        raise FileNotFoundError(f"Fichier introuvable : {input_path}")

    output_dir = Path(args.output)
    output_dir.mkdir(parents=True, exist_ok=True)

    plot_benchmarks(input_path, output_dir, args.xaxis)