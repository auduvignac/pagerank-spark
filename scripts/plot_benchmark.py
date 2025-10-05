#!/usr/bin/env python3
import pandas as pd
import matplotlib.pyplot as plt
import sys

def plot_benchmarks(csv_file: str, output_file: str, x_axis: str = "nodes"):
    """
    Trace les temps d'exécution RDD, RDD_Optimized et DF en fonction de la
    taille du graphe.

    Args:
        csv_file (str): chemin vers benchmark.csv
        output_file (str): fichier image de sortie
        x_axis (str): "nodes" ou "edges" (abscisse du graphe)
    """
    df = pd.read_csv(csv_file, sep=";")

    # Conversion numérique explicite
    df["nodes"] = pd.to_numeric(df["nodes"], errors="coerce")
    df["edges"] = pd.to_numeric(df["edges"], errors="coerce")
    df["time"]  = pd.to_numeric(df["time"], errors="coerce")

    if x_axis not in ["nodes", "edges"]:
        raise ValueError("x_axis doit être 'nodes' ou 'edges'")

    plt.figure(figsize=(7, 5))

    # Palette cohérente entre les méthodes
    color_map = {
        "RDD": "#1f77b4",
        "RDD_Optimized": "#2ca02c",
        "DF": "#ff7f0e"
    }

    # Noms plus clairs pour la légende
    label_map = {
        "RDD": "RDD classique",
        "RDD_Optimized": "RDD optimisé",
        "DF": "DataFrame"
    }

    for method in df["method"].unique():
        subset = df[df["method"] == method].sort_values(by=x_axis)
        color = color_map.get(method, None)
        label = label_map.get(method, method)

        plt.plot(
            subset[x_axis].to_numpy(),
            subset["time"].to_numpy(),
            marker="o",
            label=label,
            linestyle="--",
            color=color
        )

        # Affichage des valeurs au-dessus des points
        for x, y in zip(subset[x_axis], subset["time"]):
            plt.text(x, y + 0.1, f"{y:.2f}", ha="center", fontsize=9)

    plt.xlabel("Nombre de nœuds" if x_axis == "nodes" else "Nombre d'arêtes", fontsize=12)
    plt.ylabel("Temps d'exécution (s)", fontsize=12)
    plt.title("Comparaison RDD / RDD optimisé / DataFrame (Spark PageRank)", fontsize=14)
    plt.legend()
    plt.grid(True, linestyle="--", alpha=0.6)

    plt.tight_layout()
    plt.savefig(output_file, dpi=150, bbox_inches="tight")
    print(f"✅ Figure enregistrée : {output_file}")

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: plot_benchmark.py benchmark.csv output_file [nodes|edges]")
        sys.exit(1)

    csv_file = sys.argv[1]
    output_file = sys.argv[2]
    x_axis = sys.argv[3] if len(sys.argv) > 3 else "nodes"
    plot_benchmarks(csv_file, output_file, x_axis)