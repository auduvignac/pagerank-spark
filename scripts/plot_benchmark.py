#!/usr/bin/env python3
import pandas as pd
import matplotlib.pyplot as plt
import sys

def plot_benchmarks(csv_file: str, output_file: str, x_axis: str = "nodes"):
    """
    Trace les temps d'exécution RDD vs DF en fonction de la taille du graphe.
    
    Args:
        csv_file (str): chemin vers benchmark.csv
        x_axis (str): "nodes" ou "edges" (abscisse du graphe)
    """
    # Charger avec séparateur explicite
    df = pd.read_csv(csv_file, sep=";")

    # Forcer les colonnes numériques
    df["nodes"] = pd.to_numeric(df["nodes"], errors="coerce")
    df["edges"] = pd.to_numeric(df["edges"], errors="coerce")
    df["time"] = pd.to_numeric(df["time"], errors="coerce")

    if x_axis not in ["nodes", "edges"]:
        raise ValueError("x_axis doit être 'nodes' ou 'edges'")

    plt.figure(figsize=(7, 5))

    for method in df["method"].unique():
        subset = df[df["method"] == method]
        plt.plot(
            subset[x_axis].to_numpy(),   # conversion explicite
            subset["time"].to_numpy(),   # conversion explicite
            marker="o",
            label=method,
            linestyle="--",
        )

        # Ajouter les valeurs au-dessus des points
        for x, y in zip(subset[x_axis], subset["time"]):
            plt.text(x, y + 0.1, f"{y:.2f}", ha="center", fontsize=9)

    plt.xlabel("Nombre de nœuds" if x_axis == "nodes" else "Nombre d'arêtes", fontsize=12)
    plt.ylabel("Temps d'exécution (s)", fontsize=12)
    plt.title("Comparaison RDD vs DF (Spark PageRank)", fontsize=14)
    plt.legend()
    plt.grid(True, linestyle="--", alpha=0.6)

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