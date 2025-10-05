#!/usr/bin/env python3
import pandas as pd
import matplotlib.pyplot as plt
import sys
import os

def plot_history(csv_file: str):
    # Charger le CSV
    df = pd.read_csv(csv_file)

    # Nettoyer les noms de colonnes
    df.columns = df.columns.str.strip()

    # Les colonnes après "Iteration" sont les pages
    nodes = df.columns[1:]

    plt.figure(figsize=(8, 5))

    for node in nodes:
        x = df["Iteration"].to_numpy()
        y = df[node].to_numpy()
        plt.plot(x, y, label=node)
        # Annoter la valeur finale AU-DESSUS du dernier point
        plt.text(
            x[-1],                  # même abscisse que le dernier point
            y[-1] + 0.02,           # légèrement au-dessus
            f"{y[-1]:.3f}",         # valeur arrondie
            fontsize=12,
            ha="center",
            va="bottom"
        )

    plt.xlabel("Itération", fontsize=14)
    plt.ylabel("Probabilité", fontsize=14)
    plt.title("Évolution du PageRank par itérations", fontsize=16)
    plt.legend(fontsize=12)
    plt.grid(True, linestyle="--", alpha=0.6)
    plt.xticks(fontsize=12)
    plt.yticks(fontsize=12)
    plt.tight_layout()

    # Nom du fichier PNG à partir du CSV
    out_png = os.path.splitext(csv_file)[0] + ".png"
    plt.savefig(out_png, dpi=150)
    plt.close()

    print(f"✅ Graphique sauvegardé : {out_png}")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: plot_history.py <history.csv>")
        sys.exit(1)

    plot_history(sys.argv[1])