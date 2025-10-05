#!/usr/bin/env python3
import numpy as np
import matplotlib.pyplot as plt


def compute_pagerank_iterations(M, v0, n_iter=50):
    """
    Calcule l'évolution du vecteur de probabilité v_i = M^i v0
    sur un nombre d'itérations donné.
    """
    history = [v0]
    v = v0
    for _ in range(n_iter):
        v = M @ v
        history.append(v)
    return np.array(history)


def plot_pagerank(history):
    """
    Trace l'évolution du PageRank au fil des itérations.
    """
    plt.figure(figsize=(6, 4))
    for i in range(history.shape[1]):
        plt.plot(history[:, i], label=f"Page {chr(65+i)}")  # A, B, C, D

    plt.xlabel("Itération")
    plt.ylabel("Probabilité")
    plt.title("Évolution du PageRank par itérations")
    plt.legend()
    plt.grid(True, linestyle="--", alpha=0.6)
    plt.show()


if __name__ == "__main__":
    # Matrice de transition (pages A, B, C, D)
    M = np.array(
        [
            [0, 1 / 2, 1, 0],
            [1 / 3, 0, 0, 1 / 2],
            [1 / 3, 0, 0, 1 / 2],
            [1 / 3, 1 / 2, 0, 0],
        ]
    )

    # Vecteur initial uniforme (4 pages)
    v0 = np.array([1 / 4, 1 / 4, 1 / 4, 1 / 4])

    # Calcul des itérations
    history = compute_pagerank_iterations(M, v0, n_iter=50)

    print(
        f"✅ PageRank calculé sur {history.shape[0]-1} itérations "
        f"avec pour vecteur final : {history[-1]}"
    )

    # Tracé de la figure
    plot_pagerank(history)
