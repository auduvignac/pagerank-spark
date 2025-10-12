# PageRank Spark

Projet d'implémentation de l'algorithme PageRank en utilisant Apache Spark (Scala).

## Clone de dépôt

```bash
git clone https://github.com/auduvignac/pagerank-spark.git
```

## Exécution du projet

Après s’être positionné dans le répertoire principal du projet (par défaut : `PageRank-Spark`), le calcul du PageRank peut être lancé via le script `run-app.sh` :

```bash
./run-app.sh \
  --mode=all \
  --input="data/wiki-chti.txt,data/wiki-breton.txt,data/wiki-basque.txt,data/wiki-pt.txt,data/wiki-fr.txt" \
  --output=output/all/iteration_10/damping_0_85 \
  --damping=0.85 \
  --iteration=10 \
  --build
```

### Explication détaillée des options

| Option | Exemple | Description |
| :------------ | :------------------------------------------------------ | :------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `--mode` | `--mode=all` | Définit le ou les modes d’exécution : <br>• `rdd` → implémentation RDD classique<br>• `rdd-optimized` → version RDD avec partitionnement<br>• `df` → version DataFrame<br>• `all` → exécute successivement les trois modes |
| `--input` | `--input="data/wiki-chti.txt,data/wiki-breton.txt,..."` | Spécifie un ou plusieurs fichiers de graphe à traiter (séparés par des virgules). Chaque fichier est traité indépendamment. |
| `--output`    | `--output=output/all/iteration_10/damping_0_85` | Répertoire de sortie où les résultats du PageRank seront enregistrés. Un sous-dossier est créé pour chaque graphe et chaque mode. |
| `--iteration` | `--iteration=10` | Nombre d’itérations du calcul de PageRank à exécuter.|
| `--damping` | `--damping=0.85` | Facteur d’amortissement du modèle (probabilité de suivre un lien). <br>• `1.0` → pas d’amortissement (mode théorique) <br>• `0.85` → valeur standard utilisée par Google. |
| `--build` | *(optionnel)* | Force la recompilation du projet Scala via `sbt clean package` avant l’exécution. |
| `--debug` | *(optionnel)* | Active les logs détaillés d’exécution (utile pour le suivi du calcul). |
| `--plot` | *(optionnel)* | Génère la courbe d'évolution de la probabilité de position (si la fonctionnalité de visualisation est activée).

### Exécution complète du benchmark

Depuis la racine du projet, exécuter simplement :

```bash
./benchmark.sh
```

Le script lance automatiquement les expériences pour toutes les combinaisons de paramètres prédéfinis.

- Itérations :
  `{10, 20, 50, 100}`

- Partitions :
  `{16, 32, 64, 128, 256, 512, 1024}`

Le script exécute automatiquement le programme pour chaque combinaison d’**itérations** et de **partitions**,
en sauvegardant les résultats dans une arborescence structurée sous `output/`.

Chaque exécution génère un répertoire `output/all/iterations_N/damping_0_85/partitions_M` avec : 
- `N` : le nombre d'itérations ;
- `M` : le nombre partitions.

Ces répertoires contiennent deux fichiers :
- `benchmark.csv` : temps d'exécution de chaque structure avec l'ensemble des graphes ;
- `benchmark.png` : représentation graphique de `benchmark.csv`.


