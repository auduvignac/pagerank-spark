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
