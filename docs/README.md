# PageRank Spark

Projet d'implémentation de l'algorithme PageRank en utilisant Apache Spark (Scala).

## Compilation

```bash
sbt package
```

## Exécution

```bash
spark-submit --master local[*] --class pagerank.Main target/scala-2.12/pagerankspark_2.12-0.1.jar data/sample_graph.txt output/
```
