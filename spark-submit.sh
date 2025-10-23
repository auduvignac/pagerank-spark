#!/usr/bin/env bash
set -e

# --- Vérification des arguments ---
if [ $# -lt 1 ]; then
  echo "Usage:"
  echo "  $0 <mode> [input] [output] [iterations] [plot] [debug]"
  echo "  mode ∈ {rdd, rdd-optimized, df, all, test}"
  exit 1
fi

# --- Paramètres par défaut ---
MODE=$1
INPUT=${2:-"data/sample_graph.txt"}
OUTPUT=${3:-"output"}
ITER=${4:-10}
DAMPING=${5:-1}
PARTITIONS=${6:-128}
STORAGE=${7:-"MEMORY_AND_DISK_SER"}
PLOT=${8:-false}
DEBUG=${9:-false}
METRICS=${9:-false}

# --- Fichiers & configuration ---
JAR="target/scala-2.12/pagerankspark_2.12-0.1.jar"
LOG_CONF="$(pwd)/src/main/resources/log4j2.properties"
MAIN_CLASS="pagerank.Main"

# --- Nettoyage pour les modes de calcul ---
if [ "$MODE" != "test" ]; then
  rm -rf "$OUTPUT"
fi

# --- Commande time compatible macOS/Linux ---
if command -v /usr/bin/time &>/dev/null; then
  TIME_CMD="/usr/bin/time -p"
else
  TIME_CMD="time -p"
fi

echo "=============================================="
echo "🚀 Lancement PageRank ($MODE)"
echo "----------------------------------------------"
echo "Classe                  : $MAIN_CLASS"
echo "JAR                     : $JAR"
echo "Entrée                  : $INPUT"
echo "Sortie                  : $OUTPUT"
echo "Itérations              : $ITER"
echo "Facteur d'amortissement : $DAMPING"
echo "Nombre de partitions    : $PARTITIONS"
echo "Stockage                : $STORAGE"
echo "Plot                    : $PLOT"
echo "Debug                   : $DEBUG"
echo "Metrics                 : $METRICS"
echo "=============================================="

# --- Exécution Spark ---
$TIME_CMD spark-submit \
  --master local[*] \
  --class "$MAIN_CLASS" \
  --conf "spark.driver.extraJavaOptions=-Dfile.encoding=UTF-8 -Dlog4j.configurationFile=$LOG_CONF" \
  --conf "spark.executor.extraJavaOptions=-Dfile.encoding=UTF-8 -Dlog4j.configurationFile=$LOG_CONF" \
  "$JAR" "$MODE" "$INPUT" "$OUTPUT" "$ITER" "$DAMPING" "$PARTITIONS" "$STORAGE" "$METRICS" \
  $( [ "$PLOT" == "true" ] && echo "--plot" ) \
  $( [ "$DEBUG" == "true" ] && echo "--debug" )

# --- Tracé du PageRank optionnel ---
echo "📈 Tracé des résultats du benchmark..."
./scripts/plot_benchmark.py "$OUTPUT/benchmark.csv" "$OUTPUT/benchmark.png"

# --- Tracé du PageRank optionnel ---
if [ "$PLOT" == "true" ] && [ "$MODE" != "test" ]; then
  echo "📈 Tracé du PageRank..."
  ./scripts/plot_history.py "$OUTPUT/history.csv"
fi