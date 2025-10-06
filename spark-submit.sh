#!/bin/bash
# =========================================================
# Script de lancement Spark pour le projet PageRankSpark
# Compatible Linux / macOS - version unifiée (RDD / DF / TEST)
# =========================================================

if [ $# -lt 1 ]; then
  echo "Usage:"
  echo "  $0 <mode> [input] [output] [iterations] [plot] [debug]"
  echo "  mode ∈ {rdd, rdd-optimized, df, test}"
  exit 1
fi

MODE=$1
INPUT=${2:-"data/sample_graph.txt"}
OUTPUT=${3:-"output"}
ITER=${4:-10}
PLOT=${5:-false}
DEBUG=${6:-false}

# --- Fichiers & configuration ---
JAR="target/scala-2.12/pagerankspark_2.12-0.1.jar"
LOG_CONF="$(pwd)/src/main/resources/log4j2.properties"

# --- Choix de la classe principale selon le mode ---
case "$MODE" in
  rdd)
    MAIN_CLASS="pagerank.rdd.MainRDD"
    ;;
  rdd-optimized)
    MAIN_CLASS="pagerank.rddoptimized.MainRDDOptimized"
    ;;
  df)
    MAIN_CLASS="pagerank.df.MainDF"
    ;;
  test)
    MAIN_CLASS="pagerank.TestRunner"
    JAR="target/scala-2.12/pagerankspark_2.12-0.1-tests.jar"
    ;;
  *)
    echo "❌ Mode inconnu : $MODE"
    echo "   Modes valides : rdd | rdd-optimized | df | test"
    exit 1
    ;;
esac

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
echo "Classe     : $MAIN_CLASS"
echo "JAR        : $JAR"
echo "Entrée     : $INPUT"
echo "Sortie     : $OUTPUT"
echo "Itérations : $ITER"
echo "Plot       : $PLOT"
echo "Debug      : $DEBUG"
echo "=============================================="

$TIME_CMD spark-submit \
  --master local[*] \
  --class "$MAIN_CLASS" \
  --conf "spark.driver.extraJavaOptions=-Dlog4j.configurationFile=$LOG_CONF" \
  --conf "spark.executor.extraJavaOptions=-Dlog4j.configurationFile=$LOG_CONF" \
  "$JAR" "$INPUT" "$OUTPUT" "$ITER" \
  $( [ "$PLOT" == "true" ] && echo "--plot" ) \
  $( [ "$DEBUG" == "true" ] && echo "--debug" )

# --- Tracé optionnel ---
if [ "$PLOT" == "true" ] && [ "$MODE" != "test" ]; then
  echo "📈 Tracé du PageRank..."
  ./scripts/plot_history.py "$OUTPUT/history.csv"
fi