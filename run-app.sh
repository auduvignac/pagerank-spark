#!/bin/bash
set -e  # Stop on error

# =========================================================
# Script de lancement du cluster Spark + job PageRankSpark
# =========================================================

# --- Paramètres par défaut ---
MODE=${1:-"rdd"}                # rdd | rdd-optimized | df | test
INPUT=${2:-"data/sample_graph.txt"}
OUTPUT=${3:-"output/${MODE}"}
ITER=${4:-10}
PLOT=${5:-false}
DEBUG=${6:-false}

# =========================================================
# Étape 0 : Compilation du projet Scala
# =========================================================
echo "🔧 Compilation du projet Scala..."
if ! command -v sbt &>/dev/null; then
    echo "❌ Erreur : sbt n'est pas installé sur ta machine hôte."
    echo "   Installe-le avant de lancer ce script."
    exit 1
fi

sbt clean package

# =========================================================
# Étape 1 : Lancement du cluster Spark via Docker
# =========================================================
echo "🧹 Stopping existing Spark cluster (if any)..."
docker rm -f spark-submit spark-worker spark-master >/dev/null 2>&1 || true

echo "🚀 Starting Spark cluster..."
docker-compose up -d

echo "⏳ Waiting for Spark master to be ready..."
sleep 5

echo "⚙️  Preparing spark-submit.sh inside container..."
docker exec spark-submit dos2unix /app/spark-submit.sh >/dev/null 2>&1 || true
docker exec spark-submit chmod +x /app/spark-submit.sh

echo "🚀 Submitting Spark job..."
echo "----------------------------------------------"
echo "Mode       : $MODE"
echo "Input      : $INPUT"
echo "Output     : $OUTPUT"
echo "Iterations : $ITER"
echo "Plot       : $PLOT"
echo "Debug      : $DEBUG"
echo "----------------------------------------------"

docker exec spark-submit /app/spark-submit.sh "$MODE" "$INPUT" "$OUTPUT" "$ITER" "$PLOT" "$DEBUG"

echo ""
echo "📜 Logs du conteneur spark-submit :"
docker logs spark-submit

echo ""
echo "✅ Spark job completed successfully."