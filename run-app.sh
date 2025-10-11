#!/usr/bin/env bash
set -e  # Stop on error

# =========================================================
# Script de lancement du cluster Spark + job PageRankSpark
# =========================================================

if [ $# -lt 1 ]; then
    echo "Usage:"
    echo "  $0 [--mode=mode] [--input=\"file1,file2,...\"] [--output=dir] [--iteration=N] [--damping=val] [--plot] [--debug] [--build]"
    echo "  mode ∈ {rdd, rdd_optimized, df, all, test}"
    exit 1
fi

# === Paramètres par défaut ===
MODE="all"
INPUT="data/sample_graph.txt"
OUTPUT="output"
ITER=10
DAMPING=1.0
PLOT=false
DEBUG=false
BUILD=false

# === Parsing des arguments ===
while [[ $# -gt 0 ]]; do
    case "$1" in
        --mode=*)
        MODE="${1#*=}"
        ;;
        --input=*)
        INPUT="${1#*=}"
        ;;
        --output=*)
        OUTPUT="${1#*=}"
        ;;
        --iteration=*|--iterations=*)
        ITER="${1#*=}"
        ;;
        --damping=*)
        DAMPING="${1#*=}"
        ;;
        --plot)
        PLOT=true
        ;;
        --debug)
        DEBUG=true
        ;;
        --build)
        BUILD=true
        ;;
        *)
        echo "⚠️  Argument inconnu : $1"
        ;;
    esac
    shift
done

# =========================================================
# Étape 0 : Compilation du projet Scala (si build)
# =========================================================
# Si l'utilisateur a demandé une compilation
if [ "$BUILD" = true ]; then
    echo "🔧 Compilation du projet Scala..."

    # Vérifie que sbt est installé
    if ! command -v sbt &>/dev/null; then
        echo "❌ Erreur : 'sbt' n'est pas installé sur ta machine hôte."
        echo "   ➜ Installe-le avant de lancer ce script."
        exit 1
    fi

    # Nettoie et recompile le projet
    if sbt clean package; then
        echo "✅ Compilation réussie."
    else
        echo "❌ Échec de la compilation Scala."
        exit 1
    fi
fi

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
echo "Damping    : $DAMPING"
echo "Plot       : $PLOT"
echo "Debug      : $DEBUG"
echo "----------------------------------------------"

docker exec spark-submit /app/spark-submit.sh "$MODE" "$INPUT" "$OUTPUT" "$ITER" "$DAMPING" "$PLOT" "$DEBUG"

echo ""
echo "📜 Logs du conteneur spark-submit :"
docker logs spark-submit

echo ""
echo "✅ Spark job completed successfully."