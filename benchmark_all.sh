#!/usr/bin/env bash
set -e  # Stoppe en cas d’erreur
set -u  # Stoppe si variable non définie

# ===============================
# Paramètres fixes
# ===============================
MODE="all"
INPUT="data/wiki-chti.txt,data/wiki-breton.txt,data/wiki-basque.txt,data/wiki-pt.txt,data/wiki-fr.txt"
BASE_OUTPUT="output/all"
DAMPING="0.85"
DEBUG="--debug"

# ===============================
# Paramètres variables
# ===============================
ITERATIONS_LIST=(10 20 50 100)
PARTITIONS_LIST=(16 32 64 128 256 512 1024)

# ===============================
# Boucle principale
# ===============================
for ITER in "${ITERATIONS_LIST[@]}"; do
  for PART in "${PARTITIONS_LIST[@]}"; do

    # Dossier de sortie structuré
    OUTPUT_DIR="${BASE_OUTPUT}/iterations_${ITER}/damping_${DAMPING//./_}/partitions_${PART}"
    mkdir -p "$OUTPUT_DIR"

    echo "==============================================="
    echo "🚀 Lancement du benchmark"
    echo "  ➤ Mode        : $MODE"
    echo "  ➤ Input       : $INPUT"
    echo "  ➤ Itérations  : $ITER"
    echo "  ➤ Partitions  : $PART"
    echo "  ➤ Damping     : $DAMPING"
    echo "  ➤ Output      : $OUTPUT_DIR"
    echo "==============================================="

    # Exécution
    ./run-app.sh \
      --mode="$MODE" \
      --input="$INPUT" \
      --output="$OUTPUT_DIR" \
      --iterations="$ITER" \
      --damping="$DAMPING" \
      --partitions="$PART"

    echo "✅ Benchmark terminé pour iterations=$ITER, partitions=$PART"
    echo
  done
done

echo "🎯 Tous les benchmarks sont terminés."