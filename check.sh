#!/bin/bash
set -euo pipefail

# === Configuration ===
JARPATH="/opt/cephfs/users/students/p6emiasd2025/aduvignac-rosa/workspace/pagerank/pagerankspark_2.12-0.1.jar"
DATADIR="/opt/cephfs/users/students/p6emiasd2025/aduvignac-rosa/workspace/pagerank/data"
HDFSDATADIR="/students/p6emiasd2025/aduvignac-rosa/data"
OUTPUT="/opt/cephfs/users/students/p6emiasd2025/aduvignac-rosa/workspace/pagerank/check"
LOG4J="/opt/cephfs/users/students/p6emiasd2025/aduvignac-rosa/workspace/pagerank/log4j2.properties"
LOG_FILE="${OUTPUT}/check.log"

graph_name="wiki-basque"
ITERATIONS=10
DAMPING=0.85
PARTITIONS=64
STORAGE="MEMORY_AND_DISK_SER"
METHOD="df"
HDFS_PATH="${HDFSDATADIR}/${graph_name}.txt"

mkdir -p "$OUTPUT"

nohup spark-submit \
  --class pagerank.Main \
  --deploy-mode client \
  --executor-cores 4 \
  --num-executors 8 \
  --executor-memory 6G \
  --driver-memory 4G \
  --conf "spark.serializer=org.apache.spark.serializer.KryoSerializer" \
  --conf "spark.kryoserializer.buffer.max=512m" \
  --conf "spark.default.parallelism=$PARTITIONS" \
  --conf "spark.sql.shuffle.partitions=$PARTITIONS" \
  --conf "spark.memory.fraction=0.75" \
  --conf "spark.memory.offHeap.enabled=true" \
  --conf "spark.memory.offHeap.size=2g" \
  --conf "spark.speculation=false" \
  --conf "spark.locality.wait=0" \
  --conf "spark.shuffle.compress=true" \
  --conf "spark.shuffle.spill.compress=true" \
  --conf "spark.shuffle.file.buffer=128k" \
  --conf "spark.reducer.maxSizeInFlight=96m" \
  --conf "spark.rdd.compress=true" \
  --conf "spark.io.compression.codec=lz4" \
  --conf "spark.sql.adaptive.enabled=false" \
  --conf "spark.executor.memoryOverhead=1G" \
  --conf "spark.executor.extraJavaOptions=-XX:+UseG1GC -XX:+UseStringDeduplication" \
  --conf "spark.driver.extraJavaOptions=-Dlog4j.configurationFile=$LOG4J" \
  --files "$LOG4J" \
  "$JARPATH" \
  "$METHOD" \
  "$HDFS_PATH" \
  "$OUTPUT" \
  "$ITERATIONS" \
  "$DAMPING" \
  "$PARTITIONS" \
  "$STORAGE" \
  --metrics \
  > "$LOG_FILE" 2>&1
