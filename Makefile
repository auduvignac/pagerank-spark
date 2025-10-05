# Variables
JAR=target/scala-2.12/pagerankspark_2.12-0.1.jar
GRAPH_DIR=data
INPUT=$(GRAPH_DIR)/sample_graph.txt
OUTPUT_RDD=output_rdd/
OUTPUT_RDD_OPTIMIZED=output_rdd_optimized/
OUTPUT_DF=output_df/
OUTPUT_BENCH=output_benchmark/
SCRIPTS_DIR=$(PWD)/scripts
BENCH_CSV=$(OUTPUT_BENCH)/benchmark.csv
BENCH_PNG=$(OUTPUT_BENCH)/benchmark.png
MAIN_RDD=pagerank.rdd.MainRDD
MAIN_RDD_OPTIMIZED = pagerank.rddoptimized.MainRDDOptimized
MAIN_DF=pagerank.df.MainDF

ITER ?= 10       # valeur par défaut si non précisée
PLOT ?= false   # mettre à true pour conserver l'evolution de chaque noeud
DEBUG ?= false   # mettre à true pour activer le mode debug

# Compilation
build:
	sbt package

# Vérifier les classes main détectées par SBT
check-main:
	sbt "show discoveredMainClasses"

# Exécution version RDD avec mesure du temps
run-rdd: build
	rm -rf $(OUTPUT_RDD)
	@echo "⏱️  Exécution RDD avec $(ITER) itérations"
	/usr/bin/time -p spark-submit --master local[*] --class $(MAIN_RDD) \
		--conf "spark.driver.extraJavaOptions=-Dlog4j.configurationFile=$(PWD)/src/main/resources/log4j2.properties" \
		--conf "spark.executor.extraJavaOptions=-Dlog4j.configurationFile=$(PWD)/src/main/resources/log4j2.properties" \
		$(JAR) $(INPUT) $(OUTPUT_RDD) $(ITER) $(if $(filter true,$(PLOT)),--plot) $(if $(filter true,$(DEBUG)),--debug)
ifeq ($(PLOT),true)
	@echo "📈 Tracé du PageRank..."
	$(SCRIPTS_DIR)/plot_history.py $(OUTPUT_RDD)/history.csv
endif

run-rdd-optimized: build
	rm -rf $(OUTPUT_RDD_OPTIMIZED)
	@echo "⏱️  Exécution RDD avec $(ITER) itérations"
	/usr/bin/time -p spark-submit --master local[*] --class $(MAIN_RDD_OPTIMIZED) \
		--conf "spark.driver.extraJavaOptions=-Dlog4j.configurationFile=$(PWD)/src/main/resources/log4j2.properties" \
		--conf "spark.executor.extraJavaOptions=-Dlog4j.configurationFile=$(PWD)/src/main/resources/log4j2.properties" \
		$(JAR) $(INPUT) $(OUTPUT_RDD_OPTIMIZED) $(ITER) $(if $(filter true,$(PLOT)),--plot) $(if $(filter true,$(DEBUG)),--debug)
ifeq ($(PLOT),true)
	@echo "📈 Tracé du PageRank..."
	$(SCRIPTS_DIR)/plot_history.py $(OUTPUT_RDD_OPTIMIZED)/history.csv
endif

# Exécution version DataFrame avec mesure du temps
run-df: build
	rm -rf $(OUTPUT_DF)
	@echo "⏱️ Exécution DataFrame avec $(ITER) itérations"
	/usr/bin/time -p spark-submit --master local[*] --class $(MAIN_DF) \
		--conf "spark.driver.extraJavaOptions=-Dlog4j.configurationFile=$(PWD)/src/main/resources/log4j2.properties" \
		--conf "spark.executor.extraJavaOptions=-Dlog4j.configurationFile=$(PWD)/src/main/resources/log4j2.properties" \
		$(JAR) $(INPUT) $(OUTPUT_DF) $(ITER) $(if $(filter true,$(PLOT)),--plot) $(if $(filter true,$(DEBUG)),--debug)
ifeq ($(PLOT),true)
	@echo "📈 Tracé du PageRank (DF)..."
	$(SCRIPTS_DIR)/plot_history.py $(OUTPUT_DF)/history.csv
endif

graph-stats: build
	@stats=$$(spark-submit --master local[*] --class pagerank.GraphStats \
		--conf "spark.driver.extraJavaOptions=-Dlog4j.configurationFile=$(PWD)/src/main/resources/log4j2.properties" \
		--conf "spark.executor.extraJavaOptions=-Dlog4j.configurationFile=$(PWD)/src/main/resources/log4j2.properties" \
		$(JAR) $(INPUT)); \
	echo "Stats brutes = $$stats"; \
	nodes=$$(echo $$stats | cut -d',' -f2); \
	edges=$$(echo $$stats | cut -d',' -f3); \
	echo "Nodes = $$nodes, Edges = $$edges"

benchmark: build
	@mkdir -p $(dir $(OUTPUT_BENCH))
	@echo "method;graph;iter;nodes;edges;time" > $(BENCH_CSV)
	@for graph in $$(ls $(GRAPH_DIR)/*.txt); do \
		stats=$$(spark-submit --master local[*] --class pagerank.GraphStats \
			--conf "spark.driver.extraJavaOptions=-Dlog4j.configurationFile=$(PWD)/src/main/resources/log4j2.properties" \
			--conf "spark.executor.extraJavaOptions=-Dlog4j.configurationFile=$(PWD)/src/main/resources/log4j2.properties" \
			$(JAR) $$graph); \
		echo "$$stats" >> tmp_stats.csv; \
	done
	# Trier les graphes par nombre de nœuds (colonne 2)
	@sort -t',' -k2 -n tmp_stats.csv > tmp_stats_sorted.csv
	@while IFS=, read -r graph nodes edges; do \
		echo "⚡ Benchmark sur $$graph avec $(ITER) itérations (nodes=$$nodes, edges=$$edges)"; \
		/usr/bin/time -p -o time_rdd.log \
			spark-submit --master local[*] --class $(MAIN_RDD) \
			--conf "spark.driver.extraJavaOptions=-Dlog4j.configurationFile=$(PWD)/src/main/resources/log4j2.properties" \
			--conf "spark.executor.extraJavaOptions=-Dlog4j.configurationFile=$(PWD)/src/main/resources/log4j2.properties" \
			$(JAR) $$graph $(OUTPUT_RDD) $(ITER) $(if $(filter true,$(PLOT)),--plot) $(if $(filter true,$(DEBUG)),--debug); \
		/usr/bin/time -p -o time_rdd_optimized.log \
			spark-submit --master local[*] --class $(MAIN_RDD_OPTIMIZED) \
			--conf "spark.driver.extraJavaOptions=-Dlog4j.configurationFile=$(PWD)/src/main/resources/log4j2.properties" \
			--conf "spark.executor.extraJavaOptions=-Dlog4j.configurationFile=$(PWD)/src/main/resources/log4j2.properties" \
			$(JAR) $$graph $(OUTPUT_RDD_OPTIMIZED) $(ITER) $(if $(filter true,$(PLOT)),--plot) $(if $(filter true,$(DEBUG)),--debug); \
		/usr/bin/time -p -o time_df.log \
			spark-submit --master local[*] --class $(MAIN_DF) \
			--conf "spark.driver.extraJavaOptions=-Dlog4j.configurationFile=$(PWD)/src/main/resources/log4j2.properties" \
			--conf "spark.executor.extraJavaOptions=-Dlog4j.configurationFile=$(PWD)/src/main/resources/log4j2.properties" \
			$(JAR) $$graph $(OUTPUT_DF) $(ITER) $(if $(filter true,$(PLOT)),--plot) $(if $(filter true,$(DEBUG)),--debug); \
		grep '^real' time_rdd.log | awk -v g=$$graph -v i=$(ITER) -v n=$$nodes -v e=$$edges '{print "RDD;" g ";" i ";" n ";" e ";" $$2}' >> $(BENCH_CSV); \
		grep '^real' time_rdd_optimized.log | awk -v g=$$graph -v i=$(ITER) -v n=$$nodes -v e=$$edges '{print "RDD_Optimized;" g ";" i ";" n ";" e ";" $$2}' >> $(BENCH_CSV); \
		grep '^real' time_df.log | awk -v g=$$graph -v i=$(ITER) -v n=$$nodes -v e=$$edges '{print "DF;" g ";" i ";" n ";" e ";" $$2}' >> $(BENCH_CSV); \
	done < tmp_stats_sorted.csv
	@rm -f tmp_stats.csv tmp_stats_sorted.csv
	@echo "✅ Benchmarks terminés → résultats dans $(BENCH_CSV)"
	$(SCRIPTS_DIR)/plot_benchmark.py $(BENCH_CSV) $(BENCH_PNG)

# Nettoyage
clean:
	rm -rf $(OUTPUT_RDD) $(OUTPUT_DF) $(OUTPUT_BENCH) target/ *.log

# Recompiler tout le projet Scala
rebuild: clean
	sbt clean package

test:
	@for t in $$(find src/test/scala -name "*Spec.scala" \
		| sed 's|src/test/scala/||; s|/|.|g; s|.scala$$||'); do \
		echo "=== Running $$t ==="; \
		sbt "testOnly $$t"; \
	done
