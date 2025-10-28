package pagerank

// =======================
// Imports
// =======================
import java.io.{File, PrintWriter}
import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SparkSession, Dataset}
import org.apache.spark.{HashPartitioner, Partitioner}
import org.apache.spark.sql.functions._

/**
 * Point d'entrée principal pour exécuter PageRank
 * sur les variantes RDD, DataFrame et RDD Partitioné.
 */
object Main {

  // =======================
  // Logger
  // =======================
  implicit val logger: Logger = Logger.getLogger(getClass.getName)

  // =======================
  // Méthodes d'exécution
  // =======================

  /** Exécution PageRank RDD */
  def computePageRankRDD(
      graph: GraphRDD,
      output: String,
      iterations: Int,
      damping: Double,
      debug: Boolean,
      plot: Boolean = false,
      metrics: Boolean = false
  )(implicit spark: SparkSession, logger: Logger): Unit = {

    logger.info(s"==== [PageRank RDD] [${graph.fileName}] [$iterations itérations] [Facteur d'amortissement $damping] ====")

    // Comptes calculés à la demande (lazy, sans surcharge mémoire)
    val nodes = graph.nNodes
    val edges = graph.nEdges

    val start = System.nanoTime()

    // Exécution du PageRank sur l’instance GraphRDD
    val ranks = PageRankRDD.computePageRank(
      graph = graph,
      iterations = iterations,
      damping = damping,
      debug = debug,
      plot = plot,
      logger = logger,
      outputDir = Some(output),
      metrics = metrics
    )

    if (metrics) {
      // Taguer tous les jobs de cette itération
      spark.sparkContext.setJobGroup(
        s"PageRank-RDD-ranks.count()",
        s"PageRank-RDD-ranks.count()"
      )
    }

    // Déclenchement de tout le graphe de dépendances (DAG) via ranks.count()
    ranks.count()

    if (metrics) {
      // Fin du tag
      spark.sparkContext.clearJobGroup()
    }

    val duration = (System.nanoTime() - start) / 1e9
    logger.info(f"==== [PageRank RDD] [${graph.fileName}] [$iterations itérations] [Facteur d'amortissement $damping] [temps d'exécution $duration%.2f s] ====")

    PageRankUtils.appendBenchmark("RDD", graph.fileName, iterations, nodes, edges, duration, output)
  }

  /** Exécution PageRank RDD partitionné */
  def computePageRankRDDPartitioned(
      graph: GraphRDD,
      output: String,
      iterations: Int,
      damping: Double,
      debug: Boolean,
      plot: Boolean = false,
      numpartitions: Int,
      storage: String,
      metrics: Boolean = false
  )(implicit spark: SparkSession, logger: Logger): Unit = {

    logger.info(s"==== [PageRank RDD Partitioned] [${graph.fileName}] [$iterations itérations] [Facteur d'amortissement $damping] ====")

    // Comptes à la demande (lazy)
    val nodes = graph.nNodes
    val edges = graph.nEdges

    logger.info(s"==== [PageRank RDD Partitioned] [Nombre de partitions : $numpartitions] [Stockage : $storage] ===")

    val start = System.nanoTime()

    // Exécution du PageRank partitionné sur l'instance GraphRDD
    val ranks = PageRankRDDPartitioned.computePageRank(
      graph = graph,
      iterations = iterations,
      damping = damping,
      debug = debug,
      plot = plot,
      logger = logger,
      outputDir = Some(output),
      numParts = numpartitions,
      storage = PageRankUtils.storageLevelOf(storage),
      metrics = metrics
    )

    if (metrics) {
      // Taguer tous les jobs de cette itération
      spark.sparkContext.setJobGroup(
        s"PageRank-RDD-Partitioned-ranks.count()",
        s"PageRank-RDD-Partitioned-ranks.count()"
      )
    }

    // Déclenchement de tout le graphe de dépendances (DAG) via ranks.count()
    ranks.count()

    if (metrics) {
      // Fin du tag
      spark.sparkContext.clearJobGroup()
    }

    val duration = (System.nanoTime() - start) / 1e9
    logger.info(f"==== [PageRank RDD Partitioned] [${graph.fileName}] [$iterations itérations] [Facteur d'amortissement $damping] [temps d'exécution $duration%.2f s] ====")

    PageRankUtils.appendBenchmark("RDD_Partitioned", graph.fileName, iterations, nodes, edges, duration, output)
  }

  /** Exécution PageRank DataFrame */
  def computePageRankDF(
      graph: GraphDF,
      output: String,
      iterations: Int,
      damping: Double,
      debug: Boolean,
      plot: Boolean = false,
      storage: String,
      metrics: Boolean = false
  )(implicit spark: SparkSession, logger: Logger): Unit = {

    logger.info(s"==== [PageRank DF] [${graph.fileName}] [$iterations itérations] [Facteur d'amortissement $damping] ====")

    // Comptes à la demande (lazy)
    val nodes = graph.nNodes
    val edges = graph.nEdges

    val start = System.nanoTime()

    // Exécution du PageRank sur l'instance GraphDF
    val ranks = PageRankDF.computePageRank(
      graph = graph,
      iterations = iterations,
      damping = damping,
      debug = debug,
      plot = plot,
      logger = logger,
      outputDir = Some(output),
      storage = PageRankUtils.storageLevelOf(storage),
      metrics = metrics
    )

    if (metrics) {
      // Taguer tous les jobs de cette itération
      spark.sparkContext.setJobGroup(
        s"PageRank-DF-ranks.count()",
        s"PageRank-DF-ranks.count()"
      )
    }

    // Déclenchement de tout le graphe de dépendances (DAG) via ranks.count()
    ranks.count()

    if (metrics) {
      // Fin du tag
      spark.sparkContext.clearJobGroup()
    }

    val duration = (System.nanoTime() - start) / 1e9
    logger.info(f"==== [PageRank DF] [${graph.fileName}] [$iterations itérations] [Facteur d'amortissement $damping] [temps d'exécution $duration%.2f s] ====")

    PageRankUtils.appendBenchmark("DF", graph.fileName, iterations, nodes, edges, duration, output)
  }

  /** Exécution PageRank DataFrame */
  def computePageRankDFPartitioned(
      graph: GraphDF,
      output: String,
      iterations: Int,
      damping: Double,
      debug: Boolean,
      plot: Boolean = false,
      numpartitions: Int,
      storage: String,
      metrics: Boolean = false
  )(implicit spark: SparkSession, logger: Logger): Unit = {

    logger.info(s"==== [PageRank DF Partitioned] [${graph.fileName}] [$iterations itérations] [Facteur d'amortissement $damping] ====")

    // Comptes à la demande (lazy)
    val nodes = graph.nNodes
    val edges = graph.nEdges

    logger.info(s"==== [PageRank DF Partitioned] [Nombre de partitions : $numpartitions] [Stockage : $storage] ===")

    val start = System.nanoTime()

    // Exécution du PageRank sur l'instance GraphDF
    val ranks = PageRankDFPartitioned.computePageRank(
      graph = graph,
      iterations = iterations,
      damping = damping,
      debug = debug,
      plot = plot,
      logger = logger,
      outputDir = Some(output),
      numParts = numpartitions,
      storage = PageRankUtils.storageLevelOf(storage),
      metrics = metrics
    )

    if (metrics) {
      // Taguer tous les jobs de cette itération
      spark.sparkContext.setJobGroup(
        s"PageRank-DF-Partitioned-ranks.count()",
        s"PageRank-DF-Partitioned-ranks.count()"
      )
    }

    // Déclenchement de tout le graphe de dépendances (DAG) via ranks.count()
    ranks.count()

    if (metrics) {
      // Fin du tag
      spark.sparkContext.clearJobGroup()
    }

    val duration = (System.nanoTime() - start) / 1e9
    logger.info(f"==== [PageRank DF Partitioned] [${graph.fileName}] [$iterations itérations] [Facteur d'amortissement $damping] [temps d'exécution $duration%.2f s] ====")

    PageRankUtils.appendBenchmark("DF_Partitioned", graph.fileName, iterations, nodes, edges, duration, output)
  }

  // =======================
  // Point d'entrée principal
  // =======================
  def main(args: Array[String]): Unit = {
    logger.info("==== [Début d'exécution] PageRank ====")

    if (args.length < 3) {
      System.err.println(
        "Usage: Main <type: rdd|rdd_partitioned|df|df_partitioned|all> <input_graph> <output_dir> [iterations] [--plot] [--debug]"
      )
      System.exit(1)
    }

    // Nouvel ordre
    val method = args(0).toLowerCase()
    val input  = args(1)
    val output = args(2)

    // Valeurs par défaut
    val iterations  = if (args.length > 3 && !args(3).startsWith("--")) args(3).toInt else 10
    val damping     = if (args.length > 4 && !args(4).startsWith("--")) args(4).toDouble else 1.0
    val partitions  = if (args.length > 5 && !args(5).startsWith("--")) args(5).toInt else 128
    val storage     = if (args.length > 6 && !args(6).startsWith("--")) args(6) else "MEMORY_ONLY"
    val plot        = args.contains("--plot")
    val debug       = args.contains("--debug")
    val metrics       = args.contains("--metrics")

    // Parsing plus robuste
    val positionalArgs = args.filterNot(_.startsWith("--"))

    logger.info(s"-> Input: $input")
    logger.info(s"-> Output: $output")
    logger.info(s"-> Méthode: $method")
    logger.info(s"-> Itérations: $iterations")
    logger.info(s"-> Facteur d'amortissement: $damping")
    logger.info(s"-> Nombre de partitions: $partitions")
    logger.info(s"-> Type de stockage: $storage")
    logger.info(s"-> Plot: $plot")
    logger.info(s"-> Debug: $debug")
    logger.info(s"-> Metrics: $metrics")

    // possibilité de passer plusieurs fichiers séparés par des virgules
    // pour en générer une boucle
    val paths = input.split(",").toList

    implicit val spark: SparkSession = SparkSession.builder()
      .appName(s"PageRank-$method")
      .getOrCreate()
  
    // === Register custom metrics listener ===
    val listener = new PageRankMetricsListener(logger, Some(s"$output/pagerank_${method}_metrics.csv"))
    spark.sparkContext.addSparkListener(listener)

    for (path <- paths) {

      val fileName = path.split("/").last

      logger.info(s"==== [Traitement du graphe] [$path] [itérations : $iterations] [facteur d'amortissement : $damping] ====")

      // === (1) Créer un listener spécifique à ce graph ===
      val metricsFile = s"$output/pagerank_${fileName.replace(".txt", "")}_metrics.csv"
      val listener = new PageRankMetricsListener(logger, Some(metricsFile))

      // === (2) L’enregistrer dans le contexte Spark ===
      spark.sparkContext.addSparkListener(listener)

      try {
        method match {
          case "rdd" =>
            if (metrics) {
              // Taguer tous les jobs de cette itération
              spark.sparkContext.setJobGroup(
                s"PageRank-RDD-$fileName",
                s"PageRank-RDD-$fileName"
              )
            }
            val rddgraph = new GraphRDD(
              name = s"Graph-RDD-$fileName",
              path = path,
              debug = debug)
            rddgraph.describe()
            computePageRankRDD(
              graph = rddgraph,
              output = output,
              iterations = iterations,
              damping = damping,
              debug = debug,
              plot = plot,
              metrics = metrics
            )
            if (metrics) {
              // Fin du tag
              spark.sparkContext.clearJobGroup()
            }

          case "rdd_partitioned" =>
            if (metrics) {
              // Taguer tous les jobs de cette itération
              spark.sparkContext.setJobGroup(
                s"PageRank-RDD-Partitioned-$fileName",
                s"PageRank-RDD-Partitioned-$fileName"
              )
            }
            val rddoptmizedgraph = new GraphRDD(
              name = s"Graph-RDD-$fileName",
              path = path,
              debug = debug)
            rddoptmizedgraph.describe()
            computePageRankRDDPartitioned(
              graph = rddoptmizedgraph,
              output = output,
              iterations = iterations,
              damping = damping,
              debug = debug,
              plot = plot,
              numpartitions = partitions,
              storage = storage,
              metrics = metrics
            )
            if (metrics) {
              // Fin du tag
              spark.sparkContext.clearJobGroup()
            }

          case "df" =>
            if (metrics) {
              // Taguer tous les jobs de cette itération
              spark.sparkContext.setJobGroup(
                s"PageRank-DF-$fileName",
                s"PageRank-DF-$fileName"
              )
            }
            val dfgraph = new GraphDF(
              name = s"Graph-DF-$fileName",
              path = path,
              debug = debug)
            dfgraph.describe()
            computePageRankDF(
              graph = dfgraph,
              output = output,
              iterations = iterations,
              damping = damping,
              debug = debug,
              plot = plot,
              storage = storage,
              metrics = metrics
            )
            if (metrics) {
              // Fin du tag
              spark.sparkContext.clearJobGroup()
            }

          case "df_partitioned" =>
            if (metrics) {
              // Taguer tous les jobs de cette itération
              spark.sparkContext.setJobGroup(
                s"PageRank-DF-Partitioned-$fileName",
                s"PageRank-DF-Partitioned-$fileName"
              )
            }
            val dfgraph = new GraphDF(
              name = s"Graph-DF-$fileName",
              path = path,
              debug = debug)
            dfgraph.describe()
            computePageRankDFPartitioned(
              graph = dfgraph,
              output = output,
              iterations = iterations,
              damping = damping,
              debug = debug,
              plot = plot,
              numpartitions = partitions,
              storage = storage,
              metrics = metrics
            )
            if (metrics) {
              // Fin du tag
              spark.sparkContext.clearJobGroup()
            }

          case "all" =>
            val rddgraph = new GraphRDD(
              name = s"Graph-RDD-$fileName",
              path = path,
              debug = debug)
            rddgraph.describe()
            val dfgraph = new GraphDF(
              name = s"Graph-DF-$fileName",
              path = path,
              debug = debug)
            dfgraph.describe()
            computePageRankRDD(
              graph = rddgraph,
              output = output,
              iterations = iterations,
              damping = damping,
              debug = debug,
              plot = plot,
              metrics = metrics
            )
            computePageRankDF(
              graph = dfgraph,
              output = output,
              iterations = iterations,
              damping = damping,
              debug = debug,
              plot = plot,
              storage = storage,
              metrics = metrics
            )
            computePageRankRDDPartitioned(
              graph = rddgraph,
              output = output,
              iterations = iterations,
              damping = damping,
              debug = debug,
              plot = plot,
              numpartitions = partitions,
              storage = storage,
              metrics = metrics
            )
            computePageRankDFPartitioned(
              graph = dfgraph,
              output = output,
              iterations = iterations,
              damping = damping,
              debug = debug,
              plot = plot,
              numpartitions = partitions,
              storage = storage,
              metrics = metrics
            )

          case other =>
            System.err.println(s"Type d'exécution inconnu : $other")
            System.exit(1)
        }
      } catch {
        case e: Exception =>
          logger.error(s"Erreur lors du traitement de $input : ${e.getMessage}")
      }
    }
    
    listener.printTotals()
  }
}