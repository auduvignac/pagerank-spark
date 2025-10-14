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
import org.apache.spark.storage.StorageLevel

/**
 * Point d'entrée principal pour exécuter PageRank
 * sur les variantes RDD, DataFrame et RDD Optimisé.
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
      storage: StorageLevel,
      debug: Boolean,
      plot: Boolean = false,
      outputDir: String
  )(implicit spark: SparkSession, logger: Logger): Unit = {

    logger.info(s"==== [PageRank RDD] [${graph.fileName}] [$iterations itérations] [Facteur d'amortissement $damping] ====")

    // Comptes calculés à la demande (lazy, sans surcharge mémoire)
    val nodes = graph.nNodes
    val edges = graph.nEdges

    val start = System.nanoTime()

    // Exécution du PageRank sur l’instance GraphRDD
    val v = PageRankRDD.computePageRank(
      links = graph.links,
      iterations = iterations,
      damping = damping,
      debug = debug,
      plot = plot,
      logger = logger,
      storage = storage,
      outputDir = outputDir
    )

    val duration = (System.nanoTime() - start) / 1e9
    logger.info(f"==== [PageRank RDD] [${graph.fileName}] [$iterations itérations] [Facteur d'amortissement $damping] [temps d'exécution $duration%.2f s] ====")

    PageRankUtils.appendBenchmark("RDD", graph.fileName, iterations, nodes, edges, duration, output)
  }

  /** Exécution PageRank DataFrame */
  def computePageRankDF(
      graph: GraphDF,
      output: String,
      iterations: Int,
      damping: Double,
      debug: Boolean,
      plot: Boolean = false,
      storage: org.apache.spark.storage.StorageLevel = org.apache.spark.storage.StorageLevel.MEMORY_AND_DISK,
      numParts: Int = 0
  )(implicit spark: SparkSession, logger: Logger): Unit = {

    logger.info(s"==== [PageRank DF] [${graph.fileName}] [$iterations itérations] [Facteur d'amortissement $damping] ====")

    // === (1) Comptage paresseux (lazy) ===
    val nodes = graph.nodeCount()
    val edges = graph.edgeCount()

    logger.info(f"[PageRank DF] Nœuds = $nodes%,d | Arêtes = $edges%,d")

    val start = System.nanoTime()

    // === (2) Exécution du PageRank sur l’instance GraphDF ===
    val ranks = PageRankDF.computePageRank(
      spark = spark,
      edges = graph.links,             // DataFrame(src, dest)
      beta = damping,
      iterations = iterations,
      numParts = numParts,
      st = storage,
      debug = debug,
      plot = plot,
      outputDir = output
    )

    val duration = (System.nanoTime() - start) / 1e9

    // === (3) Log final ===
    logger.info(
      f"==== [PageRank DF] [${graph.fileName}] [$iterations itérations] " +
      f"[Facteur d'amortissement $damping] [temps d'exécution $duration%.2f s] ===="
    )

    // === (4) Benchmark ===
    PageRankUtils.appendBenchmark("DF", graph.fileName, iterations, nodes, edges, duration, output)

    // Optionnel : aperçu des 10 pages les mieux classées
    if (debug) {
      import spark.implicits._
      val top10 = ranks.orderBy(desc("rank")).limit(10).collect()
      logger.info("==== Top 10 des pages (PageRank DF) ====")
      top10.foreach { row =>
        val id = row.getAs[String]("id")
        val rank = row.getAs[Double]("rank")
        logger.info(f"  $id%-30s => $rank%.6f")
      }
    }
  }

  /** Exécution PageRank RDD optimisé */
  def computePageRankRDDOptimized(
      graph: GraphRDD,
      output: String,
      iterations: Int,
      damping: Double,
      storage: StorageLevel,
      debug: Boolean,
      plot: Boolean = false,
      outputDir: String,
      numParts: Int = 0
  )(implicit spark: SparkSession, logger: Logger): Unit = {

    logger.info(
      s"==== [PageRank RDD Optimisé] [${graph.fileName}] [$iterations itérations] [Facteur d'amortissement $damping] ===="
    )

    // Comptes calculés paresseusement (lazy)
    val nodes = graph.nNodes
    val edges = graph.nEdges

    val start = System.nanoTime()

    // Exécution du PageRank partitionné
    val v = PageRankRDDOptimized.computePageRank(
      links = graph.links,
      iterations = iterations,
      beta = damping,
      debug = debug,
      plot = plot,
      logger = logger,
      storage = storage,
      outputDir = outputDir,
      numParts = numParts
    )

    val duration = (System.nanoTime() - start) / 1e9

    logger.info(
      f"==== [PageRank RDD Optimisé] [${graph.fileName}] [$iterations itérations] [Facteur d'amortissement $damping] [temps d'exécution $duration%.2f s] ===="
    )

    // Enregistrement des statistiques de performance
    PageRankUtils.appendBenchmark(
      "RDD-Optimized",
      graph.fileName,
      iterations,
      nodes,
      edges,
      duration,
      output
    )
  }

  // =======================
  // Point d'entrée principal
  // =======================
  def main(args: Array[String]): Unit = {
    logger.info("==== [Début d'exécution] PageRank ====")

    if (args.length < 3) {
      System.err.println(
        "Usage: Main <type: rdd|df|rdd_optimized|all> <input_graph> <output_dir> [iterations] [--plot] [--debug]"
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

    // possibilité de passer plusieurs fichiers séparés par des virgules
    // pour en générer une boucle
    val paths = input.split(",").toList

    implicit val spark: SparkSession = SparkSession.builder()
      .appName(s"PageRank-$method")
      .getOrCreate()

    for (path <- paths) {

      val fileName = path.split("/").last

      logger.info(s"==== [Traitement du graphe] [$path] [itérations : $iterations] [facteur d'amortissement : $damping] ====")

      try {
        method match {
          case "rdd" =>
            val graphrdd = new GraphRDD(
              name = s"Graph-RDD-$fileName",
              path = path,
              debug = debug,
              storage = PageRankUtils.storageLevelOf(storage)
            )
            graphrdd.describe()
            computePageRankRDD(
              graph = graphrdd,
              output = output,
              iterations = iterations,
              damping = damping,
              storage = PageRankUtils.storageLevelOf(storage),
              debug = debug,
              plot = plot,
              outputDir = output
            )

          case "df" =>
            val graphdf = new GraphDF(
              name = s"Graph-DF-$fileName",
              path = path,
              debug = debug,
              storage = PageRankUtils.storageLevelOf(storage)
            )
            graphdf.describe()
            computePageRankDF(
              graph = graphdf,
              output = output,
              iterations = iterations,
              damping = damping,
              debug = debug,
              plot = plot,
              storage = PageRankUtils.storageLevelOf(storage),
              numParts = partitions
            )

          case "rdd_optimized" =>
            val graphrdd = new GraphRDD(
              name = s"Graph-RDD-Optimized-$fileName",
              path = path,
              debug = debug,
              storage = PageRankUtils.storageLevelOf(storage)
            )
            graphrdd.describe()
            computePageRankRDDOptimized(
              graph = graphrdd,
              output = output,
              iterations = iterations,
              damping = damping,
              storage = PageRankUtils.storageLevelOf(storage),
              debug = debug,
              plot = plot,
              outputDir = output,
              numParts = partitions
            )

          case "all" =>
            val graphrdd = new GraphRDD(
              name = s"Graph-RDD-$fileName",
              path = path,
              debug = debug,
              storage = PageRankUtils.storageLevelOf(storage)
            )
            graphrdd.describe()
            val graphdf = new GraphDF(
              name = s"Graph-DF-$fileName",
              path = path,
              debug = debug,
              storage = PageRankUtils.storageLevelOf(storage)
            )
            graphdf.describe()
            computePageRankRDD(
              graph = graphrdd,
              output = output,
              iterations = iterations,
              damping = damping,
              storage = PageRankUtils.storageLevelOf(storage),
              debug = debug,
              plot = plot,
              outputDir = output
            )
            computePageRankRDDOptimized(
              graph = graphrdd,
              output = output,
              iterations = iterations,
              damping = damping,
              storage = PageRankUtils.storageLevelOf(storage),
              debug = debug,
              plot = plot,
              outputDir = output,
              numParts = partitions
            )
            computePageRankDF(
              graph = graphdf,
              output = output,
              iterations = iterations,
              damping = damping,
              debug = debug,
              plot = plot,
              storage = PageRankUtils.storageLevelOf(storage),
              numParts = partitions
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
  }
}