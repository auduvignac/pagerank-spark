package pagerank

// =======================
// Imports
// =======================
import java.io.{File, PrintWriter}
import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SparkSession, Dataset}
import org.apache.spark.sql.functions._

import pagerank.df.PageRankDF
import pagerank.rdd.PageRankRDD
import pagerank.rddoptimized.PageRankRDDOptimized



/**
 * Point d'entrée principal pour exécuter PageRank
 * sur les variantes RDD, DataFrame et RDD Optimisé.
 */
object Main {

  // =======================
  // Logger
  // =======================
  val logger: Logger = Logger.getLogger(getClass.getName)

  // =======================
  // Méthodes d'exécution
  // =======================

  /** Exécution PageRank RDD */
  def computePageRankRDD(
      input: String,
      output: String,
      iterations: Int,
      debug: Boolean,
      plot: Boolean = false
  )(implicit spark: SparkSession): Unit = {
    val start = System.nanoTime()

    val lines: RDD[String] = GraphUtils.readAsRDD(input)
    val links: RDD[(String, Seq[String])] = GraphUtils.parseGraphRDD(lines, debug, logger, Some(input))

    val v = PageRankRDD.computePageRank(
      links = links,
      iterations = iterations,
      debug = debug,
      plot = plot,
      logger = logger,
      outputDir = Some(output)
    )

    val duration = (System.nanoTime() - start) / 1e9
    logger.info(f"==== Temps d'exécution RDD : $duration%.2f s ====")

    PageRankUtils.appendBenchmark("RDD", input, iterations, v.count(), links.count(), duration, output)
  }

  /** Exécution PageRank DataFrame */
  def computePageRankDF(
      input: String,
      output: String,
      iterations: Int,
      debug: Boolean,
      plot: Boolean = false
  )(implicit spark: SparkSession): Unit = {
    val start = System.nanoTime()

    val lines = GraphUtils.readAsDataset(input)
    val links = GraphUtils.parseGraphDF(lines, debug, logger, Some(input))

    val v = PageRankDF.computePageRank(
      links = links,
      iterations = iterations,
      debug = debug,
      plot = plot,
      logger = logger,
      outputDir = Some(output)
    )

    val duration = (System.nanoTime() - start) / 1e9
    logger.info(f"==== Temps d'exécution DF : $duration%.2f s ====")

    PageRankUtils.appendBenchmark("DF", input, iterations, v.count(), links.count(), duration, output)
  }

  /** Exécution PageRank RDD optimisé */
  def computePageRankRDDOptimized(
      input: String,
      output: String,
      iterations: Int,
      debug: Boolean,
      plot: Boolean = false
  )(implicit spark: SparkSession): Unit = {
    val start = System.nanoTime()

    val lines: RDD[String] = GraphUtils.readAsRDD(input)
    val links: RDD[(String, Seq[String])] = GraphUtils.parseGraphRDD(lines, debug, logger, Some(input))

    val v = PageRankRDDOptimized.computePageRank(
      links = links,
      iterations = iterations,
      debug = debug,
      plot = plot,
      logger = logger,
      outputDir = Some(output)
    )

    val duration = (System.nanoTime() - start) / 1e9
    logger.info(f"==== Temps d'exécution RDD Optimisé : $duration%.2f s ====")

    PageRankUtils.appendBenchmark("RDD_OPT", input, iterations, v.count(), links.count(), duration, output)
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
    val iterations = if (args.length > 3 && !args(3).startsWith("--")) args(3).toInt else 10
    val plot       = args.contains("--plot")
    val debug      = args.contains("--debug")

    // Parsing plus robuste
    val positionalArgs = args.filterNot(_.startsWith("--"))

    logger.info(s"-> Input: $input")
    logger.info(s"-> Output: $output")
    logger.info(s"-> Méthode: $method")
    logger.info(s"-> Itérations: $iterations")
    logger.info(s"-> Plot: $plot")
    logger.info(s"-> Debug: $debug")

    // Initialisation Spark
    implicit val spark: SparkSession = SparkSession.builder
      .appName(s"PageRank-$method")
      .getOrCreate()

    try {
      method match {
        case "rdd" =>
          computePageRankRDD(input, output, iterations, debug, plot)

        case "df" =>
          computePageRankDF(input, output, iterations, debug, plot)

        case "rdd_optimized" =>
          computePageRankRDDOptimized(input, output, iterations, debug, plot)

        case "all" =>
          logger.info("Exécution de toutes les variantes (RDD, DF, RDD_Optimized)")
          computePageRankRDD(input, output, iterations, debug, plot)
          computePageRankDF(input, output, iterations, debug, plot)
          computePageRankRDDOptimized(input, output, iterations, debug, plot)

        case other =>
          System.err.println(s"Type d'exécution inconnu : $other")
          System.exit(1)
      }

      if (plot) {
        logger.info("Génération du graphique de convergence…")
        // TODO: appel au script Python de plotting si nécessaire
      }

    } finally {
      logger.info("==== [Fin d'exécution] PageRank ====")
      spark.stop()
    }
  }
}
