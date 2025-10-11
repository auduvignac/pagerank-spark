package pagerank

// =======================
// Imports
// =======================
import java.io.{File, PrintWriter}
import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SparkSession, Dataset}
import org.apache.spark.sql.functions._

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
      damping: Double,
      debug: Boolean,
      plot: Boolean = false
  )(implicit spark: SparkSession): Unit = {

    logger.info(s"==== [Traitement du graphe] [RDD] [$input] [$iterations itération] ====")

    val lines: RDD[String] = GraphUtils.readAsRDD(input)
    val links: RDD[(String, Seq[String])] = GraphUtils.parseGraphRDD(lines, debug, logger, Some(input))

    val start = System.nanoTime()

    val v = PageRankRDD.computePageRank(
      links = links,
      iterations = iterations,
      damping = damping,
      debug = debug,
      plot = plot,
      logger = logger,
      outputDir = Some(output)
    )

    val duration = (System.nanoTime() - start) / 1e9
    logger.info(f"==== Temps d'exécution RDD : $duration%.2f s ====")

    PageRankUtils.appendBenchmark("RDD", input, iterations, GraphUtils.countNodesRDD(links), GraphUtils.countEdgesRDD(links), duration, output)
  }

  /** Exécution PageRank DataFrame */
  def computePageRankDF(
      input: String,
      output: String,
      iterations: Int,
      damping: Double,
      debug: Boolean,
      plot: Boolean = false
  )(implicit spark: SparkSession): Unit = {

    logger.info(s"==== [Traitement du graphe] [DF] [$input] [$iterations itération] ====")


    val lines = GraphUtils.readAsDataset(input)
    val links = GraphUtils.parseGraphDF(lines, debug, logger, Some(input))
    val allPages = GraphUtils.extractAllPagesDF(lines)

    val start = System.nanoTime()

    val v = PageRankDF.computePageRank(
      links = links,
      allPages = allPages,
      iterations = iterations,
      damping = damping,
      debug = debug,
      plot = plot,
      logger = logger,
      outputDir = Some(output)
    )

    val duration = (System.nanoTime() - start) / 1e9
    logger.info(f"==== Temps d'exécution DF : $duration%.2f s ====")

    PageRankUtils.appendBenchmark("DF", input, iterations, GraphUtils.countNodesDF(links, allPages), GraphUtils.countEdgesDF(links), duration, output)
  }

  /** Exécution PageRank RDD optimisé */
  def computePageRankRDDOptimized(
      input: String,
      output: String,
      iterations: Int,
      damping: Double,
      debug: Boolean,
      plot: Boolean = false
  )(implicit spark: SparkSession): Unit = {

    logger.info(s"==== [Traitement du graphe] [RDD optimisé] [$input] [$iterations itération] ====")

    val lines: RDD[String] = GraphUtils.readAsRDD(input)
    val links: RDD[(String, Seq[String])] = GraphUtils.parseGraphRDD(lines, debug, logger, Some(input))

    val start = System.nanoTime()

    val v = PageRankRDDOptimized.computePageRank(
      links = links,
      iterations = iterations,
      damping = damping,
      debug = debug,
      plot = plot,
      logger = logger,
      outputDir = Some(output)
    )

    val duration = (System.nanoTime() - start) / 1e9
    logger.info(f"==== Temps d'exécution RDD Optimisé : $duration%.2f s ====")

    PageRankUtils.appendBenchmark("RDD_OPT", input, iterations, GraphUtils.countNodesRDD(links), GraphUtils.countEdgesRDD(links), duration, output)
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
    val damping = if (args.length > 4 && !args(4).startsWith("--")) args(4).toDouble else 1.0
    val plot       = args.contains("--plot")
    val debug      = args.contains("--debug")

    // Parsing plus robuste
    val positionalArgs = args.filterNot(_.startsWith("--"))

    logger.info(s"-> Input: $input")
    logger.info(s"-> Output: $output")
    logger.info(s"-> Méthode: $method")
    logger.info(s"-> Itérations: $iterations")
    logger.info(s"-> Facteur d'amortissement: $damping")
    logger.info(s"-> Plot: $plot")
    logger.info(s"-> Debug: $debug")

    // possibilité de passer plusieurs fichiers séparés par des virgules
    // pour en générer une boucle
    val inputs = input.split(",").toList

    // Initialisation Spark
    implicit val spark: SparkSession = SparkSession.builder
      .appName(s"PageRank-$method")
      .getOrCreate()

    for (input <- inputs) {

      logger.info(s"==== [Traitement du graphe] [$input] [itérations : $iterations] [facteur d'amortissement : $damping] ====")

      try {
        method match {
          case "rdd" =>
            computePageRankRDD(input, output, iterations, damping, debug, plot)

          case "df" =>
            computePageRankDF(input, output, iterations, damping, debug, plot)

          case "rdd_optimized" =>
            computePageRankRDDOptimized(input, output, iterations, damping, debug, plot)

          case "all" =>
            logger.info(s"Exécution de toutes les variantes (RDD, DF, RDD_Optimized) avec $iterations itérations et un facteur d'amortissement de $damping")
            computePageRankRDD(input, output, iterations, damping, debug, plot)
            computePageRankDF(input, output, iterations, damping, debug, plot)
            computePageRankRDDOptimized(input, output, iterations, damping, debug, plot)

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