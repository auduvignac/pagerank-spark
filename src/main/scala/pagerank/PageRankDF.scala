package pagerank

import org.apache.log4j.Logger
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{SparkSession, DataFrame, Dataset, Row}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object PageRankDF {

    /**
    * Effectue une itération du calcul PageRank :
    * v_{i+1} = M × v_i
    *
    * @param v         vecteur de rangs actuel (page -> rank)
    * @param links     graphe des liens sortants (page -> outlinks)
    * @param allNodes  ensemble de toutes les pages
    * @param debug     booleen pour afficher les messages de debug
    * @param logger    logger permettant d'afficher les messages de log
    */
    def oneStep(
        ranks: DataFrame,
        links: DataFrame,
        allNodes: Dataset[Row],
        N: Double,
        damping: Double = 1.0,
        debug: Boolean = false,
        logger: Logger
    )(implicit spark: SparkSession): DataFrame = {

    import spark.implicits._

    val outdeg = links.groupBy("page").agg(count("outlink").as("outdeg"))

    val contributions = links
      .join(outdeg, Seq("page"))
      .join(ranks, Seq("page"))
      .select($"outlink".as("page"), ($"rank" / $"outdeg").as("contribution"))

    val nextranks = contributions
      .groupBy("page")
      .agg(sum("contribution").as("new_rank"))
      .withColumn(
        "rank",
        (lit(damping) * $"new_rank") + (lit(1.0 - damping) / lit(N))
      )
      .drop("new_rank")

    nextranks
  }

  def computePageRank(
      graph: GraphDF,
      iterations: Int,
      damping: Double = 1.0,
      debug: Boolean = false,
      plot: Boolean = false,
      logger: Logger,
      outputDir: Option[String] = None
  )(implicit spark: SparkSession): DataFrame = {

    import spark.implicits._

    val N: Double = graph.nNodes.toDouble
    val allNodes: Dataset[Row] = graph.allNodes
    val links: DataFrame = graph.links

    // === Initialisation du vecteur de rangs ===
    var ranks = allNodes
      .withColumnRenamed("node", "page")
      .withColumn("rank", lit(1.0 / N))

    // Historique optionnel
    val history =
      if (plot) {
        val buf = scala.collection.mutable.ArrayBuffer.empty[Map[String, Double]]
        PageRankUtils.appendSnapshot(Some(buf), ranks)
        Some(buf)
      } else None


    for (i <- 1 to iterations) {
      ranks = oneStep(
        ranks = ranks,
        links = links,
        allNodes = allNodes,
        N = N,
        damping = damping,
        debug = debug,
        logger = logger
      )

      if (debug) {
        val iterWord = if (i > 1) "itérations" else "itération"
        logger.debug(s"==== Nouveau vecteur après $i $iterWord ====")
        ranks.collect().foreach { row =>
          val node = row.getAs[String]("page")
          val rank = row.getAs[Double]("rank")
          logger.debug(f"$node%-5s : $rank%.6f")
        }
      }

      if (plot)
        PageRankUtils.appendSnapshot(history, ranks)
    }

    // === Export CSV si demandé ===
    if (plot && outputDir.isDefined) {
      PageRankUtils.exportHistoryToCSV(history.get.toSeq, outputDir.get, logger)
    }

    ranks
  }
}