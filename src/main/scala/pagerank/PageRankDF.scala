package pagerank

import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.functions._

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
        v: DataFrame,
        links: DataFrame,
        allNodes: Seq[String],
        debug: Boolean = false,
        logger: Logger
    )(implicit spark: SparkSession): DataFrame = {

      import spark.implicits._

      // === (1) Calcul du degré sortant de chaque page ===
      val outDegrees = links
        .groupBy($"page")
        .agg(count($"outlink").as("degree"))

      // === (2) Distribution du rang : chaque source envoie une contribution à ses destinations ===
      val contributionsDetailed = links
        .join(v, Seq("page"))          // (page, outlink, rank)
        .join(outDegrees, Seq("page")) // (page, outlink, rank, degree)
        .withColumn("contrib", $"rank" / $"degree")
        .select(
          $"outlink".as("dest"),       // destination du lien
          $"page".as("src"),           // source du lien
          $"contrib"
        )

      if (debug) {
        logger.debug("==== Contributions détaillées (chaque source distribue son rang) ====")
        contributionsDetailed.collect().foreach { row =>
          val dest    = row.getAs[String]("dest")
          val src     = row.getAs[String]("src")
          val contrib = row.getAs[Double]("contrib")
          logger.debug(f"$dest%-5s reçoit $contrib%.6f de $src")
        }
      }

      // === (3) Agrégation : somme des contributions reçues par chaque page ===
      val received = contributionsDetailed
        .groupBy("dest")
        .agg(sum("contrib").as("rank"))
        .withColumnRenamed("dest", "page")

      if (debug) {
        logger.debug("==== Rangs reçus (somme des contributions entrantes) ====")
        received.collect().foreach { row =>
          val node = row.getAs[String]("page")
          val rank = row.getAs[Double]("rank")
          logger.debug(f"$node%-5s : $rank%.6f")
        }
      }

      // === (4) Réintégration des pages sans contribution ===
      val allNodesDF = allNodes.toDF("page")

      val nextRanks = allNodesDF
        .join(received, Seq("page"), "left_outer")
        .na.fill(0.0, Seq("rank"))

      nextRanks
    }

  // Calcul complet du PageRank sur N itérations (avec option d'historique)
  def computePageRank(
      links: DataFrame,
      iterations: Int,
      debug: Boolean = false,
      plot: Boolean = false,
      logger: Logger,
      outputDir: Option[String] = None
  )(implicit spark: SparkSession): DataFrame = {

    import spark.implicits._

    val allNodes = links.select("page")
          .union(links.select("outlink"))
          .distinct()
          .as[String]
          .collect()
          .sorted

    val N = allNodes.length.toDouble
    var v = allNodes.map(n => (n, 1.0 / N)).toSeq.toDF("page", "rank")

    // Si mode "plot" activé, conservation de l'historique
    val history =
      if (plot) {
        val buf = scala.collection.mutable.ArrayBuffer.empty[Map[String, Double]]
        PageRankUtils.appendSnapshot(Some(buf), v)
        Some(buf)
      } else None


    for (i <- 1 to iterations) {
      v = oneStep(v, links, allNodes, debug, logger)

      if (debug) {
        val iterWord = if (i > 1) "itérations" else "itération"
        logger.debug(s"==== Nouveau vecteur après $i $iterWord ====")
        v.collect().foreach { row =>
          val node = row.getAs[String]("page")
          val rank = row.getAs[Double]("rank")
          logger.debug(f"$node%-5s : $rank%.6f")
        }
      }

      if (plot)
        PageRankUtils.appendSnapshot(history, v)
    }

    // === Export CSV si demandé ===
    if (plot && outputDir.isDefined) {
      PageRankUtils.exportHistoryToCSV(history.get.toSeq, outputDir.get, logger)
    }

    v
  }

}