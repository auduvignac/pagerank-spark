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
        v: DataFrame,                          // RDD représentant le rang actuel de chaque page : (page, rank)
        links: DataFrame,                      // RDD représentant la structure du graphe : (page source, liste des pages de destination)
        allNodes: Dataset[String],             // RDD contenant la liste de toutes les pages (utile pour réintégrer celles sans contribution)
        N: Double,                                // Nombre total de noeuds
        damping: Double = 1.0,                 // Valeur du facteur d'amortissement (par défaut 1.0 : pas d'amortissement)
        debug: Boolean = false,                // Active ou non les logs détaillés
        logger: Logger                         // Logger pour afficher les informations de débogage
    )(implicit spark: SparkSession): DataFrame = { // Retourne un nouveau RDD (page, nouveauRank)

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

    val allNodesDF = allNodes.toDF("page")

    // === (4) Retour du nouveau vecteur de rangs application du facteur d’amortissement ===
    val nextRanks = allNodesDF
      .join(received, Seq("page"), "left_outer")
      .na.fill(0.0, Seq("rank"))
      .withColumn("rank", lit((1 - damping) / N) + $"rank" * damping) // Application du facteur d'amortissement

    // === (5) Retour ===
    nextRanks
  }

  // Calcul complet du PageRank sur N itérations (avec option d'historique)
  def computePageRank(
      links: DataFrame,
      allPages: Dataset[String],
      iterations: Int,
      damping: Double = 1.0,
      debug: Boolean = false,
      plot: Boolean = false,
      logger: Logger,
      outputDir: Option[String] = None
  )(implicit spark: SparkSession): DataFrame = {

    import spark.implicits._

    // Use all source pages + all destination pages to get complete node list
    // DS : Dataset[string]
    val allNodesDS = allPages
          .union(links.select("outlink").as[String])
          .distinct()
          .cache()

    val N = allNodesDS.count().toDouble
    var v = allNodesDS.map(n => (n, 1.0 / N)).toDF("page", "rank")

    // Si mode "plot" activé, conservation de l'historique
    val history =
      if (plot) {
        val buf = scala.collection.mutable.ArrayBuffer.empty[Map[String, Double]]
        PageRankUtils.appendSnapshot(Some(buf), v)
        Some(buf)
      } else None


    for (i <- 1 to iterations) {
      v = oneStep(v, links, allNodesDS, N, damping, debug, logger)

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