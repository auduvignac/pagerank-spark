package pagerank

import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object PageRankRDD {

    /**
    * Effectue une itération du calcul PageRank :
    * v_{i+1} = M × v_i
    *
    * @param ranks     vecteur de rangs actuel (page -> rank)
    * @param links     graphe des liens sortants (page -> outlinks)
    * @param allNodes  ensemble de toutes les pages
    * @param debug     booleen pour afficher les messages de debug
    * @param logger    logger permettant d'afficher les messages de log
    */
    def oneStep(
        ranks: RDD[(String, Double)],
        links: RDD[(String, Seq[String])],
        N: Double,
        damping: Double = 1.0,
        debug: Boolean = false,
        logger: Logger,
    ): RDD[(String, Double)] = {

    // === Distribution : chaque page "src" distribue son rang à ses destinations ===
    val contributions = links.join(ranks).values.flatMap{ case (links, rank) =>
        val size = links.size
        links.map(url => (url, rank / size))
      }

    // === Agrégation : chaque page "dest" reçoit la somme des contributions ===
    val nextranks = contributions
      .reduceByKey(_ + _)
      .mapValues(rank => (1 - damping) / N + damping * rank)

    // === Retour
    nextranks
  }

  def computePageRank(
      graph: GraphRDD,
      iterations: Int,
      damping: Double = 1.0,
      debug: Boolean = false,
      plot: Boolean = false,
      logger: Logger,
      outputDir: Option[String] = None
  )(implicit spark: SparkSession): RDD[(String, Double)] = {

    val sc = spark.sparkContext

    val N: Double = graph.nNodes.toDouble
    val allNodes: RDD[String] = graph.allNodes
    val links: RDD[(String, Seq[String])] = graph.links

    // === Initialisation du vecteur de rangs ===
    var ranks: RDD[(String, Double)] = allNodes
      .map(n => (n, 1.0 / N))

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
        N = N,
        damping = damping,
        debug = debug,
        logger = logger
      )

      if (debug) {
        val iterWord = if (i > 1) "iterations" else "iteration"
        logger.debug(s"==== Nouveau vecteur après $i $iterWord ====")
        ranks.collect().foreach { case (node, rank) =>
          logger.debug(f"$node%-5s : $rank%.6f")
        }
      }

      if (plot) {
        PageRankUtils.appendSnapshot(history, ranks)
      }

    }

    if (plot && outputDir.isDefined) {
      PageRankUtils.exportHistoryToCSV(history.get.toSeq, outputDir.get, logger)
    }

    ranks
  }

}