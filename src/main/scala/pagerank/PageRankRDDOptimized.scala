package pagerank

import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.Partitioner
import org.apache.spark.storage.StorageLevel

object PageRankRDDOptimized {

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
        partitioner: Partitioner,
        storage: StorageLevel
    ): RDD[(String, Double)] = {

    // === Distribution : chaque page "src" distribue son rang à ses destinations ===
    val contributions = links.join(ranks, partitioner).values.flatMap{ case (links, rank) =>
        val size = links.size
        links.map(url => (url, rank / size))
      }

    // === Agrégation : chaque page "dest" reçoit la somme des contributions ===
    val nextranks = contributions
      .reduceByKey(partitioner, _ + _)
      .mapValues(rank => (1 - damping) / N + damping * rank)

    // === Ajout de la persistance afin de limiter le recalcul ===
    nextranks.persist(storage)

    // === Retour
    nextranks
  }

  // Calcul complet du PageRank sur N itérations (avec option d'historique)
  def computePageRank(
      graph: GraphRDD,
      iterations: Int,
      damping: Double = 1.0,
      debug: Boolean = false,
      plot: Boolean = false,
      logger: Logger,
      outputDir: Option[String] = None,
      partitioner: Partitioner,
      storage: StorageLevel
  )(implicit spark: SparkSession): RDD[(String, Double)] = {

    val sc = spark.sparkContext

    val N: Double = graph.nNodes.toDouble
    val allNodes: RDD[String] = graph.allNodes
    val links: RDD[(String, Seq[String])] = graph.links

    // Répartition des données selon le même partitioner pour améliorer la localité
    val partitionedLinks = links.partitionBy(partitioner).persist(storage)

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
        links = partitionedLinks,
        N = N,
        damping = damping,
        debug = debug,
        logger = logger,
        partitioner = partitioner,
        storage = storage
      )

      if (debug) {
        val iteration_str = if (i > 1) "iterations" else "iteration"
        logger.debug(s"==== Nouveau vecteur après $i $iteration_str ====")
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