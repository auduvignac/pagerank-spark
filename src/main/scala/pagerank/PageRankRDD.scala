package pagerank

import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

object PageRankRDD {

    /**
    * Effectue une itération du calcul PageRank :
    * v_{i+1} = M × v_i
    * Optimisé pour gérer les dangling nodes et la normalisation
    *
    * @param ranks        vecteur de rangs actuel (page -> rank)
    * @param nodesWithOut graphe avec degré sortant (page -> (degré, outlinks))
    * @param allNodes     ensemble de toutes les pages
    * @param N            nombre total de nœuds
    * @param damping      facteur de damping (beta)
    * @param debug        booleen pour afficher les messages de debug
    * @param logger       logger permettant d'afficher les messages de log
    */
    def oneStep(
        ranks: RDD[(String, Double)],
        nodesWithOut: RDD[(String, (Int, Seq[String]))],
        allNodes: RDD[String],
        N: Double,
        damping: Double = 1.0,
        debug: Boolean = false,
        logger: Logger
    ): RDD[(String, Double)] = {

    // Calculer la masse des dangling nodes (nœuds sans liens sortants)
    val danglingMass = nodesWithOut.join(ranks)
      .filter { case (_, ((deg, _), _)) => deg == 0 }
      .map { case (_, ((_, _), r)) => r }
      .sum()

    // Calculer les contributions des nœuds non-dangling
    val contribs = nodesWithOut.join(ranks)
      .flatMap { case (_, ((deg, outs), r)) =>
        if (deg == 0) Iterator.empty
        else outs.iterator.map(d => (d, r / deg))
      }
      .reduceByKey(_ + _)

    // Base du PageRank : redistribution uniforme + masse des dangling nodes
    val base = ((1.0 - damping) / N) + (damping * danglingMass / N)

    // Calculer les nouveaux rangs avec normalisation
    val newRanks = allNodes
      .map(id => (id, base))
      .leftOuterJoin(contribs)
      .mapValues { case (b, cOpt) => b + damping * cOpt.getOrElse(0.0) }

    // Normaliser pour que la somme = 1.0
    val sumRanks = newRanks.values.sum()
    val normalizedRanks = newRanks.mapValues(_ / sumRanks)

    normalizedRanks
  }

  def computePageRank(
      graph: GraphRDD,
      iterations: Int,
      damping: Double = 1.0,
      debug: Boolean = false,
      plot: Boolean = false,
      logger: Logger,
      outputDir: Option[String] = None,
      storage: StorageLevel = StorageLevel.MEMORY_ONLY,
      metrics: Boolean = false
  )(implicit spark: SparkSession): RDD[(String, Double)] = {

    val sc = spark.sparkContext

    val N: Double = graph.nNodes.toDouble
    val allNodes: RDD[String] = graph.allNodes.persist(storage)
    val links: RDD[(String, Seq[String])] = graph.links

    // === Préparation: calculer le degré sortant pour chaque nœud ===
    // Réduire les liens pour éliminer les doublons et compter les outlinks
    val outMap: RDD[(String, (Int, Seq[String]))] = links
      .reduceByKey(_ ++ _)
      .mapValues(_.distinct)
      .mapValues(outs => (outs.size, outs))

    // Créer la structure complète incluant les nœuds sans liens sortants (dangling nodes)
    val nodesWithOut: RDD[(String, (Int, Seq[String]))] = allNodes
      .map(nodeId => (nodeId, (0, Seq.empty[String])))
      .leftOuterJoin(outMap)
      .mapValues {
        case ((_, _), Some((deg, outs))) => (deg, outs)
        case _                           => (0, Seq.empty[String])
      }
      .persist(storage)

    // === Initialisation du vecteur de rangs ===
    var ranks: RDD[(String, Double)] = allNodes
      .map(n => (n, 1.0 / N))
      .persist(storage)

    // Historique optionnel
    val history =
      if (plot) {
        val buf = scala.collection.mutable.ArrayBuffer.empty[Map[String, Double]]
        PageRankUtils.appendSnapshot(Some(buf), ranks)
        Some(buf)
      } else None

    for (i <- 1 to iterations) {

      if (metrics) {
        logger.info(s"==== Début itération $i/$iterations ====")

        // Taguer tous les jobs de cette itération
        spark.sparkContext.setJobGroup(
          s"PageRank-RDD-iter-$i",
          s"PageRank RDD iteration $i"
        )
      }

      val newRanks = oneStep(
        ranks = ranks,
        nodesWithOut = nodesWithOut,
        allNodes = allNodes,
        N = N,
        damping = damping,
        debug = debug,
        logger = logger
      )

      if (metrics) {
        // Déclenche l’action qui matérialise cette itération
        // (permet à Spark de produire les métriques dans le listener)
        newRanks.count()
      }

      // Nettoyage et persistance
      ranks.unpersist(blocking = false)
      ranks = newRanks.persist(storage)

      if (metrics) {
        // Fin du tag
        spark.sparkContext.clearJobGroup()
      }

      if (debug) {
        val iterWord = if (i > 1) "iterations" else "iteration"
        logger.debug(s"==== Nouveau vecteur après $i $iterWord ====")
        val ranksArray = ranks.collect()
        ranksArray.foreach { case (node, rank) =>
          logger.debug(f"$node%-5s : $rank%.6f")
        }
        val sum = ranksArray.map(_._2).sum
        logger.debug(f"Somme des rangs: $sum%.6f")
      }

      if (plot) {
        PageRankUtils.appendSnapshot(history, ranks)
      }
    }

    if (plot && outputDir.isDefined) {
      PageRankUtils.exportHistoryToCSV(history.get.toSeq, outputDir.get, logger)
    }

    if (metrics) {
      // Taguer tous les jobs de cette itération
      spark.sparkContext.setJobGroup(
        s"PageRank-RDD-ranks.count()",
        s"PageRank-RDD-ranks.count()"
      )
    }

    // Forcer la matérialisation finale
    val finalCount = ranks.count()

    if (metrics) {
      // Fin du tag
      spark.sparkContext.clearJobGroup()
    }

    if (debug) {
      logger.debug(s"Nombre final de nœuds avec rang: $finalCount")
    }

    // Cleanup
    nodesWithOut.unpersist(blocking = false)
    allNodes.unpersist(blocking = false)

    ranks
  }

}