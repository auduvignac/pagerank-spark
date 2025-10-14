package pagerank

import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

object PageRankRDD {

  // =========================================================================
  // (1) Étape élémentaire du calcul PageRank : v_{i+1} = M × v_i
  // =========================================================================
  def oneStep(
      v: RDD[(String, Double)],              // vecteur des rangs actuels
      links: RDD[(String, Seq[String])],     // graphe des liens sortants
      allNodes: RDD[String],                 // ensemble de toutes les pages
      N: Double,                             // nombre total de nœuds
      damping: Double = 0.85,                // facteur d'amortissement
      debug: Boolean = false,                // affichage détaillé
      logger: Logger                         // logger Spark
  ): RDD[(String, Double)] = {

    import org.apache.spark.rdd.RDD.rddToPairRDDFunctions

    // === (1) Distribution : chaque page distribue son rang à ses destinations ===
    val contributions = links
      .join(v)
      .flatMap { case (src, (outs, rankSrc)) =>
        if (outs.isEmpty) Iterator.empty
        else outs.iterator.map(dest => (dest, rankSrc / outs.size))
      }

    // === (2) Agrégation des contributions reçues ===
    val receivedRanks = contributions.reduceByKey(_ + _)

    // === (3) Réintégration des pages sans contribution ===
    val allRanks = receivedRanks
      .union(allNodes.map(n => (n, 0.0)))
      .reduceByKey(_ + _)

    // === (4) Application du facteur d’amortissement ===
    val nextRanks = allRanks.mapValues(rank => (1 - damping) / N + damping * rank)

    if (debug) {
      logger.debug("==== [oneStep] Nouveau vecteur de rangs ====")
      nextRanks.take(10).foreach { case (page, r) =>
        logger.debug(f"$page%-20s => $r%.6f")
      }
    }

    nextRanks
  }

  // =========================================================================
  // (2) Calcul complet du PageRank (appel explicite à oneStep dans la boucle)
  // =========================================================================
  def computePageRank(
      links: RDD[(String, Seq[String])],
      iterations: Int,
      damping: Double = 0.85,
      debug: Boolean = false,
      plot: Boolean = false,
      logger: Logger,
      storage: StorageLevel = StorageLevel.MEMORY_AND_DISK,
      outputDir: String
  )(implicit spark: SparkSession): RDD[(String, Double)] = {

    import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
    val sc = spark.sparkContext

    logger.info(s"[PageRankRDD] Initialisation du calcul PageRank ($iterations itérations, damping=$damping)")

    // === (1) Préparation du graphe complet ===
    val srcNodes = links.keys
    val dstNodes = links.values.flatMap(identity)
    val allNodes = srcNodes.union(dstNodes).distinct().persist(storage)
    val N = allNodes.count().toDouble

    val linksFull: RDD[(String, Seq[String])] = {
      val emptyByNode = allNodes.map(n => (n, Seq.empty[String]))
      emptyByNode.leftOuterJoin(links)
        .mapValues { case (_, maybeOuts) => maybeOuts.getOrElse(Seq.empty[String]) }
        .persist(storage)
    }

    // === (2) Initialisation du vecteur de rangs ===
    var ranks: RDD[(String, Double)] = allNodes.map(n => (n, 1.0 / N)).persist(storage)

    // === (3) Initialisation de l’historique si plot ===
    val history =
      if (plot) {
        val buf = scala.collection.mutable.ArrayBuffer.empty[Map[String, Double]]
        PageRankUtils.appendSnapshot(Some(buf), ranks)
        Some(buf)
      } else None

    // === (4) Boucle principale ===
    for (i <- 1 to iterations) {
      if (debug) {
        logger.info(s"[PageRankRDD] --- Itération $i/$iterations ---")
      }

      // (a) Masse pendante : somme des rangs des pages sans liens sortants
      val danglingMass = linksFull.join(ranks)
        .filter { case (_, (outs, _)) => outs.isEmpty }
        .map { case (_, (_, r)) => r }
        .sum()

      // (b) Étape principale : appel à oneStep
      var newRanks = oneStep(ranks, linksFull, allNodes, N, damping, debug, logger)

      // (c) Redistribution de la masse pendante
      val redistribution = damping * danglingMass / N
      newRanks = newRanks.mapValues(_ + redistribution).persist(storage)

      // (d) Normalisation du vecteur (somme = 1)
      val sumRanks = newRanks.values.sum()
      ranks.unpersist(blocking = false)
      ranks = newRanks.mapValues(_ / sumRanks).persist(storage)

      // (e) Ajout de la snapshot pour le plotting
      if (plot)
        PageRankUtils.appendSnapshot(history, ranks)

      if (debug) {
        val sample = ranks.take(10)
        logger.info(f"[PageRankRDD] Itération $i terminée (somme=$sumRanks%.6f)")
        sample.foreach { case (n, r) => logger.info(f"  $n%-20s => $r%.6f") }
      }
    }

    logger.info("[PageRankRDD] Calcul PageRank terminé")

    // === (4) Export CSV ===
    if (plot && outputDir.nonEmpty) {
      PageRankUtils.exportHistoryToCSV(history.get.toSeq, outputDir, logger)
    }

    ranks
  }
}