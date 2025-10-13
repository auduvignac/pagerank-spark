package pagerank

import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD
import org.apache.spark.Partitioner
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

object PageRankRDDOptimized {

  /**
    * Effectue une itération du calcul PageRank optimisé :
    * v_{i+1} = M × v_i
    *
    * Optimisations :
    *  - co-partitionnement entre links, v et allNodes
    *  - réduction du volume shufflé (on ne transporte plus la source)
    *  - rightOuterJoin pour réintégrer les pages sans contribution
    */
  def oneStep(
      v: RDD[(String, Double)],              // vecteur de rangs actuel (page -> rank)
      links: RDD[(String, Seq[String])],     // graphe des liens sortants (page source -> destinations)
      allNodesZero: RDD[(String, Double)],   // RDD de toutes les pages avec rang 0 (co-partitionné)
      N: Double,                             // nombre total de noeuds
      damping: Double = 1.0,                 // facteur d'amortissement
      partitioner: Partitioner,              // partitionneur partagé
      debug: Boolean = false,                // logs détaillés
      logger: Logger                         // logger
  ): RDD[(String, Double)] = {

    // === (1) Distribution : chaque page "src" distribue son rang à ses destinations ===
    val contributions: RDD[(String, Double)] = links
      .join(v, partitioner) // jointure co-partitionnée
      .flatMap { case (_, (outs, rankSrc)) =>
        if (outs.isEmpty) Iterator.empty
        else {
          val share = rankSrc / outs.size
          outs.iterator.map(dest => (dest, share))
        }
      }

    // === (2) Agrégation : chaque page "dest" reçoit la somme des contributions ===
    val reduceFunc: (Double, Double) => Double = _ + _
    val receivedRanks: RDD[(String, Double)] =
      contributions.reduceByKey(partitioner, reduceFunc)

    // === (3) Réintégration des pages sans contribution ===
    val allRanks: RDD[(String, Double)] =
      receivedRanks
        .rightOuterJoin(allNodesZero, partitioner)
        .mapValues { case (maybeSum, _) => maybeSum.getOrElse(0.0) }

    // === (4) Application du facteur d’amortissement ===
    val nextRanks: RDD[(String, Double)] =
      allRanks.mapValues(rank => (1 - damping) / N + damping * rank)

    if (debug) {
      logger.debug("==== Rangs mis à jour après itération ====")
      nextRanks.take(20).foreach { case (node, rank) =>
        logger.debug(f"$node%-5s : $rank%.6f")
      }
    }

    nextRanks
  }

  /**
    * Calcul complet du PageRank optimisé sur N itérations
    *
    * @param links       graphe des liens sortants
    * @param iterations  nombre d’itérations à exécuter
    * @param damping     facteur d’amortissement
    * @param partitioner partitionneur utilisé pour toutes les jointures
    * @param debug       active les logs détaillés
    * @param plot        génère un historique pour affichage
    * @param logger      logger
    * @param outputDir   dossier de sortie optionnel
    */
  def computePageRank(
      links: RDD[(String, Seq[String])],
      iterations: Int,
      damping: Double = 1.0,
      partitioner: Partitioner,
      storage: String,
      debug: Boolean = false,
      plot: Boolean = false,
      logger: Logger,
      outputDir: Option[String] = None
  )(implicit spark: SparkSession): RDD[(String, Double)] = {

    val sc = spark.sparkContext

    // === (1) Préparation du graphe complet (co-partitionné) ===
    val srcNodes = links.keys
    val dstNodes = links.values.flatMap(identity)
    val allNodes = srcNodes.union(dstNodes).distinct().cache()
    val N = allNodes.count().toDouble

    val linksFull: RDD[(String, Seq[String])] = {
      val emptyByNode = allNodes.map(n => (n, Seq.empty[String]))
      emptyByNode
        .leftOuterJoin(links)
        .mapValues { case (_, maybeOuts) => maybeOuts.getOrElse(Seq.empty[String]) }
        .partitionBy(partitioner) // une seule fois
        .persist(storageLevelOf(storage))
    }

    val allNodesZero: RDD[(String, Double)] =
      allNodes
        .map(n => (n, 0.0))
        .partitionBy(partitioner)
        .persist(storageLevelOf(storage))

    // === (2) Initialisation du vecteur de rangs ===
    var v: RDD[(String, Double)] = allNodes
      .map(n => (n, 1.0 / N))
      .partitionBy(partitioner)
      .persist(storageLevelOf(storage))

    // Historique optionnel (pour le plot)
    val history =
      if (plot) {
        val buf = scala.collection.mutable.ArrayBuffer.empty[Map[String, Double]]
        PageRankUtils.appendSnapshot(Some(buf), v)
        Some(buf)
      } else None

    // === (3) Boucle principale ===
    for (i <- 1 to iterations) {
      val newV = oneStep(v, linksFull, allNodesZero, N, damping, partitioner, debug, logger)
        .persist(storageLevelOf(storage))

      // libère l’ancienne version toutes les 5 itérations pour éviter la surcharge mémoire
      if (i % 5 == 0 || i == iterations) v.unpersist(blocking = false)

      v = newV

      if (debug) {
        val iteration_str = if (i > 1) "itérations" else "itération"
        logger.debug(s"==== Nouveau vecteur après $i $iteration_str ====")
        v.take(20).foreach { case (node, rank) =>
          logger.debug(f"$node%-5s : $rank%.6f")
        }
      }

      if (plot)
        PageRankUtils.appendSnapshot(history, v)
    }

    // === (4) Export CSV si demandé ===
    if (plot && outputDir.isDefined) {
      PageRankUtils.exportHistoryToCSV(history.get.toSeq, outputDir.get, logger)
    }

    v
  }

  def storageLevelOf(name: String): StorageLevel =
    name.toUpperCase match {
      case "MEMORY_ONLY"         => StorageLevel.MEMORY_ONLY
      case "MEMORY_ONLY_SER"     => StorageLevel.MEMORY_ONLY_SER
      case "MEMORY_AND_DISK"     => StorageLevel.MEMORY_AND_DISK
      case "MEMORY_AND_DISK_SER" => StorageLevel.MEMORY_AND_DISK_SER
      case "DISK_ONLY"           => StorageLevel.DISK_ONLY
      case _                     => StorageLevel.MEMORY_ONLY
    }

}