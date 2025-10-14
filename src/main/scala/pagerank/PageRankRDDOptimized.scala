package pagerank

import org.apache.log4j.Logger
import org.apache.spark.HashPartitioner
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

/**
  * Implémentation optimisée du PageRank RDD :
  *  - utilisation d’un HashPartitioner partagé
  *  - limitation des shuffles (reduceByKey avec partitioner)
  *  - persistences contrôlées
  *  - prise en compte de la masse pendante
  *  - option plot/export comme PageRankRDD
  */
object PageRankRDDOptimized {

  // =========================================================================
  // (1) Étape élémentaire du calcul PageRank partitionné
  // =========================================================================
  def oneStepPartitioned(
      ranks: RDD[(String, Double)],
      nodesWithEmpty: RDD[(String, (Int, Array[String]))],
      N: Double,
      beta: Double,
      partitioner: HashPartitioner,
      debug: Boolean,
      logger: Logger
  ): RDD[(String, Double)] = {

    import org.apache.spark.rdd.RDD.rddToPairRDDFunctions

    // (1) Calcul des contributions
    val contribs = nodesWithEmpty
      .join(ranks)
      .flatMap { case (_, ((deg, outs), rank)) =>
        if (deg == 0) Iterator.empty
        else outs.iterator.map(dest => (dest, rank / deg))
      }
      .reduceByKey(partitioner, _ + _)

    // (2) Ajout de baseRDD (valeurs par défaut) et somme
    val baseRDD = ranks.mapValues(_ => (1.0 - beta) / N)
    val next = baseRDD.leftOuterJoin(contribs).mapValues {
      case (b, optC) => b + beta * optC.getOrElse(0.0)
    }

    if (debug) {
      logger.debug("[oneStepPartitioned] Exemple de rangs :")
      next.take(10).foreach { case (p, r) => logger.debug(f"$p%-20s => $r%.6f") }
    }

    next
  }

  // =========================================================================
  // (2) Calcul complet du PageRank optimisé
  // =========================================================================
  def computePageRank(
      links: RDD[(String, Seq[String])],
      iterations: Int,
      beta: Double = 0.85,
      debug: Boolean = false,
      plot: Boolean = false,
      logger: Logger,
      storage: StorageLevel = StorageLevel.MEMORY_AND_DISK_SER,
      outputDir: String = "",
      numParts: Int = 0
  )(implicit spark: SparkSession): RDD[(String, Double)] = {

    import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
    val sc = spark.sparkContext

    // (0) Partitionner et préparer le graphe
    val P = new HashPartitioner(if (numParts > 0) numParts else sc.defaultParallelism * 2)

    logger.info(s"[PageRankRDDOptimized] Initialisation ($iterations itérations, β=$beta, parts=${P.numPartitions})")

    val sources = links.keys
    val dests   = links.values.flatMap(identity)
    val allNodes = (sources union dests).distinct().persist(storage)
    val N = allNodes.count().toDouble

    // === Structure partitionnée du graphe ===
    val outMap = links
      .reduceByKey(P, _ ++ _)
      .mapValues(_.distinct.toArray)
      .mapValues(arr => (arr.length, arr))
      .partitionBy(P)
      .persist(storage)

    val nodesWithEmpty = allNodes
      .map(id => (id, (0, Array.empty[String])))
      .partitionBy(P)
      .leftOuterJoin(outMap)
      .mapValues {
        case ((_, _), Some((deg, outs))) => (deg, outs)
        case _                           => (0, Array.empty[String])
      }
      .persist(storage)

    // === Initialisation du vecteur de rangs ===
    var ranks = allNodes.map(id => (id, 1.0 / N)).partitionBy(P).persist(storage)

    // === Historique (si plot) ===
    val history =
      if (plot) {
        val buf = scala.collection.mutable.ArrayBuffer.empty[Map[String, Double]]
        PageRankUtils.appendSnapshot(Some(buf), ranks)
        Some(buf)
      } else None

    // === Boucle d’itérations ===
    for (i <- 1 to iterations) {
      if (debug) logger.info(s"[PageRankRDDOptimized] --- Itération $i/$iterations ---")

      // (a) Masse pendante
      val danglingMass = nodesWithEmpty.join(ranks)
        .filter { case (_, ((deg, _), _)) => deg == 0 }
        .map { case (_, ((_, _), r)) => r }
        .sum()

      // (b) Étape principale
      var newRanks = oneStepPartitioned(ranks, nodesWithEmpty, N, beta, P, debug, logger)

      // (c) Redistribution de la masse pendante
      val redistribution = beta * danglingMass / N
      newRanks = newRanks.mapValues(_ + redistribution).partitionBy(P).persist(storage)

      // (d) Normalisation
      val sumRanks = newRanks.values.sum()
      ranks.unpersist(false)
      ranks = newRanks.mapValues(_ / sumRanks).partitionBy(P).persist(storage)

      // (e) Ajout au plot
      if (plot)
        PageRankUtils.appendSnapshot(history, ranks)

      if (debug)
        logger.info(f"[PageRankRDDOptimized] Itération $i terminée (somme=$sumRanks%.6f)")
    }

    logger.info("[PageRankRDDOptimized] Calcul PageRank terminé ✅")

    // (f) Export CSV
    if (plot && outputDir.nonEmpty)
      PageRankUtils.exportHistoryToCSV(history.get.toSeq, outputDir, logger)

    // nettoyage
    nodesWithEmpty.unpersist(false)
    allNodes.unpersist(false)

    ranks
  }
}