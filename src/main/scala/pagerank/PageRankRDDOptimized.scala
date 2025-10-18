package pagerank

import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.Partitioner
import org.apache.spark.storage.StorageLevel
import org.apache.spark.HashPartitioner

object PageRankRDDOptimized {

    /**
    * Effectue une itération du calcul PageRank optimisée avec partitioning :
    * v_{i+1} = M × v_i
    *
    * CRITICAL: Tous les RDDs utilisent le même partitioner pour éviter les shuffles
    *
    * @param ranks        vecteur de rangs actuel (page -> rank) - DOIT être partitionné avec P
    * @param nodesWithOut graphe avec degré sortant (page -> (degré, outlinks)) - partitionné avec P
    * @param N            nombre total de nœuds
    * @param damping      facteur de damping (beta)
    * @param partitioner  partitioner utilisé (HashPartitioner)
    * @param debug        booleen pour afficher les messages de debug
    * @param logger       logger permettant d'afficher les messages de log
    * @param storage      niveau de persistance
    */
    def oneStep(
        ranks: RDD[(String, Double)],
        nodesWithOut: RDD[(String, (Int, Array[String]))],
        N: Double,
        damping: Double = 1.0,
        partitioner: Partitioner,
        debug: Boolean = false,
        logger: Logger,
        storage: StorageLevel = StorageLevel.MEMORY_ONLY
    ): RDD[(String, Double)] = {

    // Calculer la masse des dangling nodes (nœuds sans liens sortants)
    // join() est efficace car ranks et nodesWithOut partagent le même partitioner
    val danglingMass = nodesWithOut.join(ranks)
      .filter { case (_, ((deg, _), _)) => deg == 0 }
      .map { case (_, ((_, _), r)) => r }
      .sum()

    // Calculer les contributions des nœuds non-dangling
    // join() sans shuffle car même partitioner
    val contribs = nodesWithOut.join(ranks)
      .flatMap { case (_, ((deg, outs), r)) =>
        if (deg == 0) Iterator.empty
        else outs.iterator.map(d => (d, r / deg))
      }
      .reduceByKey(partitioner, _ + _) // CRITICAL: utiliser le partitioner pour éviter shuffle

    // Base du PageRank : redistribution uniforme + masse des dangling nodes
    val base = ((1.0 - damping) / N) + (damping * danglingMass / N)

    // CRITICAL: Créer baseRDD à partir de ranks pour préserver le partitioner
    val baseRDD = ranks.mapValues(_ => base)

    // leftOuterJoin sans shuffle car baseRDD et contribs ont le même partitioner
    val newRanks = baseRDD
      .leftOuterJoin(contribs)
      .mapValues { case (b, cOpt) => b + damping * cOpt.getOrElse(0.0) }
      .partitionBy(partitioner) // assure que le partitioner est préservé
      .persist(storage)

    // Normaliser pour que la somme = 1.0
    val sumRanks = newRanks.values.sum()
    val normalizedRanks = newRanks
      .mapValues(_ / sumRanks)
      .partitionBy(partitioner) // CRITICAL: préserver le partitioner
      .persist(storage)

    // Cleanup
    newRanks.unpersist(blocking = false)

    normalizedRanks
  }

  /**
    * Calcul complet du PageRank sur N itérations optimisé avec partitioning
    *
    * OPTIMIZATION STRATEGY:
    * 1. Utilise un HashPartitioner cohérent pour TOUS les RDDs
    * 2. Pre-partitionne les structures de données avant les itérations
    * 3. Évite les shuffles dans les joins/reduceByKey en gardant le même partitioner
    * 4. Utilise Array au lieu de Seq pour réduire l'overhead mémoire
    *
    * @param graph       le graphe RDD à analyser
    * @param iterations  nombre d'itérations
    * @param damping     facteur de damping (beta), typiquement 0.85
    * @param debug       afficher les logs de debug
    * @param plot        capturer l'historique pour export
    * @param logger      logger pour les messages
    * @param outputDir   répertoire pour exporter l'historique (si plot=true)
    * @param numParts    nombre de partitions (0 = auto = defaultParallelism * 2)
    * @param storage     niveau de persistance
    */
  def computePageRank(
      graph: GraphRDD,
      iterations: Int,
      damping: Double = 1.0,
      debug: Boolean = false,
      plot: Boolean = false,
      logger: Logger,
      outputDir: Option[String] = None,
      numParts: Int = 0,
      storage: StorageLevel = StorageLevel.MEMORY_ONLY
  )(implicit spark: SparkSession): RDD[(String, Double)] = {

    val sc = spark.sparkContext

    // CRITICAL: Créer un partitioner cohérent pour TOUS les RDDs
    // numParts = 0 => auto = defaultParallelism * 2 (meilleur pour les gros graphes)
    val P = new HashPartitioner(
      if (numParts > 0) numParts
      else sc.defaultParallelism * 2
    )

    if (debug) {
      logger.debug(s"[PageRankRDDOptimized] Utilisation de ${P.numPartitions} partitions")
    }

    val N: Double = graph.nNodes.toDouble
    val allNodes: RDD[String] = graph.allNodes.persist(storage)
    val links: RDD[(String, Seq[String])] = graph.links

    // === STEP 1: Préparer la structure d'adjacence avec partitioning ===
    // Fusionner les duplicatas, déduplicater les outlinks, compter le degré
    val outMap: RDD[(String, (Int, Array[String]))] = links
      .reduceByKey(P, _ ++ _)                     // merge avec partitioner P
      .mapValues(_.distinct.toArray)              // dédup + conversion en Array (plus efficace)
      .mapValues(arr => (arr.length, arr))        // (degré sortant, outlinks[])
      .partitionBy(P)                             // CRITICAL: partitionner avec P
      .persist(storage)

    // === STEP 2: Créer la table complète incluant les dangling nodes ===
    // CRITICAL: Tous les nœuds doivent être présents, même ceux sans outlinks
    val nodesWithOut: RDD[(String, (Int, Array[String]))] = allNodes
      .map(id => (id, (0, Array.empty[String]))) // dangling node par défaut
      .partitionBy(P)                             // CRITICAL: partitionner avec P
      .leftOuterJoin(outMap)                      // join SANS SHUFFLE (même partitioner)
      .mapValues {
        case ((_, _), Some((deg, outs))) => (deg, outs)
        case _                           => (0, Array.empty[String])
      }
      .persist(storage)

    // === STEP 3: Initialiser les rangs avec le même partitioner ===
    var ranks: RDD[(String, Double)] = allNodes
      .map(id => (id, 1.0 / N))
      .partitionBy(P)                             // CRITICAL: partitionner avec P
      .persist(storage)

    // Historique optionnel
    val history =
      if (plot) {
        val buf = scala.collection.mutable.ArrayBuffer.empty[Map[String, Double]]
        PageRankUtils.appendSnapshot(Some(buf), ranks)
        Some(buf)
      } else None

    // === STEP 4: Itérations PageRank ===
    for (i <- 1 to iterations) {
      val newRanks = oneStep(
        ranks = ranks,
        nodesWithOut = nodesWithOut,
        N = N,
        damping = damping,
        partitioner = P,                          // CRITICAL: passer le même partitioner
        debug = debug,
        logger = logger,
        storage = storage
      )

      // Cleanup des anciens rangs
      ranks.unpersist(blocking = false)
      ranks = newRanks

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

    // Forcer la matérialisation finale
    val finalCount = ranks.count()
    if (debug) {
      logger.debug(s"[PageRankRDDOptimized] Nombre final de nœuds: $finalCount")
    }

    // Cleanup
    nodesWithOut.unpersist(blocking = false)
    outMap.unpersist(blocking = false)
    allNodes.unpersist(blocking = false)

    ranks
  }

}