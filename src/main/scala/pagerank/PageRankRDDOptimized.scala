package pagerank

import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.apache.spark.HashPartitioner

object PageRankRDDOptimized {

  /**
    * Effectue une itération du calcul PageRank :
    * v_{i+1} = M × v_i
    *
    * Optimisations :
    *  - mapPartitions pour limiter les allocations d’objets
    *  - partitionBy pour préserver la localité du join
    *  - rightOuterJoin pour éviter une passe complète sur allNodes
    */
  def oneStep(
      v: RDD[(String, Double)],
      links: RDD[(String, Seq[String])],
      allNodes: RDD[String],
      debug: Boolean = false,
      logger: Logger,
      partitioner: Option[org.apache.spark.Partitioner] = None
  ): RDD[(String, Double)] = {

    // === (1) Distribution : chaque page "src" distribue son rang à ses destinations ===
    val contributionsDetailed: RDD[(String, String, Double)] = links
      .join(v) // (src, (outlinks, rankSrc))
      .mapPartitions(
        iter => iter.flatMap { case (src, (outs, rankSrc)) =>
          if (outs.isEmpty) Iterator.empty
          else {
            val share = rankSrc / outs.size
            outs.iterator.map(dest => (dest, src, share))
          }
        }
      )

    if (debug) {
      logger.debug("==== Contributions détaillées (chaque source distribue son rang) ====")
      contributionsDetailed.collect().foreach { case (dest, src, contrib) =>
        logger.debug(f"$dest%-5s reçoit $contrib%.6f de $src")
      }
    }

    // === (2) Agrégation : chaque page "dest" reçoit la somme des contributions ===
    val receivedRanks: RDD[(String, Double)] =
      contributionsDetailed
        .map { case (dest, _, contrib) => (dest, contrib) }
        .reduceByKey(partitioner.getOrElse(v.partitioner.orNull), _ + _)

    if (debug) {
      logger.debug("==== Rangs reçus (somme des contributions entrantes) ====")
      receivedRanks.collect().foreach { case (node, rank) =>
        logger.debug(f"$node%-5s : $rank%.6f")
      }
    }

    // === (3) Réintégration des pages sans contribution ===
    val nextRanks = receivedRanks
      .union(allNodes.map(n => (n, 0.0)))
      .reduceByKey(partitioner.getOrElse(v.partitioner.orNull), _ + _)

    nextRanks
  }

  /**
    * Calcul complet du PageRank sur N itérations (version optimisée)
    * avec cache et persistance mémoire/disque.
    */
  def computePageRank(
      links: RDD[(String, Seq[String])],
      iterations: Int,
      debug: Boolean = false,
      plot: Boolean = false,
      logger: Logger,
      outputDir: Option[String] = None
  )(implicit spark: SparkSession): RDD[(String, Double)] = {

    val sc = spark.sparkContext

    // Adaptive partitioning based on input data size (avoids expensive distinct().count())
    val inputPartitions = links.getNumPartitions
    val useCustomPartitioning = inputPartitions <= 50  // Only use custom partitioning for small/medium graphs

    val numPartitions = {
      if (inputPartitions <= 2) sc.defaultParallelism           // Tiny: use default
      else if (inputPartitions <= 10) sc.defaultParallelism * 2 // Small: 2x
      else if (inputPartitions <= 50) sc.defaultParallelism * 4 // Medium: 4x
      else inputPartitions                                       // Large: keep input partitioning
    }

    logger.info(f"Using $numPartitions partitions (input had $inputPartitions partitions, custom=${useCustomPartitioning})")
    val partitioner = new HashPartitioner(numPartitions)

    // === (1) Préparation du graphe complet ===
    val srcNodes = links.keys
    val dstNodes = links.values.flatMap(identity)
    val allNodes = srcNodes.union(dstNodes).distinct().cache()
    val N = allNodes.count().toDouble

    val linksFull: RDD[(String, Seq[String])] = {
      val emptyByNode = allNodes.map(n => (n, Seq.empty[String]))
      val joined = emptyByNode
        .leftOuterJoin(links)
        .mapValues { case (_, outs) => outs.getOrElse(Seq.empty[String]) }

      // Only repartition for small/medium graphs; large graphs keep natural partitioning
      if (useCustomPartitioning) joined.partitionBy(partitioner).cache()
      else joined.cache()
    }

    // === (2) Initialisation du vecteur de rangs ===
    // Adaptive persistence: MEMORY_ONLY for small/medium, cache for large
    val storageLevel = if (useCustomPartitioning) StorageLevel.MEMORY_ONLY else StorageLevel.MEMORY_ONLY

    var v: RDD[(String, Double)] = {
      val initial = allNodes.map(n => (n, 1.0 / N))
      // Only repartition for small/medium graphs
      if (useCustomPartitioning) initial.partitionBy(partitioner).persist(storageLevel)
      else initial.persist(storageLevel)
    }

    // Historique facultatif (pour le plot)
    val history =
      if (plot) {
        val buf = scala.collection.mutable.ArrayBuffer.empty[Map[String, Double]]
        PageRankUtils.appendSnapshot(Some(buf), v)
        Some(buf)
      } else None

    // === (3) Boucle principale : v_{i+1} = M × v_i ===
    for (i <- 1 to iterations) {
      // For large graphs, don't pass partitioner to avoid unnecessary shuffles
      val partOpt = if (useCustomPartitioning) Some(partitioner) else None
      val newV = oneStep(v, linksFull, allNodes, debug, logger, partOpt)
        .persist(storageLevel)

      v.unpersist()
      v = newV

      if (debug) {
        val iteration_str = if (i > 1) "itérations" else "itération"
        logger.debug(s"==== Nouveau vecteur après $i $iteration_str ====")
        v.collect().foreach { case (node, rank) =>
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
}