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
      v: RDD[(String, Double)],                                // RDD représentant le rang actuel de chaque page : (page, rank)
      links: RDD[(String, Seq[String])],                       // RDD représentant la structure du graphe : (page source, liste des pages de destination)
      allNodes: RDD[String],                                   // RDD contenant la liste de toutes les pages (utile pour réintégrer celles sans contribution)
      N: Double,                                               // Nombre total de noeuds
      damping: Double = 1.0,                                   // Valeur du facteur d'amortissement (par défaut 1.0 : pas d'amortissement)
      debug: Boolean = false,                                  // Active ou non les logs détaillés
      logger: Logger,                                          // Logger pour afficher les informations de débogage
      partitioner: Option[org.apache.spark.Partitioner] = None // Activation du partitionnement
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
    val receivedRanks: RDD[(String, Double)] = {
      val contributions = contributionsDetailed.map { case (dest, _, contrib) => (dest, contrib) }
      partitioner match {
        case Some(part) => contributions.reduceByKey(part, _ + _)
        case None => contributions.reduceByKey(_ + _)  // Use default partitioning
      }
    }

    if (debug) {
      logger.debug("==== Rangs reçus (somme des contributions entrantes) ====")
      receivedRanks.collect().foreach { case (node, rank) =>
        logger.debug(f"$node%-5s : $rank%.6f")
      }
    }

    // === (3) Réintégration des pages sans contribution ===
    val allRanks = {
      val combined = receivedRanks.union(allNodes.map(n => (n, 0.0)))
      partitioner match {
        case Some(part) => combined.reduceByKey(part, _ + _)
        case None => combined.reduceByKey(_ + _)  // Use default partitioning
      }
    }

    // === (4) Application du facteur d’amortissement ===
    val nextRanks = allRanks.mapValues(rank => (1 - damping) / N + damping * rank)

    // === (5) Retour ===
    nextRanks
  }

  /**
    * Calcul complet du PageRank sur N itérations (version optimisée)
    * avec cache et persistance mémoire/disque.
    */
  def computePageRank(
      links: RDD[(String, Seq[String])],
      iterations: Int,
      damping: Double = 1.0,                 // Valeur du facteur d'amortissement (par défaut 1.0 : pas d'amortissement)
      debug: Boolean = false,
      plot: Boolean = false,
      logger: Logger,
      outputDir: Option[String] = None
  )(implicit spark: SparkSession): RDD[(String, Double)] = {

    val sc = spark.sparkContext

    // Adaptive partitioning based on graph characteristics
    // Strategy: RDD_OPT should dominate on MEDIUM graphs (20K-1M nodes)
    val inputPartitions = links.getNumPartitions

    val edgeCount = Some(GraphUtils.countEdgesRDD(links))

    // Estimate graph size category from edge count (avoiding expensive count operation)
    val estimatedNodes = edgeCount.getOrElse(inputPartitions * 5000L)  // Fallback: ~5K nodes per partition
    val graphCategory = {
      if (estimatedNodes < 100000) "SMALL"         // < 100K edges (~10K nodes): RDD wins
      else if (estimatedNodes < 5000000) "MEDIUM"  // 100K-5M edges (~10K-500K nodes): RDD_OPT wins
      else "LARGE"                                  // > 5M edges (>500K nodes): DF wins
    }

    // RDD_OPT optimizations: Aggressive ONLY for medium graphs
    val useCustomPartitioning = graphCategory == "MEDIUM"

    // Scale partitions proportionally to graph size
    val numPartitions = graphCategory match {
      case "SMALL"  => Math.max(inputPartitions, sc.defaultParallelism)
      case "MEDIUM" => Math.max(inputPartitions * 2, sc.defaultParallelism * 4)  // 2x input or 4x default
      case "LARGE"  => Math.max(inputPartitions * 2, sc.defaultParallelism * 8)  // 2x input or 8x default
    }

    logger.info(f"Catégorie du graph: $graphCategory, utilisation de $numPartitions partitions (input: $inputPartitions)")
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
    // Adaptive persistence strategy
    val storageLevel = graphCategory match {
      case "SMALL"  => StorageLevel.NONE           // Don't cache, too small
      case "MEDIUM" => StorageLevel.MEMORY_ONLY    // Aggressive caching for medium
      case "LARGE"  => StorageLevel.MEMORY_ONLY    // Cache, but no repartitioning
    }

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
      val newV = oneStep(v, linksFull, allNodes, N, damping, debug, logger, partOpt)
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