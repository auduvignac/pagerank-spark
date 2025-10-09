package pagerank

import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object PageRankRDD {

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
        v: RDD[(String, Double)],
        links: RDD[(String, Seq[String])],
        allNodes: RDD[String],
        debug: Boolean = false,
        logger: Logger
    ): RDD[(String, Double)] = {

      // === (1) Distribution : chaque page "src" distribue son rang à ses destinations ===
      val contributionsDetailed: RDD[(String, String, Double)] = links
        .join(v) // (src, (outlinks, rankSrc))
        .flatMap { case (src, (outs, rankSrc)) =>
          if (outs.isEmpty)
            Seq.empty[(String, String, Double)] // pas de liens sortants
          else {
            val share = rankSrc / outs.size
            outs.map(dest => (dest, src, share)) // <-- conservation de la source
          }
        }

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
          .reduceByKey(_ + _)

      if (debug) {
        logger.debug("==== Rangs reçus (somme des contributions entrantes) ====")
        receivedRanks.collect().foreach { case (node, rank) =>
          logger.debug(f"$node%-5s : $rank%.6f")
        }
      }

      // === (3) Réintégration des pages sans contribution ===
      val nextRanks = receivedRanks
        .union(allNodes.map(n => (n, 0.0)))
        .reduceByKey(_ + _)

      nextRanks
    }


  // Calcul complet du PageRank sur N itérations (avec option d'historique)
  def computePageRank(
      links: RDD[(String, Seq[String])],
      iterations: Int,
      debug: Boolean = false,
      plot: Boolean = false,
      logger: Logger,
      outputDir: Option[String] = None
  )(implicit spark: SparkSession): RDD[(String, Double)] = {

    val sc = spark.sparkContext

    val srcNodes = links.keys
    val dstNodes = links.values.flatMap(identity)
    val allNodes = srcNodes.union(dstNodes).distinct().cache()
    val N = allNodes.count().toDouble

    val linksFull: RDD[(String, Seq[String])] = {
      val emptyByNode = allNodes.map(n => (n, Seq.empty[String]))
      emptyByNode.leftOuterJoin(links)
        .mapValues { case (_, maybeOuts) => maybeOuts.getOrElse(Seq.empty[String]) }
        .cache()
    }

    var v: RDD[(String, Double)] = allNodes.map(n => (n, 1.0 / N))

    val history =
      if (plot) {
        val buf = scala.collection.mutable.ArrayBuffer.empty[Map[String, Double]]
        PageRankUtils.appendSnapshot(Some(buf), v)
        Some(buf)
      } else None


    for (i <- 1 to iterations) {
      v = oneStep(v, linksFull, allNodes, debug, logger)

      if (debug) {
        val iteration_str = if (i > 1) "iterations" else "iteration"
        logger.debug(s"==== Nouveau vecteur après $i $iteration_str ====")
        v.collect().foreach { case (node, rank) =>
          logger.debug(f"$node%-5s : $rank%.6f")
        }
      }

      if (plot)
        PageRankUtils.appendSnapshot(history, v)
    }

    // Export CSV si demandé
    if (plot && outputDir.isDefined) {
      PageRankUtils.exportHistoryToCSV(history.get.toSeq, outputDir.get, logger)
    }

    v
  }

}