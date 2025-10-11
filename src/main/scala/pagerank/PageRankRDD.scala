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
        v: RDD[(String, Double)],              // RDD représentant le rang actuel de chaque page : (page, rank)
        links: RDD[(String, Seq[String])],     // RDD représentant la structure du graphe : (page source, liste des pages de destination)
        allNodes: RDD[String],                 // RDD contenant la liste de toutes les pages (utile pour réintégrer celles sans contribution)
        N: Double,                             // Nombre total de noeuds
        damping: Double = 1.0,                 // Valeur du facteur d'amortissement (par défaut 1.0 : pas d'amortissement)
        debug: Boolean = false,                // Active ou non les logs détaillés
        logger: Logger                         // Logger pour afficher les informations de débogage
    ): RDD[(String, Double)] = {               // Retourne un nouveau RDD (page, nouveauRank)

    // === (1) Distribution : chaque page "src" distribue son rang à ses destinations ===
    val contributionsDetailed: RDD[(String, String, Double)] = links
      .join(v) // associe chaque page source à son rang courant : (src, (outlinks, rankSrc))
      .flatMap { case (src, (outs, rankSrc)) =>
        if (outs.isEmpty)
          Seq.empty[(String, String, Double)] // si la page n’a pas de liens sortants, elle ne distribue rien
        else {
          val share = rankSrc / outs.size      // chaque lien sortant reçoit une part égale du rang de la page source
          outs.map(dest => (dest, src, share)) // pour chaque destination, on émet (destination, source, contribution)
        }
      }

    // (Optionnel) Affichage des contributions détaillées en mode debug
    if (debug) {
      logger.debug("==== Contributions détaillées (chaque source distribue son rang) ====")
      contributionsDetailed.collect().foreach { case (dest, src, contrib) =>
        logger.debug(f"$dest%-5s reçoit $contrib%.6f de $src")
      }
    }

    // === (2) Agrégation : chaque page "dest" reçoit la somme des contributions ===
    val receivedRanks: RDD[(String, Double)] =
      contributionsDetailed
        .map { case (dest, _, contrib) => (dest, contrib) }  // on garde (destination, contribution)
        .reduceByKey(_ + _)                                  // on additionne toutes les contributions reçues par une même page

    // (Optionnel) Affichage des rangs reçus avant correction
    if (debug) {
      logger.debug("==== Rangs reçus (somme des contributions entrantes) ====")
      receivedRanks.collect().foreach { case (node, rank) =>
        logger.debug(f"$node%-5s : $rank%.6f")
      }
    }

    // === (3) Réintégration des pages sans contribution ===
    // Certaines pages n'apparaissent pas dans receivedRanks (aucune contribution entrante)
    // On les ajoute avec un rang nul afin de conserver tous les nœuds du graphe
    val allRanks = receivedRanks
      .union(allNodes.map(n => (n, 0.0)))  // ajoute (page, 0.0) pour les pages absentes
      .reduceByKey(_ + _)                  // fusionne les doublons éventuels (somme inchangée)

    // === (4) Application du facteur d’amortissement ===
    val nextRanks = allRanks.mapValues(rank => (1 - damping) / N + damping * rank)

    // === (5) Retour ===
    nextRanks
  }


  // Calcul complet du PageRank sur N itérations (avec option d'historique)
  def computePageRank(
      links: RDD[(String, Seq[String])],
      iterations: Int,
      damping: Double = 1.0,
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
      v = oneStep(v, linksFull, allNodes, N, damping, debug, logger)

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