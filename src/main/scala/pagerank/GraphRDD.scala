package pagerank

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.apache.log4j.Logger

/**
  * Représentation RDD du graphe (lazy + scalable + debug détaillé)
  *
  * Étapes :
  *  - Lecture lazy (aucun chargement direct)
  *  - Nettoyage complet (self-loops, doublons, liens rouges)
  *  - Debug progressif : logs intermédiaires avec comptage partiel
  */
class GraphRDD(
    name: String,
    path: String,
    debug: Boolean = false,
    sepRegex: String = "\\|",
    storage: StorageLevel = StorageLevel.MEMORY_AND_DISK_SER
)(implicit spark: SparkSession, @transient logger: Logger)
  extends BaseGraph(name, path, debug)(spark, logger) {

  // === Chargement paresseux du fichier ===
  private lazy val raw: RDD[String] =
    spark.sparkContext.textFile(path).map(_.trim).filter(_.nonEmpty)

  /**
    * Structure principale du graphe :
    * (page source, Seq[pages de destination])
    */
  lazy val links: RDD[(String, Seq[String])] = {
    val sc = spark.sparkContext
    val sepRegex_ = sepRegex
    val storage_ = storage
    val debug_ = debug
    val logger_ = logger

    if (debug_) {
      logger_.info(s"[GraphRDD:$name] Début du parsing RDD depuis : $path")
    }

    // (1) === Lecture + parsing brut ===
    val pass1: RDD[(String, Array[String])] = raw.flatMap { line =>
      val toks = line.split(sepRegex_, -1).iterator.map(_.trim).filter(_.nonEmpty).toArray
      if (toks.isEmpty) None
      else {
        val src  = toks(0)
        val outs =
          if (toks.length > 1) java.util.Arrays.copyOfRange(toks, 1, toks.length)
          else Array.empty[String]
        Some((src, outs))
      }
    }.persist(storage_)

    if (debug_) {
      val count = pass1.count()
      logger_.info(s"[GraphRDD:$name] Étape 1 : Parsing brut terminé ($count pages sources)")
    }

    // (2) === Extraction des pages valides ===
    val validPagesRDD = pass1.keys.distinct().persist(storage_)
    if (debug_) {
      val nValid = validPagesRDD.count()
      logger_.info(s"[GraphRDD:$name] Étape 2 : Pages valides détectées : $nValid")
    }

    // (3) === Construction des arêtes (dest, src) ===
    val destToSrc = pass1.flatMap { case (src, outs) =>
      outs.iterator
        .filter(_ != null)
        .map(_.trim)
        .filter(_.nonEmpty)
        .filter(_ != src)
        .map(dest => (dest, src))
    }
    if (debug_) {
      val nEdgesRaw = destToSrc.count()
      logger_.info(s"[GraphRDD:$name] Étape 3 : Arêtes brutes générées : $nEdgesRaw")
    }

    // (4) === Retrait des liens rouges via jointure ===
    // Un lien rouge (red link) est un lien hypertexte pointant vers une page
    // qui n'existe pas dans le graphe.
    val validDestToSrc = destToSrc
      .join(validPagesRDD.map(v => (v, ())))
      .map { case (dest, (src, _)) => (src, dest) }
      .persist(storage_)

    if (debug_) {
      val nEdgesClean = validDestToSrc.count()
      logger_.info(s"[GraphRDD:$name] Étape 4 : Liens rouges supprimés ($nEdgesClean arêtes valides)")
    }

    // (5) === Agrégation par page source ===
    val aggregated: RDD[(String, Set[String])] =
      validDestToSrc.aggregateByKey(Set.empty[String])(
        (acc, dest) => acc + dest,
        (a, b) => a ++ b
      ).persist(storage_)

    if (debug_) {
      val nAgg = aggregated.count()
      logger_.info(s"[GraphRDD:$name] Étape 5 : Agrégation terminée ($nAgg nœuds avec liens sortants)")
    }

    // (6) === Conversion finale ===
    val parsed: RDD[(String, Seq[String])] =
      aggregated.mapValues(_.toSeq).persist(storage_)

    if (debug_) {
      logger_.info(s"[GraphRDD:$name] Étape 6 : Graphe final prêt ✅")
      logger_.info(s"==== Aperçu du graphe RDD '$name' ====")
      parsed.take(10).foreach { case (src, outs) =>
        logger_.info(s"$src -> [${outs.mkString(", ")}]")
      }
    }

    parsed
  }

  // === Métriques ===

  /** Nombre total de nœuds (sources + destinations uniques) */
  lazy val nNodes: Long = {
    val src = links.keys
    val dst = links.values.flatMap(identity)
    val total = src.union(dst).distinct().count()
    if (debug) logger.info(s"[GraphRDD:$name] Nombre total de nœuds = $total")
    total
  }

  /** Nombre total d'arêtes */
  lazy val nEdges: Long = {
    val total = links.map { case (_, outs) => outs.size.toLong }.sum().toLong
    if (debug) logger.info(s"[GraphRDD:$name] Nombre total d'arêtes = $total")
    total
  }

  /** Affichage d'un résumé du graphe */
  override def describe(): Unit = {
    logger.info(s"===== Graphe RDD: $name =====")
    logger.info(s"Fichier source : $fileName")
    logger.info(f"Nombre de nœuds : $nNodes%,d | Nombre d'arêtes : $nEdges%,d")
    if (debug) links.take(5).foreach(println)
    logger.info("=============================")
  }
}