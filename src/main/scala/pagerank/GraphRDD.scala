package pagerank

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger

/**
  * Représentation RDD du graphe (lazy : rien n'est chargé à la construction)
  * Uniformisée avec GraphDF, séparateur configurable via regex.
  */
class GraphRDD(
    name: String,
    path: String,
    regex: String = "\\|",
    debug: Boolean = false
)(implicit
    spark: SparkSession,
    logger: Logger
) extends BaseGraph(name, path, debug)(spark, logger) {

  /** Lecture du fichier texte brut */
  private lazy val lines: RDD[String] = {
    val rdd = spark.sparkContext.textFile(path)
    if (debug) {
      val sample = rdd.take(5)
      logger.debug(s"[GraphRDD:$name] Exemples de lignes lues (${sample.length}) :")
      sample.zipWithIndex.foreach { case (line, i) =>
        logger.debug(f"  [$i%02d] $line")
      }
    }
    rdd
  }

  /** Structure du graphe : (page source, Seq[pages de destination]) */
  lazy val links: RDD[(String, Seq[String])] = {
  // capture de regex en variable locale purement sérialisable
  val localRegex = regex

    val parsed = lines.map { line =>
      val parts = line.split(localRegex).map(_.trim)
      val page = parts.headOption.getOrElse("")
      val outs =
        if (parts.length > 1) parts.tail.filter(_.nonEmpty)
        else Array.empty[String]
      (page, outs.toSeq)
    }

    // affichage en driver (hors exécuteurs)
    if (debug) {
      logger.debug(s"[GraphRDD:$name] Échantillon de lignes parsées :")
      parsed.take(5).foreach { case (page, outs) =>
        logger.debug(s"  $page -> ${outs.mkString("[", ", ", "]")}")
      }
    }

    parsed
  }

  /** Ensemble de tous les nœuds (sources + destinations) */
  lazy val allNodes: RDD[String] = {
    val src = links.keys
    val dst = links.values.flatMap(identity)
    val all = src.union(dst)
      .map(_.trim)                      // supprime les espaces inutiles
      .filter(_.nonEmpty)               // élimine les chaînes vides
      .distinct()
    all
  }

  /** Nombre total de nœuds */
  lazy val nNodes: Long = {
    val count = allNodes.count()
    count
  }

  /** Nombre total d'arêtes */
  lazy val nEdges: Long = {
    val edgesCount = links.map(_._2.size.toLong).sum().toLong
    edgesCount
  }

  /** Description globale du graphe */
  override def describe(): Unit = {
    logger.info(s"===== Graphe RDD: $name =====")
    logger.info(s"Fichier source : $fileName")
    logger.info(s"Nombre de nœuds : $nNodes")
    logger.info(s"Nombre d'arêtes : $nEdges")
    if (debug) {
      logger.debug(s"[GraphRDD:$name] Extrait du graphe (5 premiers liens) :")
      links.take(5).foreach { case (src, outs) =>
        logger.debug(s"  $src -> ${outs.mkString(", ")}")
      }
    }
    logger.info("=============================")
  }
}