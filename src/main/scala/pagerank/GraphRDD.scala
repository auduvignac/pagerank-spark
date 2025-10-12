package pagerank

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger

/**
  * Représentation RDD du graphe (lazy : rien n'est chargé à la construction)
  */
class GraphRDD(
    name: String,
    path: String,
    debug: Boolean = false
)(implicit spark: SparkSession, logger: Logger)
  extends BaseGraph(name, path, debug)(spark, logger) {

  private lazy val lines: RDD[String] = spark.sparkContext.textFile(path)

  /** Structure du graphe : (page, Seq[outlinks]) */
  lazy val links: RDD[(String, Seq[String])] = {
    lines.map { line =>
      val parts = line.split("\\|").map(_.trim)
      val page = parts.head
      val outs = if (parts.length > 1) parts.tail.filter(_.nonEmpty) else Array.empty[String]
      (page, outs.toSeq)
    }
  }

  /** Nombre de nœuds uniques (évalué à la demande) */
  def nodeCount(): Long = {
    val src = links.keys
    val dst = links.values.flatMap(identity)
    src.union(dst).distinct().count()
  }

  /** Nombre total d'arêtes */
  def edgeCount(): Long = links.map(_._2.size.toLong).sum().toLong

  override def describe(): Unit = {
    logger.info(s"===== Graphe RDD: $name =====")
    logger.info(s"Fichier source : $fileName")
    logger.info(s"Lazy RDD prêt (chargement à la demande)")
    if (debug) links.take(5).foreach(println)
    logger.info("=============================")
  }
}