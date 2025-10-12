package pagerank

import org.apache.spark.sql.{SparkSession, Dataset, DataFrame}
import org.apache.log4j.Logger

/**
  * Représentation DataFrame du graphe (lazy)
  */
class GraphDF(
    name: String,
    path: String,
    debug: Boolean = false
)(implicit spark: SparkSession, logger: Logger)
  extends BaseGraph(name, path, debug)(spark, logger) {

  import spark.implicits._

  private lazy val lines: Dataset[String] = spark.read.textFile(path)

  /** DataFrame (page, outlink) */
  lazy val links: DataFrame = lines.flatMap { line =>
    val parts = line.split("\\|").map(_.trim)
    val page = parts.head
    if (parts.length > 1) parts.tail.filter(_.nonEmpty).map(out => (page, out))
    else Seq.empty[(String, String)]
  }.toDF("page", "outlink")

  /** Liste de toutes les pages (y compris isolées) */
  lazy val allPages: Dataset[String] = lines.map { line =>
    val parts = line.split("\\|").map(_.trim)
    parts.head
  }.distinct()

  def nodeCount(): Long =
    allPages.union(links.select("outlink").as[String]).distinct().count()

  def edgeCount(): Long = links.count()

  override def describe(): Unit = {
    logger.info(s"===== Graphe DF: $name =====")
    logger.info(s"Fichier source : $fileName")
    logger.info(s"Lazy DataFrame prêt (chargement à la demande)")
    if (debug) links.show(5, truncate = false)
    logger.info("=============================")
  }
}