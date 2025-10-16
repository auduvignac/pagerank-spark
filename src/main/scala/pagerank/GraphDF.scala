package pagerank

import org.apache.spark.sql.{SparkSession, Dataset, DataFrame, Row}
import org.apache.log4j.Logger
import org.apache.spark.sql.functions._

/**
  * Représentation DataFrame du graphe (lazy)
  * Alignée avec GraphRDD.
  */
class GraphDF(
    name: String,
    path: String,
    debug: Boolean = false
)(implicit spark: SparkSession, logger: Logger)
  extends BaseGraph(name, path, debug)(spark, logger) {

  import spark.implicits._

  private lazy val lines: Dataset[String] = spark.read.textFile(path)

  /** Structure du graphe : (page source, page destination) */
  lazy val links: DataFrame = {
    val parsed = lines.flatMap { line =>
      val parts = line.split("\\|").map(_.trim)
      val page = parts.headOption.getOrElse("")
      val outs =
        if (parts.length > 1) parts.tail.filter(_.nonEmpty)
        else Array.empty[String]

      if (page.isEmpty) Seq.empty[(String, String)]
      else if (outs.nonEmpty) outs.map(out => (page, out))
      else Seq((page, null)) // ⚠️ garder la page même sans outlinks
    }.toDF("page", "outlink")
     .filter($"page" =!= "") // ignorer seulement les pages vides

    parsed
  }

  /** Ensemble de tous les nœuds (sources + destinations) */
  lazy val allNodes: Dataset[Row] = {
    links
      .select($"page".as("node"))
      .union(links.select($"outlink".as("node")))
      .filter($"node".isNotNull && length(trim($"node")) > 0)
      .distinct()
  }

  /** Nombre total de nœuds */
  lazy val nNodes: Long = allNodes.count()

  /** Nombre total d'arêtes */
  lazy val nEdges: Long = links.filter($"outlink".isNotNull).count()

  override def describe(): Unit = {
    logger.info(s"===== Graphe DF: $name =====")
    logger.info(s"Fichier source : $fileName")
    logger.info(s"Nombre de nœuds : $nNodes")
    logger.info(s"Nombre d'arêtes : $nEdges")
    if (debug) links.show(5, truncate = false)
    logger.info("=============================")
  }
}
