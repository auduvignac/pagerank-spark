package pagerank.df

import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.functions._

import pagerank.PageRankUtils

object PageRankDF {

  /** Une itération de l’algorithme PageRank */
  def oneStep(
      v: DataFrame,
      links: DataFrame,
      allNodes: Seq[String],
      debug: Boolean = false,
      logger: Logger
  )(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._

    val outDegrees = links.groupBy($"page")
        .agg(count($"outlink").as("degree"))

    // Contributions = rank / degree
    val contributions = links
      .join(v, Seq("page"))
      .join(outDegrees, Seq("page"))
      .withColumn("contrib", $"rank" / $"degree")
      .select($"outlink".as("dest"), $"page".as("src"), $"contrib")

    if (debug) {
      logger.debug("==== Contributions (chaque source distribue son rang) ====")
      contributions.collect().foreach { row =>
        val dest    = row.getAs[String]("dest")
        val src     = row.getAs[String]("src")
        val contrib = row.getAs[Double]("contrib")
        logger.debug(f"$src%-5s -> $dest%-5s : $contrib%.6f")
      }
    }

    val received = contributions
      .groupBy("dest")
      .agg(sum("contrib").as("rank"))
      .withColumnRenamed("dest", "page")

    if (debug) {
      logger.debug("==== Rangs reçus (somme des contributions) ====")
      received.collect().foreach { row =>
        val node = row.getAs[String]("page")
        val rank = row.getAs[Double]("rank")
        logger.debug(f"$node%-5s : $rank%.6f")
      }
    }

    val allNodesDF = allNodes.toDF("page")

    val result = allNodesDF
      .join(received, Seq("page"), "left_outer")
      .na.fill(0.0, Seq("rank"))

    result
  }

  // Calcul complet du PageRank sur N itérations (avec option d'historique)
  def computePageRank(
      links: DataFrame,
      iterations: Int,
      debug: Boolean = false,
      plot: Boolean = false,
      logger: Logger,
      outputDir: Option[String] = None
  )(implicit spark: SparkSession): DataFrame = {

    import spark.implicits._

    val allNodes = links.select("page")
          .union(links.select("outlink"))
          .distinct()
          .as[String]
          .collect()
          .sorted

    val N = allNodes.length.toDouble
    var v = allNodes.map(n => (n, 1.0 / N)).toSeq.toDF("page", "rank")

    // Si mode "plot" activé, conservation de l'historique
    val history =
      if (plot) {
        val buf = scala.collection.mutable.ArrayBuffer.empty[Map[String, Double]]
        PageRankUtils.appendSnapshot(Some(buf), v)
        Some(buf)
      } else None


    for (i <- 1 to iterations) {
      v = oneStep(v, links, allNodes, debug, logger)

      if (debug) {
        val iterWord = if (i > 1) "itérations" else "itération"
        logger.debug(s"==== Nouveau vecteur après $i $iterWord ====")
        v.collect().foreach { row =>
          val node = row.getAs[String]("page")
          val rank = row.getAs[Double]("rank")
          logger.debug(f"$node%-5s : $rank%.6f")
        }
      }

      if (plot)
        PageRankUtils.appendSnapshot(history, v)
    }

    // === Export CSV si demandé ===
    if (plot && outputDir.isDefined) {
      PageRankUtils.exportHistoryToCSV(history.get.toSeq, outputDir.get, logger)
    }

    v
  }

}