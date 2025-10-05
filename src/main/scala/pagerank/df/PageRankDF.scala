package pagerank.df

import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.functions._

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

  /** Calcul complet du PageRank sur N itérations */
  def computePageRank(
      links: DataFrame,
      allNodes: Seq[String],
      iterations: Int,
      debug: Boolean = false,
      logger: Logger,
      plot: Boolean = false,
      outputPath: Option[String] = None
  )(implicit spark: SparkSession): DataFrame = {

    import spark.implicits._

    val N = allNodes.length.toDouble
    var v = allNodes.map(n => (n, 1.0 / N)).toSeq.toDF("page", "rank")

    // Si on est en mode "plot", conservation de l'historique
    val history =
      if (plot) scala.collection.mutable.ArrayBuffer(v.collect().map(r => r.getString(0) -> r.getDouble(1)).toMap)
      else null

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

      if (plot) {
        history.append(v.collect().map(r => r.getString(0) -> r.getDouble(1)).toMap)
      }
    }

    // === Export CSV si demandé ===
    if (plot && outputPath.nonEmpty) {
      val dir = new java.io.File(outputPath.get)
      if (!dir.exists()) dir.mkdirs()
      val outFile = new java.io.PrintWriter(s"${outputPath.get}/history.csv")
      try {
        outFile.println("Iteration," + allNodes.mkString(","))
        for ((snapshot, i) <- history.zipWithIndex) {
          val line = allNodes.map(n => snapshot.getOrElse(n, 0.0))
          outFile.println(s"$i," + line.mkString(","))
        }
      } finally {
        outFile.close()
      }
      logger.info(s"==== Historique PageRank sauvegardé dans ${outputPath.get}/history.csv ====")
    }

    v
  }

}