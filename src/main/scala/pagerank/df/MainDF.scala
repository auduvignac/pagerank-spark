package pagerank.df

import java.io.File
import java.io.PrintWriter
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import pagerank.GraphUtils

object MainDF {
  val logger: Logger = Logger.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {
    logger.info("==== [Début d'exécution] PageRank DF ====")

    if (args.length < 2) {
      System.err.println("Usage: MainDF <input> <output> [iterations] [--plot] [--debug]")
      System.exit(1)
    }

    val input      = args(0)
    val output     = args(1)
    val iterations = if (args.length > 2 && !args(2).startsWith("--")) args(2).toInt else 10
    val plot       = args.contains("--plot")
    val debug      = args.contains("--debug")

    implicit val spark: SparkSession = GraphUtils.initSpark("PageRank DF")
    import spark.implicits._

    val lines = GraphUtils.readAsDataset(input)
    val links = GraphUtils.parseGraphDF(
      lines,
      debug,
      logger,
      Some(input)
    )

    val allNodes = links.select("page")
      .union(links.select("outlink"))
      .distinct()
      .as[String]
      .collect()
      .sorted

    // === Mode normal ===
    if (!plot) {
      val v = PageRankDF.computePageRank(links, allNodes, iterations, debug, logger)

      if (debug) {
        val iterWord = if (iterations > 1) "itérations" else "itération"
        logger.debug(s"==== Vecteur obtenu après $iterations $iterWord à partir du fichier : $input ====")
        v.orderBy(desc("rank")).show(false)
      }
    }
    // === Mode plot ===
    else {
      val N = allNodes.length.toDouble
      var v = allNodes.map(n => (n, 1.0 / N)).toSeq.toDF("page", "rank")

      val history = scala.collection.mutable.ArrayBuffer.empty[Map[String, Double]]
      history.append(v.collect().map(r => r.getString(0) -> r.getDouble(1)).toMap)

      for (i <- 1 to iterations) {
        v = PageRankDF.oneStep(v, links, allNodes, debug, logger)
        history.append(v.collect().map(r => r.getString(0) -> r.getDouble(1)).toMap)
      }

      val outDir = new File(output)
      if (!outDir.exists()) outDir.mkdirs()

      val outFile = new PrintWriter(s"$output/history.csv")
      try {
        outFile.println("Iteration," + allNodes.mkString(","))
        for ((snapshot, i) <- history.zipWithIndex) {
          val line = allNodes.map(n => snapshot.getOrElse(n, 0.0))
          outFile.println(s"$i," + line.mkString(","))
        }
      } finally outFile.close()

      if (debug) logger.debug(s"==== Historique écrit dans $output/history.csv ====")
    }

    logger.info("==== [Fin d'exécution] PageRank DF ====")
    spark.stop()
  }
}