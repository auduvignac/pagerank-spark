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

    val v = PageRankDF.computePageRank(
          links = links,
          iterations = iterations,
          debug = debug,
          logger = logger,
          plot = plot,
          outputDir = Some(output),
        )

    logger.info("==== [Fin d'exécution] PageRank DF ====")
    spark.stop()
  }
}