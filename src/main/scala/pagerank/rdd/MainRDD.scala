package pagerank.rdd

import java.io.File
import java.io.PrintWriter
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD
import pagerank.GraphUtils

object MainRDD {
  val logger: Logger = Logger.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {
    logger.info("==== [Début d'exécution] PageRank RDD ====")

    if (args.length < 2) {
      System.err.println("Usage: MainRDD <input> <output> [iterations] [--plot] [--debug]")
      System.exit(1)
    }

    val input      = args(0)
    val output     = args(1)
    val iterations = if (args.length > 2 && !args(2).startsWith("--")) args(2).toInt else 10
    val plot       = args.contains("--plot")
    val debug      = args.contains("--debug")

    implicit val spark: SparkSession = GraphUtils.initSpark("PageRank RDD")
    val sc = spark.sparkContext

    val lines: RDD[String] = GraphUtils.readAsRDD(input)
    val links: RDD[(String, Seq[String])] = GraphUtils.parseGraphRDD(
      lines,
      debug,
      logger,
      Some(input)
    )

    val v = PageRankRDD.computePageRank(
      links = links,
      iterations = iterations,
      debug = debug,
      plot = plot,
      outputDir = Some(output),
      logger = logger
    )


    logger.info("==== [Fin d'exécution] PageRank RDD ====")
    spark.stop()
  }
}