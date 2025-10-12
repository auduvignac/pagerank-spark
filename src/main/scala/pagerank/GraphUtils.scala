package pagerank

import org.apache.spark.sql.{Dataset, SparkSession, DataFrame}
import org.apache.spark.rdd.RDD
import org.apache.log4j.Logger

object GraphUtils {

  // Initialise un SparkSession partagé
  def initSpark(appName: String): SparkSession = {
    SparkSession.builder()
      .appName(appName)
      .master("local[*]")
      .getOrCreate()
  }

  // Lecture générique en RDD
  def readAsRDD(path: String)(implicit spark: SparkSession): RDD[String] = {
    spark.sparkContext.textFile(path)
  }

  // Lecture générique en Dataset[String]
  def readAsDataset(path: String)(implicit spark: SparkSession): Dataset[String] = {
    spark.read.textFile(path)
  }

  // Parser version RDD
  def parseGraphRDD(
      lines: RDD[String],
      debug: Boolean = false,
      logger: Logger = Logger.getLogger(getClass.getName),
      input: Option[String] = None
  ): RDD[(String, Seq[String])] = {
    val links = lines.map { line =>
      val parts = line.split("\\|").map(_.trim)
      val page = parts.head
      val outlinks = if (parts.length > 1) parts.tail.filter(_.nonEmpty) else Array.empty[String]
      (page, outlinks.toSeq)
    }

    if (debug) {
      val src = input.getOrElse("RDD fourni en mémoire")
      logger.debug(s"==== Graphe $src chargé ====")
      links.collect().foreach { case (p, outs) =>
        logger.debug(s"$p -> [${outs.mkString(", ")}]")
      }
    }

    links
  }

  // Count total unique nodes (pages) in RDD format
  def countNodesRDD(links: RDD[(String, Seq[String])]): Long = {
    val srcNodes = links.keys
    val dstNodes = links.values.flatMap(identity)
    srcNodes.union(dstNodes).distinct().count()
  }

  // Count total edges (individual outlinks) in RDD format
  def countEdgesRDD(links: RDD[(String, Seq[String])]): Long = {
    links.map { case (_, outlinks) => outlinks.size.toLong }.sum().toLong
  }

  // Count total unique nodes (pages) in DataFrame format
  def countNodesDF(links: DataFrame, allPages: Dataset[String])(implicit spark: SparkSession): Long = {
    import spark.implicits._
    allPages.union(links.select("outlink").as[String]).distinct().count()
  }

  // Count total edges (individual outlinks) in DataFrame format
  def countEdgesDF(links: DataFrame): Long = {
    links.count()
  }

  // Extract all unique page names from input lines (including pages without outlinks)
  def extractAllPagesDF(lines: Dataset[String])(implicit spark: SparkSession): Dataset[String] = {
    import spark.implicits._
    lines.map { line =>
      val parts = line.split("\\|").map(_.trim)
      parts.head // First column is always the page name
    }.distinct()
  }

  // Parser version Dataset[String] → DataFrame(page, outlink)
  def parseGraphDF(
      lines: Dataset[String],
      debug: Boolean = false,
      logger: Logger = Logger.getLogger(getClass.getName),
      input: Option[String] = None
  )(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._

    val df = lines.flatMap { line =>
      val parts = line.split("\\|").map(_.trim)
      val page = parts.head
      if (parts.length > 1) parts.tail.filter(_.nonEmpty).map(out => (page, out))
      else Seq.empty[(String, String)] // Pages without outlinks are not included
    }.toDF("page", "outlink")

    if (debug) {
      val src = input.getOrElse("Dataset fourni en mémoire")
      logger.debug(s"==== Graphe $src chargé ====")
      df.show(false)
    }

    df
  }

  def NumPartitions(nodeCount: Long)(implicit spark: SparkSession): Int = {
    val cores = spark.sparkContext.defaultParallelism

    // Heuristique de base selon la taille du graphe
    val base =
      if (nodeCount < 100000) cores * 2
      else if (nodeCount < 1000000) cores * 4
      else if (nodeCount < 10000000) cores * 8
      else math.min((nodeCount / 100000).toInt, 2048)

    // Bornes minimales et maximales de sécurité
    val rawPartitions = math.max(base, cores)

    val nextPow2 = Integer.highestOneBit(rawPartitions - 1) * 2

    nextPow2
  }


}