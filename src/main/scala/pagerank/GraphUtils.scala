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
      else Seq.empty[(String, String)]
    }.toDF("page", "outlink")

    if (debug) {
      val src = input.getOrElse("Dataset fourni en mémoire")
      logger.debug(s"==== Graphe $src chargé ====")
      df.show(false)
    }

    df
  }
}