package pagerank

import org.scalatest.funsuite.AnyFunSuite
import org.apache.log4j.Logger
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{SparkSession, DataFrame, Dataset, Row}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import utils.SparkTestSession

class MainDFSpec extends AnyFunSuite with SparkTestSession {
  val testLogger: Logger = Logger.getLogger("TestLogger")
  lazy val sc = spark.sparkContext
  implicit val sparkSession = spark
  implicit val logger = testLogger

  val path = "data/sample_graph.txt"

  val graph = new GraphDF(
    name = "Graph-DF-sample_graph",
    path = path
  )

  test("oneStep keeps invariants on sample_graph.txt") {

    val N: Double = graph.nNodes.toDouble
    val allNodes: Dataset[Row] = graph.allNodes
    val links: DataFrame = graph.links

    val ranks = allNodes
      .withColumnRenamed("node", "page")
      .withColumn("rank", lit(1.0 / N))

    // by default damping = 1.0 and debug = false
    val ranks_oneStep = PageRankDF.oneStep(
      ranks = ranks,
      links = links,
      allNodes = allNodes,
      N = N,
      logger = testLogger
    )

    val collected = ranks_oneStep.collect().map(r => r.getString(0) -> r.getDouble(1)).toMap

    // Check if the ranks' sum ≈ 1
    val sum = collected.values.sum
    assert(math.abs(sum - 1.0) < 1e-6)

    // Check if all nodes still there
    val nodes = collected.keySet
    assert(nodes == Set("A", "B", "C", "D"))
  }

  test("computePageRank converges correctly on sample_graph.txt") {

    // by default damping = 1.0, debug = false, 
    // plot = false and outputDir = None
    val result = PageRankDF.computePageRank(
      graph = graph,
      iterations = 10,
      logger = testLogger
    )

    val ranks = result.collect().map(r => r.getString(0) -> r.getDouble(1)).toMap

    val expected = Map(
      "A" -> 0.33333333,
      "B" -> 0.22222222,
      "C" -> 0.22222222,
      "D" -> 0.22222222
    )

    val sum = ranks.values.sum
    assert(math.abs(sum - 1.0) < 1e-6)

    expected.foreach { case (node, expRank) =>
      assert(
        math.abs(ranks(node) - expRank) < 1e-3,
        s"Rank for $node was ${ranks(node)} but expected $expRank"
      )
    }
  }

}