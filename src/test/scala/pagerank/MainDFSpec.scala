package pagerank

import org.scalatest.funsuite.AnyFunSuite
import org.apache.log4j.Logger
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{SparkSession, DataFrame, Dataset, Row}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.storage.StorageLevel

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

    val p = sc.defaultParallelism * 2

    val N: Double = graph.nNodes.toDouble

    val edges = graph.links
      .filter(col("outlink").isNotNull)
      .select(col("page").as("src"), col("outlink").as("dest"))
      .distinct()
      .repartition(p, col("src"))
      .persist(StorageLevel.MEMORY_ONLY)

    val nodes = edges.select(col("src").as("id"))
      .union(edges.select(col("dest").as("id")))
      .distinct()
      .repartition(p, col("id"))
      .persist(StorageLevel.MEMORY_ONLY)

    val outdeg = edges
      .groupBy(col("src"))
      .agg(count(lit(1)).as("outdeg"))
      .withColumnRenamed("src", "id")
      .repartition(p, col("id"))
      .persist(StorageLevel.MEMORY_ONLY)

    val nodesWithDeg = nodes
      .join(outdeg, Seq("id"), "left")
      .na.fill(0, Seq("outdeg"))
      .persist(StorageLevel.MEMORY_ONLY)

    var ranks = nodes
      .withColumn("rank", lit(1.0 / N))
      .persist(StorageLevel.MEMORY_ONLY)

    ranks = ranks.localCheckpoint(eager = true)

    val danglingIds = nodesWithDeg
      .filter(col("outdeg") === 0)
      .select(col("id"))
      .repartition(p, col("id"))
      .persist(StorageLevel.MEMORY_ONLY)

    // by default damping = 1.0 and debug = false
    val ranks_oneStep = PageRankDF.oneStep(
      ranks = ranks,
      edges = edges,
      nodes = nodes,
      outdeg = outdeg,
      danglingIds = danglingIds,
      N = N,
      numParts = p,
      storage = StorageLevel.MEMORY_ONLY,
      logger = testLogger
    )

    val collected = ranks_oneStep.collect().map(r => r.getString(0) -> r.getDouble(1)).toMap

    // Check if the ranks' sum ≈ 1
    val sum = collected.values.sum
    assert(math.abs(sum - 1.0) < 1e-6)

    // Check if all nodes still there
    val nodes_check = collected.keySet
    assert(nodes_check == Set("A", "B", "C", "D"))
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