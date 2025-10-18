package pagerank

import org.scalatest.funsuite.AnyFunSuite
import org.apache.spark.rdd.RDD
import org.apache.log4j.Logger
import org.apache.spark.storage.StorageLevel
import utils.SparkTestSession

class MainRDDSpec extends AnyFunSuite with SparkTestSession {
  val testLogger: Logger = Logger.getLogger("TestLogger")
  lazy val sc = spark.sparkContext
  implicit val sparkSession = spark
  implicit val logger = testLogger

  val path = "data/sample_graph.txt"

  val graph = new GraphRDD(
    name = "Graph-RDD-sample_graph",
    path = path
  )

  test("oneStep keeps invariants on sample_graph.txt") {

    val N: Double = graph.nNodes.toDouble
    val allNodes: RDD[String] = graph.allNodes.persist(StorageLevel.MEMORY_ONLY)
    val links: RDD[(String, Seq[String])] = graph.links

    val outMap: RDD[(String, (Int, Seq[String]))] = links
      .reduceByKey(_ ++ _)
      .mapValues(_.distinct)
      .mapValues(outs => (outs.size, outs))

    val nodesWithOut: RDD[(String, (Int, Seq[String]))] = allNodes
      .map(nodeId => (nodeId, (0, Seq.empty[String])))
      .leftOuterJoin(outMap)
      .mapValues {
        case ((_, _), Some((deg, outs))) => (deg, outs)
        case _                           => (0, Seq.empty[String])
      }
      .persist(StorageLevel.MEMORY_ONLY)

    val ranks: RDD[(String, Double)] = allNodes
      .map(n => (n, 1.0 / N))
      .persist(StorageLevel.MEMORY_ONLY)

    // by default damping = 1.0 and debug = false
    val ranks_oneStep = PageRankRDD.oneStep(
      ranks = ranks,
      nodesWithOut = nodesWithOut,
      allNodes = allNodes,
      N = N,
      logger = testLogger
    )

    val sum = ranks_oneStep.map(_._2).sum()
    assert(math.abs(sum - 1.0) < 1e-6)

    val nodes = ranks_oneStep.keys.collect().toSet
    assert(nodes == Set("A", "B", "C", "D"))
  }

  test("computePageRank converges correctly on sample_graph.txt") {

    // by default damping = 1.0, debug = false, 
    // plot = false and outputDir = None
    val result = PageRankRDD.computePageRank(
      graph = graph,
      iterations = 10,
      logger = testLogger
    )

    val ranks = result.collect().toMap

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