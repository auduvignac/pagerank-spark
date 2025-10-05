package pagerank.rddoptimized

import org.scalatest.funsuite.AnyFunSuite
import org.apache.spark.rdd.RDD
import org.apache.log4j.Logger
import utils.SparkTestSession
import pagerank.GraphUtils

class MainRDDOptimizedSpec extends AnyFunSuite with SparkTestSession {

  val testLogger: Logger = Logger.getLogger("TestLogger")
  lazy val sc = spark.sparkContext
  implicit val sparkSession = spark

  val path = "data/sample_graph.txt"

  // Lecture du fichier
  val lines: RDD[String] = GraphUtils.readAsRDD(path)

  // Parsing en RDD
  val links: RDD[(String, Seq[String])] = GraphUtils.parseGraphRDD(
    lines,
    false,
    testLogger,
    Some(path)
  )

  test("oneStep conserve les invariants sur sample_graph.txt") {

    val srcNodes = links.keys
    val dstNodes = links.values.flatMap(identity)
    val allNodes = srcNodes.union(dstNodes).distinct().cache()

    val N = allNodes.count().toDouble
    val v0 = allNodes.map(n => (n, 1.0 / N))

    val v1 = PageRankRDDOptimized.oneStep(v0, links, allNodes, debug = false, testLogger)

    val sum = v1.map(_._2).sum()
    assert(math.abs(sum - 1.0) < 1e-6)

    val nodes = v1.keys.collect().toSet
    assert(nodes == Set("A", "B", "C", "D"))
  }

  test("computePageRank converge correctement sur sample_graph.txt") {

    val result = PageRankRDDOptimized.computePageRank(
      links,
      iterations = 10,
      debug = false,
      logger = testLogger
    )

    val v = result.collect().toMap

    val expected = Map(
      "A" -> 0.33333333,
      "B" -> 0.22222222,
      "C" -> 0.22222222,
      "D" -> 0.22222222
    )

    val sum = v.values.sum
    assert(math.abs(sum - 1.0) < 1e-6)

    expected.foreach { case (node, expRank) =>
      assert(
        math.abs(v(node) - expRank) < 1e-3,
        s"Rank for $node was ${v(node)} but expected $expRank"
      )
    }
  }
}