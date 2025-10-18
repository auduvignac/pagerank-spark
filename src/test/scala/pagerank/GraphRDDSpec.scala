package pagerank

import org.scalatest.funsuite.AnyFunSuite
import org.apache.log4j.Logger
import utils.SparkTestSession
import java.io.File

class GraphRDDSpec extends AnyFunSuite with SparkTestSession {

  val testLogger: Logger = Logger.getLogger("TestLogger")
  lazy val sc = spark.sparkContext
  implicit val sparkSession = spark
  implicit val logger = testLogger

  // Expected values for each graph file (nodes, edges)
  val expectedGraphStats = Map(
    "sample_graph.txt" -> (expectedNodesSample, expectedEdgesSample),
    "wiki-chti.txt" -> (expectedNodesChti, expectedEdgesChti),
    "wiki-breton.txt" -> (expectedNodesBreton, expectedEdgesBreton),
    "wiki-basque.txt" -> (expectedNodesBasque, expectedEdgesBasque),
    "wiki-pt.txt" -> (expectedNodesPt, expectedEdgesPt),
    "wiki-fr.txt" -> (expectedNodesFr, expectedEdgesFr)
  )

  lazy val expectedNodesSample = 4L
  lazy val expectedEdgesSample = 8L
  lazy val expectedNodesChti = 15489L
  lazy val expectedEdgesChti = 40124L
  lazy val expectedNodesBreton = 379763L
  lazy val expectedEdgesBreton = 1408847L
  lazy val expectedNodesBasque = 761282L
  lazy val expectedEdgesBasque = 4616203L
  lazy val expectedNodesPt = 3765539L
  lazy val expectedEdgesPt = 31571024L  
  lazy val expectedNodesFr = 6740183L
  lazy val expectedEdgesFr = 80920486L

  // Get all .txt files in the data directory
  val dataDir = new File("data")
  val dataFiles = if (dataDir.exists() && dataDir.isDirectory) {
    dataDir.listFiles()
      .filter(_.isFile)
      .filter(_.getName.endsWith(".txt"))
      .map(_.getName)
      .sorted
  } else {
    Array.empty[String]
  }

  // Test that we can find data files
  test("data directory should contain text files") {
    assert(dataFiles.nonEmpty, "No .txt files found in data/ directory")
  }

  // Test each data file individually
  dataFiles.foreach { fileName =>
    test(s"GraphRDD should correctly parse $fileName") {
      val path = s"data/$fileName"

      val graph = new GraphRDD(
        name = s"Graph-RDD-${fileName.replace(".txt", "")}",
        path = path
      )

      // Verify the graph can be created and parsed
      assert(graph != null, s"Failed to create GraphRDD for $fileName")

      // Verify nodes and edges can be counted
      val nodeCount = graph.nNodes
      val edgeCount = graph.nEdges

      testLogger.info(s"[$fileName] Nodes: $nodeCount, Edges: $edgeCount")

      // Assertions: graphs should have at least 1 node and 0 edges
      assert(nodeCount > 0, s"$fileName should have at least 1 node, got $nodeCount")
      assert(edgeCount >= 0, s"$fileName should have non-negative edges, got $edgeCount")

      // If expected values are defined, check them
      expectedGraphStats.get(fileName).foreach { case (expectedNodes, expectedEdges) =>
        if (expectedNodes > 0) {
          assert(nodeCount == expectedNodes,
            s"$fileName: expected $expectedNodes nodes but got $nodeCount")
        }
        if (expectedEdges > 0) {
          assert(edgeCount == expectedEdges,
            s"$fileName: expected $expectedEdges edges but got $edgeCount")
        }
      }
    }
  }

  // Test specific sample_graph.txt structure
  test("sample_graph.txt should have expected structure") {
    val path = "data/sample_graph.txt"

    val graph = new GraphRDD(
      name = "Graph-RDD-sample_graph",
      path = path
    )

    // Verify node count
    assert(graph.nNodes == 4, s"sample_graph.txt should have 4 nodes, got ${graph.nNodes}")

    // Verify edge count
    assert(graph.nEdges == 8, s"sample_graph.txt should have 8 edges, got ${graph.nEdges}")

    // Verify all expected nodes are present
    val nodes = graph.allNodes.collect().toSet
    val expectedNodes = Set("A", "B", "C", "D")
    assert(nodes == expectedNodes,
      s"Expected nodes $expectedNodes but got $nodes")

    // Verify links structure
    val linksMap = graph.links.collect().toMap
    assert(linksMap("A").toSet == Set("B", "C", "D"), "A should link to B, C, D")
    assert(linksMap("B").toSet == Set("A", "C"), "B should link to A, C")
    assert(linksMap("C").toSet == Set("B", "D"), "C should link to B, D")
    assert(linksMap("D").toSet == Set("A"), "D should link to A")
  }

  // Test that GraphRDD properly handles the pipe separator
  test("GraphRDD should correctly parse pipe-separated format") {
    val path = "data/sample_graph.txt"

    val graph = new GraphRDD(
      name = "Graph-RDD-pipe-test",
      path = path,
      regex = "\\|"  // explicit pipe separator
    )

    val linksMap = graph.links.collect().toMap

    // Each line should be split correctly by pipe
    linksMap.foreach { case (src, dsts) =>
      assert(dsts.forall(_.nonEmpty), s"Source $src has empty destinations")
      assert(dsts.forall(!_.contains("|")), s"Destinations for $src should not contain pipe character")
    }
  }

  // Summary test that reports all graph statistics
  test("Summary: all data files statistics") {
    testLogger.info("=" * 60)
    testLogger.info("Graph Statistics Summary")
    testLogger.info("=" * 60)

    dataFiles.foreach { fileName =>
      val path = s"data/$fileName"
      val graph = new GraphRDD(
        name = s"Graph-${fileName.replace(".txt", "")}",
        path = path
      )

      testLogger.info(f"${fileName}%-20s | Nodes: ${graph.nNodes}%8d | Edges: ${graph.nEdges}%8d")
    }

    testLogger.info("=" * 60)
  }
}