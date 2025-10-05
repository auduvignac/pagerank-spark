package pagerank.df

import org.scalatest.funsuite.AnyFunSuite
import org.apache.log4j.Logger
import utils.SparkTestSession
import pagerank.GraphUtils

class MainDFSpec extends AnyFunSuite with SparkTestSession {

  import spark.implicits._

  val testLogger: Logger = Logger.getLogger("TestLogger")
  implicit val sparkSession = spark

  val path = "data/sample_graph.txt"

  // Lecture du fichier
  val lines = GraphUtils.readAsDataset(path)

  // Parsing en DataFrame (page, outlink)
  val links = GraphUtils.parseGraphDF(
    lines,
    false,
    testLogger,
    Some(path)
  )

  test("oneStep conserve les invariants sur sample_graph.txt (DF)") {

    // Ensemble de tous les noeuds (sources + destinations)
    val allNodes = links.select("page")
      .union(links.select("outlink"))
      .distinct()
      .as[String]
      .collect()
      .sorted

    val N = allNodes.length.toDouble

    // Initialisation uniforme
    var v0 = allNodes.map(n => (n, 1.0 / N)).toSeq.toDF("page", "rank")

    // Appliquer une itération de PageRank
    val v1 = PageRankDF.oneStep(v0, links, allNodes, debug = false, testLogger)

    val collected = v1.collect().map(r => r.getString(0) -> r.getDouble(1)).toMap

    // Vérifie que la somme des rangs est ≈ 1
    val sum = collected.values.sum
    assert(math.abs(sum - 1.0) < 1e-6)

    // Vérifie que tous les noeuds sont présents
    val nodes = collected.keySet
    assert(nodes == Set("A", "B", "C", "D"))
  }

  test("computePageRank converge correctement sur sample_graph.txt (DF)") {

    // Calcul complet avec 50 itérations
    val result = PageRankDF.computePageRank(
      links,
      iterations = 10,
      debug = false,
      logger = testLogger
    )

    val v = result.collect().map(r => r.getString(0) -> r.getDouble(1)).toMap

    // Résultats attendus
    val expected = Map(
      "A" -> 0.33333333,
      "B" -> 0.22222222,
      "C" -> 0.22222222,
      "D" -> 0.22222222
    )

    // Vérifie somme
    val sum = v.values.sum
    assert(math.abs(sum - 1.0) < 1e-6)

    // Vérifie convergence
    expected.foreach { case (node, expRank) =>
      assert(
        math.abs(v(node) - expRank) < 1e-3,
        s"Rank for $node was ${v(node)} but expected $expRank"
      )
    }
  }
}