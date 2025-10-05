package pagerank.rdd

import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object PageRankRDD {

    // Implémentation d'une itération de l’algorithme PageRank
    def oneStep(
        v: RDD[(String, Double)],
        links: RDD[(String, Seq[String])],
        allNodes: RDD[String],
        debug: Boolean = false,
        logger: Logger
    ): RDD[(String, Double)] = {

    val contributionsDetailed: RDD[(String, String, Double)] =
      links.join(v) // (q, (outlinks, v(q)))
        .flatMap { case (q, (outs, rankQ)) =>
          if (outs.isEmpty) Seq.empty[(String, String, Double)]
          else outs.map(dest => (dest, q, rankQ / outs.size))
        }

    if (debug) {
      logger.debug("==== Contributions (chaque source distribue son rang) ====")
      contributionsDetailed.collect().foreach { case (dest, src, contrib) =>
        logger.debug(f"$src%-5s -> $dest%-5s : $contrib%.6f")
      }
    }

    val contributions: RDD[(String, Double)] =
      contributionsDetailed.map { case (dest, _, contrib) => (dest, contrib) }

    val received: RDD[(String, Double)] = contributions.reduceByKey(_ + _)

    if (debug) {
      logger.debug("==== Rangs reçus (somme des contributions) ====")
      received.collect().foreach { case (node, rank) =>
        logger.debug(f"$node%-5s : $rank%.6f")
      }
    }

    val result = received
      .union(allNodes.map(n => (n, 0.0)))
      .reduceByKey(_ + _)

    result
  }

  // Calcul complet du PageRank sur N itérations (avec option d'historique)
  def computePageRank(
      links: RDD[(String, Seq[String])],
      iterations: Int,
      debug: Boolean = false,
      plot: Boolean = false,
      outputDir: Option[String] = None,
      logger: Logger
  )(implicit spark: SparkSession): RDD[(String, Double)] = {

    val sc = spark.sparkContext

    val srcNodes = links.keys
    val dstNodes = links.values.flatMap(identity)
    val allNodes = srcNodes.union(dstNodes).distinct().cache()
    val N = allNodes.count().toDouble

    val linksFull: RDD[(String, Seq[String])] = {
      val emptyByNode = allNodes.map(n => (n, Seq.empty[String]))
      emptyByNode.leftOuterJoin(links)
        .mapValues { case (_, maybeOuts) => maybeOuts.getOrElse(Seq.empty[String]) }
        .cache()
    }

    var v: RDD[(String, Double)] = allNodes.map(n => (n, 1.0 / N))

    // Historique facultatif
    val history = if (plot) Some(scala.collection.mutable.ArrayBuffer.empty[Map[String, Double]]) else None
    if (plot) history.get.append(v.collect().toMap)

    for (i <- 1 to iterations) {
      v = oneStep(v, linksFull, allNodes, debug, logger)

      if (debug) {
        val iteration_str = if (i > 1) "iterations" else "iteration"
        logger.debug(s"==== Nouveau vecteur après $i $iteration_str ====")
        v.collect().foreach { case (node, rank) =>
          logger.debug(f"$node%-5s : $rank%.6f")
        }
      }

      if (plot) {
        history.get.append(v.collect().toMap)
      }
    }

    // Export CSV si demandé
    if (plot && outputDir.isDefined) {
      import java.io.{File, PrintWriter}

      val outDir = new File(outputDir.get)
      if (!outDir.exists()) outDir.mkdirs()

      val outFile = new PrintWriter(s"${outputDir.get}/history.csv")
      try {
        val nodes = allNodes.collect().sorted
        outFile.println("Iteration," + nodes.mkString(","))
        for ((snapshot, i) <- history.get.zipWithIndex) {
          val line = nodes.map(n => snapshot.getOrElse(n, 0.0))
          outFile.println(s"$i," + line.mkString(","))
        }
      } finally {
        outFile.close()
      }

      logger.info(s"==== Historique PageRank exporté vers ${outputDir.get}/history.csv ====")
    }

    v
  }

}