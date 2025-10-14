package pagerank

import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel

/**
  * Implémentation DataFrame du PageRank :
  *  - Compatible avec GraphDF (src, dest)
  *  - Gestion des dangling nodes (redistribution de la masse)
  *  - Normalisation du vecteur à chaque itération
  *  - Persistences contrôlées pour réduire les shuffles
  *  - Support du mode debug et plot
  */
object PageRankDF {

  // =========================================================================
  // (1) Étape élémentaire (itération unique)
  // =========================================================================
  def oneStep(
      spark: SparkSession,
      ranks: DataFrame,
      edges: DataFrame,
      outdeg: DataFrame,
      danglingIds: DataFrame,
      N: Double,
      beta: Double,
      numParts: Int,
      st: StorageLevel
  ): DataFrame = {

    import spark.implicits._
    val p = if (numParts > 0) numParts else spark.sparkContext.defaultParallelism * 2

    // Masse totale des nœuds pendants
    val dm = ranks
      .join(danglingIds, Seq("id"), "left_semi")
      .agg(sum(col("rank")).as("dm"))
      .select(coalesce(col("dm"), lit(0.0)).as("dm"))
      .first()
    val danglingMass = dm.getDouble(0)

    // Calcul des contributions
    val outdegPos = outdeg.filter(col("outdeg") > 0)
      .repartition(p, col("id"))
      .withColumnRenamed("id", "src")
      .persist(st)

    val contribs = edges
      .join(ranks.select(col("id").as("src"), col("rank").as("rank_src")), Seq("src"), "inner")
      .join(outdegPos, Seq("src"), "inner")
      .withColumn("contrib", col("rank_src") / col("outdeg"))
      .groupBy(col("dest").as("id"))
      .agg(sum(col("contrib")).as("sumContrib"))
      .repartition(p, col("id"))
      .persist(st)

    val base = ((1.0 - beta) / N) + (beta * danglingMass / N)

    val newRanks = ranks
      .select("id")
      .join(contribs, Seq("id"), "left")
      .select(
        col("id"),
        (lit(base) + lit(beta) * coalesce(col("sumContrib"), lit(0.0))).as("rank")
      )
      .persist(st)

    // Normalisation
    val sumRanks = newRanks.agg(sum(col("rank")).as("s")).collect()(0).getAs[Double]("s")

    val newRanksNorm = newRanks
      .withColumn("rank", col("rank") / lit(sumRanks))
      .persist(st)
      .localCheckpoint(eager = true)

    outdegPos.unpersist(false)
    contribs.unpersist(false)
    newRanksNorm
  }

  // =========================================================================
  // (2) Calcul complet du PageRank (boucle principale)
  // =========================================================================
  def computePageRank(
      spark: SparkSession,
      edges: DataFrame,                  // DataFrame(src, dest)
      beta: Double = 0.85,
      iterations: Int = 20,
      numParts: Int = 0,
      st: StorageLevel = StorageLevel.MEMORY_AND_DISK,
      debug: Boolean = false,
      plot: Boolean = false,
      outputDir: String = ""
  )(implicit logger: Logger): DataFrame = {

    import spark.implicits._
    val p = if (numParts > 0) numParts else spark.sparkContext.defaultParallelism * 2

    logger.info(s"[PageRankDF] Initialisation ($iterations itérations, β=$beta)")

    // Préparation du graphe
    val E = edges.select(col("src"), col("dest"))
      .distinct()
      .repartition(p, col("src"))
      .persist(st)

    val nodes = E.select(col("src").as("id"))
      .union(E.select(col("dest").as("id")))
      .distinct()
      .repartition(p, col("id"))
      .persist(st)

    val outdeg = E.groupBy(col("src"))
      .agg(count(lit(1)).as("outdeg"))
      .withColumnRenamed("src", "id")
      .repartition(p, col("id"))
      .persist(st)

    val nodesWithDeg = nodes
      .join(outdeg, Seq("id"), "left")
      .na.fill(0, Seq("outdeg"))
      .persist(st)

    val N = nodes.count().toDouble
    var ranks = nodes.withColumn("rank", lit(1.0 / N)).persist(st)
    ranks = ranks.localCheckpoint(eager = true)

    val danglingIds = nodesWithDeg
      .filter(col("outdeg") === 0)
      .select(col("id"))
      .repartition(p, col("id"))
      .persist(st)
    danglingIds.count() // matérialisation

    // Historique pour le mode plot
    val history =
      if (plot) {
        val buf = scala.collection.mutable.ArrayBuffer.empty[Map[String, Double]]
        PageRankUtils.appendSnapshot(Some(buf), ranks)
        Some(buf)
      } else None

    // === Boucle principale ===
    for (i <- 1 to iterations) {
      if (debug) logger.info(s"[PageRankDF] --- Itération $i/$iterations ---")

      val newRanks = oneStep(spark, ranks, E, outdeg, danglingIds, N, beta, p, st)

      if (debug) {
        val sumRanks = newRanks.agg(sum(col("rank"))).as[Double].first()
        logger.info(f"[PageRankDF] Itération $i terminée (somme=$sumRanks%.6f)")
        newRanks.show(10, truncate = false)
      }

      ranks.unpersist(false)
      ranks = newRanks

      if (plot) PageRankUtils.appendSnapshot(history, ranks)
    }

    logger.info("[PageRankDF] Calcul PageRank terminé ✅")

    // Export CSV si demandé
    if (plot && outputDir.nonEmpty)
      PageRankUtils.exportHistoryToCSV(history.get.toSeq, outputDir, logger)

    // Matérialisation du résultat
    val _ = ranks.count()

    // Nettoyage
    E.unpersist(false)
    nodes.unpersist(false)
    outdeg.unpersist(false)
    nodesWithDeg.unpersist(false)
    danglingIds.unpersist(false)

    ranks
  }
}