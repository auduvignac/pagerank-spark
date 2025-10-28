package pagerank

import org.apache.log4j.Logger
import org.apache.spark.sql.{SparkSession, DataFrame, Dataset, Row}
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel

object PageRankDFPartitioned {

  /**
    * Effectue une itération optimisée du calcul PageRank avec DataFrames.
    *
    * Points clés :
    * - Pas de repartition dans la boucle : on s'appuie sur l'alignement initial.
    * - Agrégations globales via RDD[Double].sum() pour limiter la planification Catalyst.
    * - Persistance sérialisée disque fallback (MEMORY_AND_DISK_SER) par défaut.
    */
  def oneStep(
      ranks: DataFrame,       // (id, rank)
      edges: DataFrame,       // (src, dest)
      nodes: DataFrame,       // (id)
      outdegPos: DataFrame,   // (id, outdeg)
      danglingIds: DataFrame, // (id) où outdeg == 0
      N: Double,
      damping: Double = 1.0,
      storage: StorageLevel = StorageLevel.MEMORY_ONLY,
      debug: Boolean = false,
      logger: Logger
  )(implicit spark: SparkSession): DataFrame = {

    import spark.implicits._

    // === 1) Masse des dangling nodes (RDD.sum pour limiter les coûts Catalyst) ===
    val danglingMass: Double =
      ranks.join(danglingIds, Seq("id"), "left_semi")
        .select(col("rank"))
        .rdd
        .map(_.getDouble(0))
        .sum()

    if (debug) logger.debug(f"[PageRankDF] Dangling mass: $danglingMass%.6f")

    val contribs = edges
      .join(
        ranks.select(col("id").as("src"), col("rank").as("rank_src")),
        Seq("src"),
        "inner"
      )
      .join(outdegPos, Seq("src"), "inner")
      .withColumn("contrib", col("rank_src") / col("outdeg"))
      .groupBy(col("dest").as("id"))
      .agg(sum(col("contrib")).as("sumContrib"))
      .persist(storage)

    // === 3) Nouveau rang : base + damping * contributions (left join sur tous les nœuds) ===
    val base = ((1.0 - damping) / N) + (damping * danglingMass / N)

    val newRanks = nodes
      .join(contribs, Seq("id"), "left")
      .select(
        col("id"),
        (lit(base) + lit(damping) * coalesce(col("sumContrib"), lit(0.0))).as("rank")
      )
      .persist(storage)

    // === 4) Normalisation (RDD.sum) ===
    val sumRanks = newRanks.select(col("rank")).rdd.mapPartitions(it =>
      Iterator(it.map(_.getDouble(0)).sum)
    ).sum()

    val normalizedRanks = newRanks
      .withColumn("rank", col("rank") / lit(sumRanks))
      .localCheckpoint(eager = true)

    // === Cleanup temporaires ===
    contribs.unpersist(blocking = false)
    outdegPos.unpersist(blocking = false)
    newRanks.unpersist(blocking = false)

    normalizedRanks
  }

  /**
    * Calcul complet du PageRank optimisé avec DataFrames.
    *
    * Stratégie :
    * - Repartition initiale UNIQUE (hors boucle), persistance sérialisée robuste.
    * - Itérations sans repartition supplémentaires.
    * - Agrégations globales via RDD.sum.
    * - Option pour désactiver AQE sur gros graphes (peut éviter des reshuffles adaptatifs).
    */
  def computePageRank(
      graph: GraphDF,
      iterations: Int,
      damping: Double = 1.0,
      debug: Boolean = false,
      plot: Boolean = false,
      logger: Logger,
      outputDir: Option[String] = None,
      numParts: Int = 0,
      storage: StorageLevel = StorageLevel.MEMORY_ONLY,
      metrics: Boolean = false
  )(implicit spark: SparkSession): DataFrame = {

    import spark.implicits._

    // Déterminer le nombre de partitions (une seule préparation hors boucle)
    val p = if (numParts > 0) numParts else spark.sparkContext.defaultParallelism * 2
    if (debug) {
      logger.debug(s"[PageRankDF] Partitions utilisées: $p")
      logger.debug(s"[PageRankDF] StorageLevel: $storage")
    }

    val N: Double = graph.nNodes.toDouble

    // === (1) Edges (src, dest) ===
    val edges = graph.links
      .filter(col("outlink").isNotNull)
      .select(col("page").as("src"), col("outlink").as("dest"))
      .distinct()
      .repartitionByRange(p, col("src"))
      .persist(storage)

    // === (2) Nodes (id) ===
    // Remarque : si GraphDF expose allNodes, on peut l'utiliser directement.
    val nodes = edges.select(col("src").as("id"))
      .union(edges.select(col("dest").as("id")))
      .distinct()
      .repartition(p, col("id")) // **unique repartition**, alignée sur id
      .persist(storage)

    // === (3) Out-degree (id, outdeg) ===
    val outdeg = edges
      .groupBy(col("src"))
      .agg(count(lit(1)).as("outdeg"))
      .withColumnRenamed("src", "id")
      .repartition(p, col("id"))
      .persist(storage)

    // === (4) NodesWithDeg + Dangling ===
    val nodesWithDeg = nodes
      .join(outdeg, Seq("id"), "left")
      .na.fill(0, Seq("outdeg"))
      .persist(storage)

    // === Contributions : joindre edges ↔ ranks ↔ outdeg>0 puis groupBy(dest) ===
    val outdegPos = outdeg
      .filter(col("outdeg") > 0)
      .withColumnRenamed("id", "src")
      .persist(storage)

    val danglingIds = nodesWithDeg
      .filter(col("outdeg") === 0)
      .select(col("id"))
      .persist(storage)

    if (debug) {
      val danglingCount = danglingIds.count()
      logger.debug(s"[PageRankDF] Dangling nodes: $danglingCount")
    }

    // === (5) Ranks init (id, rank) ===
    var ranks = nodes
      .withColumn("rank", lit(1.0 / N))
      .persist(storage)
      .localCheckpoint(eager = true)

    // Historique optionnel
    val history =
      if (plot) {
        val buf = scala.collection.mutable.ArrayBuffer.empty[Map[String, Double]]
        PageRankUtils.appendSnapshot(Some(buf), ranks.withColumnRenamed("id", "page"))
        Some(buf)
      } else None

    // === (6) Boucle des itérations ===
    for (i <- 1 to iterations) {

      if (metrics) {
        logger.info(s"==== Début itération $i/$iterations ====")
        spark.sparkContext.setJobGroup(
          s"PageRank-DF-Partitioned-iter-$i",
          s"PageRank DF-Partitioned iteration $i"
        )
      }

      val newRanks = oneStep(
        ranks = ranks,
        edges = edges,
        nodes = nodes,
        outdegPos = outdegPos,
        danglingIds = danglingIds,
        N = N,
        damping = damping,
        storage = storage,
        debug = debug,
        logger = logger
      )

      if (metrics) {
        // Matérialise pour exposer les métriques au listener
        newRanks.count()
      }

      // Rotation des persistes
      ranks.unpersist(blocking = false)
      ranks = newRanks

      if (metrics) {
        spark.sparkContext.clearJobGroup()
      }

      if (debug) {
        val iterWord = if (i > 1) "itérations" else "itération"
        logger.debug(s"==== Nouveau vecteur après $i $iterWord ====")
        val ranksArray = ranks.collect()
        ranksArray.foreach { row =>
          val node = row.getAs[String]("id")
          val rank = row.getAs[Double]("rank")
          logger.debug(f"$node%-5s : $rank%.6f")
        }
        val sum = ranksArray.map(_.getAs[Double]("rank")).sum
        logger.debug(f"Somme des rangs: $sum%.6f")
      }

      if (plot) {
        PageRankUtils.appendSnapshot(history, ranks.withColumnRenamed("id", "page"))
      }
    }

    if (plot && outputDir.isDefined) {
      PageRankUtils.exportHistoryToCSV(history.get.toSeq, outputDir.get, logger)
    }

    if (metrics) {
      spark.sparkContext.setJobGroup(
        s"PageRank-DF-Partitioned-ranks.count()",
        s"PageRank-DF-Partitioned-ranks.count()"
      )
    }

    val finalCount = ranks.count()

    if (debug) {
      logger.debug(s"[PageRankDF] Nombre final de nœuds: $finalCount")
    }

    if (metrics) {
      spark.sparkContext.clearJobGroup()
    }

    // === Cleanup structures intermédiaires ===
    edges.unpersist(blocking = false)
    nodes.unpersist(blocking = false)
    outdeg.unpersist(blocking = false)
    nodesWithDeg.unpersist(blocking = false)
    danglingIds.unpersist(blocking = false)

    // Renommer "id" en "page" pour homogénéité avec l'API RDD
    ranks.withColumnRenamed("id", "page")
  }
}
