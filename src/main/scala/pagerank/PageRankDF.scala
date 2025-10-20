package pagerank

import org.apache.log4j.Logger
import org.apache.spark.sql.{SparkSession, DataFrame, Dataset, Row}
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel

object PageRankDF {

    /**
    * Effectue une itération optimisée du calcul PageRank avec DataFrames
    *
    * OPTIMIZATION STRATEGY:
    * 1. Tous les DataFrames sont repartitionnés sur la même clé (id)
    * 2. Utilise des joins pré-partitionnés pour éviter les shuffles
    * 3. Gère correctement les dangling nodes (nœuds sans liens sortants)
    * 4. Normalise après chaque itération pour stabilité numérique
    *
    * @param ranks        DataFrame des rangs (id, rank) - repartitionné sur "id"
    * @param edges        DataFrame des arêtes (src, dest) - repartitionné sur "src"
    * @param nodes        DataFrame des nœuds (id) - repartitionné sur "id"
    * @param outdeg       DataFrame des degrés sortants (id, outdeg) - repartitionné sur "id"
    * @param danglingIds  DataFrame des IDs dangling (id) - repartitionné sur "id"
    * @param N            nombre total de nœuds
    * @param damping      facteur de damping (beta)
    * @param numParts     nombre de partitions
    * @param storage      niveau de persistance
    * @param debug        afficher les logs de debug
    * @param logger       logger pour les messages
    */
    def oneStep(
        ranks: DataFrame,
        edges: DataFrame,
        nodes: DataFrame,
        outdeg: DataFrame,
        danglingIds: DataFrame,
        N: Double,
        damping: Double = 1.0,
        numParts: Int,
        storage: StorageLevel,
        debug: Boolean = false,
        logger: Logger
    )(implicit spark: SparkSession): DataFrame = {

    import spark.implicits._

    // STEP 1: Calculer la masse des dangling nodes
    val danglingMassRow = ranks
      .join(danglingIds, Seq("id"), "left_semi")  // Semi-join efficient
      .agg(sum(col("rank")).as("dm"))
      .select(coalesce(col("dm"), lit(0.0)).as("dm"))
      .first()

    val danglingMass = danglingMassRow.getDouble(0)

    if (debug) {
      logger.debug(f"Dangling mass: $danglingMass%.6f")
    }

    // STEP 2: Calculer les contributions (optimized avec joins pré-partitionnés)
    // outdegPos: seulement les nœuds avec des sorties
    val outdegPos = outdeg
      .filter(col("outdeg") > 0)
      .repartition(numParts, col("id"))
      .withColumnRenamed("id", "src")
      .persist(storage)

    // Contributions: 2 joins sur "src" puis 1 groupBy sur "dest"
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
      .repartition(numParts, col("id"))  // Aligner pour la jointure suivante
      .persist(storage)

    // STEP 3: Calculer les nouveaux rangs avec la formule PageRank
    val base = ((1.0 - damping) / N) + (damping * danglingMass / N)

    val newRanks = nodes
      .join(contribs, Seq("id"), "left")
      .select(
        col("id"),
        (lit(base) + lit(damping) * coalesce(col("sumContrib"), lit(0.0))).as("rank")
      )
      .persist(storage)

    // STEP 4: Normaliser pour éviter la dérive numérique
    val sumRanks = newRanks
      .agg(sum(col("rank")).as("s"))
      .collect()(0)
      .getAs[Double]("s")

    val normalizedRanks = newRanks
      .withColumn("rank", col("rank") / lit(sumRanks))
      .persist(storage)
      .localCheckpoint(eager = true)  // Coupe la lignée pour éviter le stackoverflow

    // Cleanup temporaires
    contribs.unpersist(blocking = false)
    outdegPos.unpersist(blocking = false)
    newRanks.unpersist(blocking = false)

    normalizedRanks
  }

  /**
    * Calcul complet du PageRank optimisé avec DataFrames
    *
    * OPTIMIZATION STRATEGY:
    * 1. Repartitionne TOUS les DataFrames sur la même clé pour éviter les shuffles
    * 2. Persiste les structures pré-calculées avant les itérations
    * 3. Utilise localCheckpoint() pour couper la lignée et éviter StackOverflow
    * 4. Gère les dangling nodes correctement
    * 5. Normalise après chaque itération
    *
    * @param graph      le graphe DataFrame à analyser
    * @param iterations nombre d'itérations
    * @param damping    facteur de damping (beta), typiquement 0.85
    * @param debug      afficher les logs de debug
    * @param plot       capturer l'historique pour export
    * @param logger     logger pour les messages
    * @param outputDir  répertoire pour exporter l'historique (si plot=true)
    * @param numParts   nombre de partitions (0 = auto = defaultParallelism * 2)
    * @param storage    niveau de persistance
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

    // Déterminer le nombre de partitions
    val p = if (numParts > 0) numParts else spark.sparkContext.defaultParallelism * 2

    if (debug) {
      logger.debug(s"[PageRankDF] Utilisation de $p partitions")
    }

    val N: Double = graph.nNodes.toDouble

    // === STEP 1: Préparer les arêtes (src, dest) et repartitionner ===
    val edges = graph.links
      .filter(col("outlink").isNotNull)  // Filtrer les outlinks null
      .select(col("page").as("src"), col("outlink").as("dest"))
      .distinct()
      .repartition(p, col("src"))
      .persist(storage)

    // === STEP 2: Préparer l'ensemble des nœuds et repartitionner ===
    val nodes = edges.select(col("src").as("id"))
      .union(edges.select(col("dest").as("id")))
      .distinct()
      .repartition(p, col("id"))
      .persist(storage)

    // === STEP 3: Calculer le degré sortant pour chaque nœud ===
    val outdeg = edges
      .groupBy(col("src"))
      .agg(count(lit(1)).as("outdeg"))
      .withColumnRenamed("src", "id")
      .repartition(p, col("id"))
      .persist(storage)

    // === STEP 4: Créer nodesWithDeg (tous les nœuds avec leur degré) ===
    val nodesWithDeg = nodes
      .join(outdeg, Seq("id"), "left")
      .na.fill(0, Seq("outdeg"))
      .persist(storage)

    // === STEP 5: Initialiser les rangs ===
    var ranks = nodes
      .withColumn("rank", lit(1.0 / N))
      .persist(storage)

    // Couper la lignée pour éviter le StackOverflow
    ranks = ranks.localCheckpoint(eager = true)

    // === STEP 6: Identifier les dangling nodes (degré sortant = 0) ===
    val danglingIds = nodesWithDeg
      .filter(col("outdeg") === 0)
      .select(col("id"))
      .repartition(p, col("id"))
      .persist(storage)

    // Matérialiser pour vérifier
    val danglingCount = danglingIds.count()
    if (debug) {
      logger.debug(s"[PageRankDF] Nombre de dangling nodes: $danglingCount")
    }

    // Historique optionnel
    val history =
      if (plot) {
        val buf = scala.collection.mutable.ArrayBuffer.empty[Map[String, Double]]
        PageRankUtils.appendSnapshot(Some(buf), ranks.withColumnRenamed("id", "page"))
        Some(buf)
      } else None

    // === STEP 7: Itérations PageRank ===
    for (i <- 1 to iterations) {

      if (metrics) {
        logger.info(s"==== Début itération $i/$iterations ====")

        // Taguer tous les jobs de cette itération
        spark.sparkContext.setJobGroup(
          s"PageRank-DF-iter-$i",
          s"PageRank DF iteration $i"
        )
      }

      val newRanks = oneStep(
        ranks = ranks,
        edges = edges,
        nodes = nodes,
        outdeg = outdeg,
        danglingIds = danglingIds,
        N = N,
        damping = damping,
        numParts = p,
        storage = storage,
        debug = debug,
        logger = logger
      )

      if (metrics) {
        // Déclenche l’action qui matérialise cette itération
        // (permet à Spark de produire les métriques dans le listener)
        newRanks.count()
      }

      // Cleanup ancien ranks
      ranks.unpersist(blocking = false)
      ranks = newRanks

      if (metrics) {
        // Fin du tag
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
      // Taguer tous les jobs de cette itération
      spark.sparkContext.setJobGroup(
        s"PageRank-DF-ranks.count()",
        s"PageRank-DF-ranks.count()"
      )
    }

    // Matérialiser le résultat final
    val finalCount = ranks.count()

    if (debug) {
      logger.debug(s"[PageRankDF] Nombre final de nœuds: $finalCount")
    }

    if (metrics) {
      // Fin du tag
      spark.sparkContext.clearJobGroup()
    }

    // Cleanup des structures intermédiaires
    edges.unpersist(blocking = false)
    nodes.unpersist(blocking = false)
    outdeg.unpersist(blocking = false)
    nodesWithDeg.unpersist(blocking = false)
    danglingIds.unpersist(blocking = false)

    // Renommer "id" en "page" pour compatibilité
    ranks.withColumnRenamed("id", "page")
  }
}