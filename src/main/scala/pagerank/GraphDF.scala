package pagerank

import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel

/**
  * Représentation DataFrame du graphe (lazy + scalable + debug détaillé)
  *
  * Étapes :
  *  - Lecture lazy (aucun chargement direct)
  *  - Nettoyage complet (self-loops, doublons, liens rouges)
  *  - Debug progressif : logs intermédiaires avec comptage partiel
  *  - Persistance contrôlée
  */
class GraphDF(
    name: String,
    path: String,
    debug: Boolean = false,
    sepRegex: String = "\\|",
    storage: StorageLevel = StorageLevel.MEMORY_AND_DISK,
    numParts: Int = 0
)(implicit spark: SparkSession, @transient logger: Logger)
  extends BaseGraph(name, path, debug)(spark, logger) {

  import spark.implicits._

  // =========================================================================
  // (1) Parsing DataFrame du graphe (lazy)
  // =========================================================================
  lazy val (edges, nNodes, nEdges): (DataFrame, Long, Long) = {

    val p = if (numParts > 0) numParts else spark.sparkContext.defaultParallelism * 2

    if (debug) {
      logger.info(s"[GraphDF:$name] Début du parsing DataFrame depuis : $path")
      logger.info(s"[GraphDF:$name] Options : sep='$sepRegex' | partitions=$p | storage=$storage")
    }

    // === (1) Lecture des lignes non vides ===
    val raw = spark.read.text(path)
      .select(trim(col("value")).as("value"))
      .filter(length(col("value")) > 0)
      .repartition(p)

    if (debug) {
      val count = raw.count()
      logger.info(s"[GraphDF:$name] Étape 1 : Lecture du fichier ($count lignes non vides)")
    }

    // === (2) Tokenisation + nettoyage ===
    val withToks = raw
      .withColumn("toks", split(col("value"), sepRegex))
      .withColumn("toks", transform(col("toks"), x => trim(x)))
      .withColumn("cleaned", filter(col("toks"), x => x =!= lit("")))

    val perLine = withToks.select(
      when(size(col("cleaned")) >= 1, element_at(col("cleaned"), 1)).as("src"),
      when(size(col("cleaned")) >= 2, slice(col("cleaned"), 2, 1000000)).otherwise(array()).as("outs")
    ).filter(col("src").isNotNull && length(col("src")) > 0)

    if (debug) {
      val nSrc = perLine.select("src").distinct().count()
      logger.info(s"[GraphDF:$name] Étape 2 : Extraction des pages sources : $nSrc")
    }

    // === (3) Ensemble des pages valides ===
    val pages = perLine.select(col("src")).distinct()
      .repartition(p, col("src"))
      .persist(storage)
    val nValid = pages.count()
    if (debug) logger.info(s"[GraphDF:$name] Étape 3 : Pages valides = $nValid")

    // === (4) Construction des arêtes (src, dest) ===
    val pagesAsDest = pages.withColumnRenamed("src", "dest")

    val edgesDF = perLine
      .select(col("src"), explode_outer(col("outs")).as("dest"))
      .filter(col("dest").isNotNull && length(col("dest")) > 0)
      .filter(col("dest") =!= col("src"))               // self-loops
      .join(pagesAsDest, Seq("dest"), "left_semi")      // liens rouges
      .dropDuplicates("src", "dest")
      .repartition(p, col("src"))
      .persist(storage)

    val nEdgesValides = edgesDF.count()
    if (debug) logger.info(s"[GraphDF:$name] Étape 4 : Arêtes finales = $nEdgesValides")

    // === (5) Comptage final des nœuds (src ∪ dest) ===
    val allNodes = edgesDF
      .select($"src".as("node"))
      .union(edgesDF.select($"dest".as("node")))
      .distinct()
      .persist(storage)

    val nNodesTotal = allNodes.count()
    if (debug) logger.info(s"[GraphDF:$name] Étape 5 : Nœuds uniques = $nNodesTotal")

    if (debug) {
      logger.info(s"[GraphDF:$name] Graphe DF final prêt ✅")
      logger.info(s"==== Aperçu du graphe DataFrame '$name' ====")
      edgesDF.show(10, truncate = false)
    }

    (edgesDF, nNodesTotal, nEdgesValides)
  }

  // =========================================================================
  // (2) Accesseurs et résumé
  // =========================================================================

  /** DataFrame des arêtes (src, dest) */
  lazy val links: DataFrame = edges

  /** Nombre total de nœuds */
  def nodeCount(): Long = nNodes

  /** Nombre total d’arêtes */
  def edgeCount(): Long = nEdges

  /** Liste de toutes les pages */
  lazy val allPages: Dataset[String] =
    links.select($"src".as[String]).union(links.select($"dest".as[String])).distinct()

  /** Affichage résumé */
  override def describe(): Unit = {
    logger.info(s"===== Graphe DF: $name =====")
    logger.info(s"Fichier source : $fileName")
    logger.info(f"Nombre de nœuds : $nNodes%,d | Nombre d'arêtes : $nEdges%,d")
    if (debug) {
      logger.info("Aperçu des premières arêtes :")
      links.show(5, truncate = false)
    }
    logger.info("=============================")
  }
}