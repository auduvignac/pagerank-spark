// EMIASD 2025 - Projet Spark Graphes - PageRank
// Jean-Marc Fauvel - Aurélien Duvignac Rosa - Edoardo Piciucchi - 2024/2025
//
// Implémentation de PageRank en RDD et DataFrame
// Mesures de temps et mémoire
// Benchmarks et export des résultats
// Export des top N nœuds par approche
// Usage: voir script_run

// Scala 2.12/2.13 compatible

// Imports des bibliothèques Spark et Java
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.rdd.RDD
import org.apache.spark.HashPartitioner

import java.nio.file.{Files, Paths}
import java.util.concurrent.TimeUnit

// Objet principal
// Contient toutes les fonctions et le main
// Variables volatiles pour le contexte d’export TopN
object PageRankBench {

  @volatile private[PageRankBench] var _currentDataset: String = ""
  @volatile private[PageRankBench] var _currentBeta: Double = 0.85
  @volatile private[PageRankBench] var _currentIters: Int = 5
  @volatile private[PageRankBench] var _currentExportTopN: Int = 0
  @volatile private[PageRankBench] var _currentExportDir: String = ""

  // ===== Valeurs par défaut des arguments de run =====
  final case class Args(
    // Algorithme(s): rdd | rddp | df | all
    algo: String = "all",
    // Chemin d’un fichier ou répertoire (Spark textFile supporte wildcard et répertoires)
    input: String = "",
    // Nom logique du dataset (apparaitra dans le CSV)
    dataset: String = "dataset",
    // Beta (damping factor)
    beta: Double = 0.85,
    // Nombre d’itérations
    iters: Int = 5,
    // Séparateur brut (regex Java)
    sepRegex: String = "[|\\s]+",
    // Partitions
    partsDF: Int = 0,   // 0 => 2 * defaultParallelism (par défaut)
    partsRDD: Int = 0,  // 0 => 2 * defaultParallelism (par défaut)
    // Sortie résultats
    exportTopN: Int = 0,          // 0 = désactivé ; sinon nb de nœuds à exporter
    exportDir: String = "",       // dossier cible des exports CSV (ex: /tmp/pagerank_exports)
    outCsv: String = "pagerank_bench_results",
    outFormat: String = "csv", // csv | parquet
    // storage level ("MEMORY_ONLY", "MEMORY_AND_DISK", etc.)
    storage: String = "MEMORY_ONLY",
    // Repartir des runs précédents (append) ?
    append: Boolean = true,
    // Verbose
    verbose: Boolean = true
  )

  // ===== Bench record =====
  // Pour l’export des résultats
  case class BenchRecord(
    dataset: String,
    approach: String,    // "rdd" | "rdd_partitioned" | "dataframe"
    nNodes: Long,
    nEdges: Long,
    parseMs: Long,
    computeMs: Long,
    totalMs: Long,
    peakUsedMemMB: Long
  )

  // ===== Utils temps & mémoire =====
  // Temps en ns, conversion en ms  
  def nowNs(): Long = System.nanoTime()
  def elapsedMs(startNs: Long): Long =
    TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNs)

  // Mémoire utilisée en MB (driver + executors)
  def currentUsedMemMB(spark: SparkSession): Long = {
    val statuses = spark.sparkContext.getExecutorMemoryStatus // Map[id,(max,free)]
    val execUsed = statuses.values.map { case (max, free) => max - free }.sum
    val rt = Runtime.getRuntime
    val driverUsed = rt.totalMemory() - rt.freeMemory()
    val used = execUsed + driverUsed
    math.round(used.toDouble / (1024 * 1024))
  }

  // Exécute un bloc de code en mesurant le pic mémoire atteint pendant son exécution
  def withMemPeak[T](spark: SparkSession)(block: => T): (T, Long) = {
    var peak = currentUsedMemMB(spark)
    val res = block
    peak = math.max(peak, currentUsedMemMB(spark))
    (res, peak)
  }

  // ===== StorageLevel from string =====
  // Défini dans Args.storage
  // Défaut MEMORY_ONLY si non reconnu  
  def storageLevelOf(name: String): StorageLevel =
    name.toUpperCase match {
      case "MEMORY_ONLY"         => StorageLevel.MEMORY_ONLY
      case "MEMORY_ONLY_SER"     => StorageLevel.MEMORY_ONLY_SER
      case "MEMORY_AND_DISK"     => StorageLevel.MEMORY_AND_DISK
      case "MEMORY_AND_DISK_SER" => StorageLevel.MEMORY_AND_DISK_SER
      case "DISK_ONLY"           => StorageLevel.DISK_ONLY
      case _                     => StorageLevel.MEMORY_ONLY
    }


  // ===== fonction d'export =====
  // Utilise les variables volatiles pour le contexte
  //
  // DF : export topN
  private def exportTopNDF(
    spark: SparkSession,
    ranksDF: DataFrame,        // colonnes: id, rank
    dataset: String,
    approach: String,
    beta: Double,
    iters: Int,
    topN: Int,
    outDir: String
  ): Unit = {
    import org.apache.spark.sql.functions._
    require(topN > 0 && outDir.nonEmpty, "exportTopNDF: topN>0 et outDir requis")

    val toWrite = ranksDF
      .orderBy(desc("rank"))
      .limit(topN)
      .withColumn("dataset", lit(dataset))
      .withColumn("approach", lit(approach))
      .withColumn("beta", lit(beta))
      .withColumn("iters", lit(iters))
      .select("dataset","approach","beta","iters","id","rank")
      .coalesce(1)

    toWrite.write
      .mode("append")
      .option("header","true")
      .csv(outDir)
  }

  // RDD: export topN
  private def exportTopNRDD(
      spark: SparkSession,
      ranksRDD: RDD[(String, Double)],
      dataset: String,
      approach: String,
      beta: Double,
      iters: Int,
      topN: Int,
      outDir: String
  ): Unit = {
    import spark.implicits._
    require(topN > 0 && outDir.nonEmpty, "exportTopNRDD: topN>0 et outDir requis")

    // top N côté driver (N=20 → négligeable)
    val top = ranksRDD.top(topN)(Ordering.by(_._2))
    val df  = spark.createDataset(top).toDF("id","rank")
      .withColumn("dataset", lit(dataset))
      .withColumn("approach", lit(approach))
      .withColumn("beta", lit(beta))
      .withColumn("iters", lit(iters))
      .select("dataset","approach","beta","iters","id","rank")
      .coalesce(1)

    df.write
      .mode("append")
      .option("header","true")
      .csv(outDir)
  }
  
  // ===== Parsing commun RDD =====
  // raw: lecture des lignes texte non vides
  // parsed: en sortie RDD[(src, List(dest))] avec dest distinct par ligne
  // nEdges: somme des tailles de outs (par ligne)
  // nNodes: | src ∪ dest | union des sources et de toutes les destinations
  // sepRegex: séparateur
  // parsedRDD devant être réutilisé, nous le persistons
  // st: StorageLevel pour le persist  
  def parseRDD(
    spark: SparkSession,
    path: String,
    sepRegex: String,
    st: StorageLevel
  ): (RDD[(String, List[String])], Long, Long) = {
    val sc = spark.sparkContext

    val raw = sc.textFile(path).map(_.trim).filter(_.nonEmpty)

    // Pass 1 : (src, outs[])
    val pass1: RDD[(String, Array[String])] = raw.flatMap { line =>
      val toks = line.split(sepRegex, -1).iterator.map(_.trim).filter(_.nonEmpty).toArray
      if (toks.isEmpty) None
      else {
        val src  = toks(0)
        val outs = if (toks.length > 1) java.util.Arrays.copyOfRange(toks, 1, toks.length) else Array.empty[String]
        Some((src, outs))
      }
    }.persist(st)

    // Pages existantes = src distinctes
    val validPages = pass1.keys.distinct().collect().toSet
    val validBc = sc.broadcast(validPages)

    // Dédup global par (src,dest) + filtre liens rouges + self-loops + vides
    // On a besoin de 'src' dans seqOp, donc on map d'abord chaque ligne en (src, outs filtrés),
    // puis on agrège en Set par 'src'.
    val cleanedPerLine: RDD[(String, Array[String])] = pass1.map { case (src, outs) =>
      val filtered = outs.iterator
        .filter(_ != null)
        .map(_.trim)
        .filter(_.nonEmpty)
        .filter(_ != src)                       // retire self-loop ici (comme DF)
        .filter(validBc.value.contains)         // retire liens rouges (dest ∈ pages)
        .toArray
      (src, filtered)
    }

    val aggregated: RDD[(String, Set[String])] =
      cleanedPerLine.aggregateByKey(Set.empty[String])(
        (acc, outs) => acc ++ outs.iterator,    // seqOp
        (a, b) => a ++ b                        // combOp
      )

    val parsed: RDD[(String, List[String])] = aggregated.mapValues(_.toList).persist(st)

    val nEdges = parsed.map { case (_, outs) => outs.size.toLong }.sum().toLong
    val nNodes = validPages.size.toLong

    (parsed, nNodes, nEdges)
  }


  // ===== Parsing DF (edges(src,dest)) =====
  // raw: lecture des lignes texte non vides
  // edges: en sortie DataFrame(src, dest) avec dest distinct par src
  // nEdges: nombre total d’arêtes (taille de edges)
  // nNodes: | src ∪ dest | union des sources et de toutes les destinations
  // sepRegex: séparateur
  // edges devant être réutilisé, nous le persistons
  // st: StorageLevel pour le persist
  // ===== Parsing commun DF (edges(src,dest)) =====
  // - pages = DISTINCT src (comme parseRDD -> validPages)
  // - enlève self-loops
  // - enlève liens rouges (dest ∉ pages)
  // - dédoublonne (src, dest)
  // - persiste et renvoie (edges, nNodes, nEdges)
  def parseDF(
      spark: SparkSession,
      path: String,
      sepRegex: String,
      numParts: Int,
      st: StorageLevel
  ): (DataFrame, Long, Long) = {
    import org.apache.spark.sql.functions._
    val p = if (numParts > 0) numParts else spark.sparkContext.defaultParallelism * 2

    // 1) Lecture lignes non vides
    val raw = spark.read.text(path)
     .select(trim(col("value")).as("value"))
     .filter(length(col("value")) > 0)


   val withToks = raw
     .withColumn("toks", split(col("value"), sepRegex))
     .withColumn("toks", transform(col("toks"), x => trim(x)))
     .withColumn("cleaned", filter(col("toks"), x => x =!= lit("")))

    // 3) src + outs
    val perLine = withToks.select(
      when(size(col("cleaned")) >= 1, element_at(col("cleaned"), 1)).as("src"),
      when(size(col("cleaned")) >= 2, slice(col("cleaned"), 2, 1000000)).otherwise(array()).as("outs")
    ).filter(col("src").isNotNull && length(col("src")) > 0)

    // 4) Pages existantes = toutes les src (on retient les nœuds sans sorties)
    val pages = perLine.select(col("src")).distinct()
      .repartition(p, col("src"))
      .persist(st)
    val nNodes = pages.count()

    // 5) Edges brutes (src,dest) + filtre self-loops + liens rouges + dédup global
    val pagesAsDest = pages.withColumnRenamed("src", "dest")

    val edges = perLine
      .select(col("src"), explode_outer(col("outs")).as("dest"))
      .filter(col("dest").isNotNull && length(col("dest")) > 0)
      .filter(col("dest") =!= col("src"))              // retire self-loops
      .join(pagesAsDest, Seq("dest"), "left_semi")     // retire liens rouges (dest ∈ pages)
      .dropDuplicates("src", "dest")                   // dédup global (src,dest)
      .repartition(p, col("src"))
      .persist(st)

    val nEdges = edges.count()
    (edges, nNodes, nEdges)
  }




  // ===== PageRank RDD baseline ====================================================
  // parsed: RDD[(src, List(dest))] avec dest distinct par ligne
  // st: StorageLevel pour les persistences
  // Approche "classique" non partitionnée
  // Voir https://spark.apache.org/docs/latest/graphx-programming-guide.html#pagerank
  // et https://nlp.stanford.edu/IR-book/html/htmledition/pagerank-1.html
  // On gère les dangling nodes en redistribuant leur masse uniformément
  // à chaque itération
  // On normalise les rangs à chaque itération pour éviter la dérive
  // On matérialise le RDD final par un count()
  // On exporte le top N si demandé (utilise des variables volatiles pour le contexte)  
  def pageRankRDD(
      spark: SparkSession,
      parsed: RDD[(String, List[String])],
      beta: Double,
      nbIter: Int,
      st: StorageLevel
  ): Unit = {
    import org.apache.spark.rdd.RDD.rddToPairRDDFunctions

    val sources  = parsed.keys
    val dests    = parsed.flatMap { case (_, outs) => outs }
    val allNodes = (sources union dests).distinct().persist(st)

    val outMap: RDD[(String, (Int, List[String]))] = parsed
      .reduceByKey(_ ++ _)
      .mapValues(_.distinct)
      .mapValues(outs => (outs.size, outs))

    val nodesWithEmpty: RDD[(String, (Int, List[String]))] =
      allNodes
        .map(nodeId => (nodeId, (0, List.empty[String])))
        .leftOuterJoin(outMap)
        .mapValues {
          case ((_, _), Some((deg, outs))) => (deg, outs)
          case _                           => (0, List.empty[String])
        }
        .persist(st)

    val nCount = allNodes.count().toDouble
    var ranks: RDD[(String, Double)] = allNodes.map(id => (id, 1.0 / nCount)).persist(st)

    (1 to nbIter).foreach { _ =>
      val danglingMass = nodesWithEmpty.join(ranks)
        .filter { case (_, ((deg, _), _)) => deg == 0 }
        .map { case (_, ((_, _), r)) => r }
        .sum()

      val contribs = nodesWithEmpty.join(ranks)
        .flatMap { case (_, ((deg, outs), r)) =>
          if (deg == 0) Iterator.empty
          else outs.iterator.map(d => (d, r / deg))
        }
        .reduceByKey(_ + _)

      val base = ((1.0 - beta) / nCount) + (beta * danglingMass / nCount)

      val newRanks = allNodes
        .map(id => (id, base))
        .leftOuterJoin(contribs)
        .mapValues { case (b, cOpt) => b + beta * cOpt.getOrElse(0.0) }
        .persist(st)

      val sumRanks = newRanks.values.sum()
      ranks.unpersist(blocking = false)
      ranks = newRanks.mapValues(_ / sumRanks).persist(st)
    }

    // matérialisation
    val _cnt = ranks.count()

    // Export top N si demandé (utilise une variable implicite ou passe spark en param)
    if (PageRankBench._currentExportTopN > 0 && PageRankBench._currentExportDir.nonEmpty) {
      exportTopNRDD(
        spark,
        ranks,
        PageRankBench._currentDataset,
        "rdd",
        PageRankBench._currentBeta,
        PageRankBench._currentIters,
        PageRankBench._currentExportTopN,
        PageRankBench._currentExportDir
      )
    }

    nodesWithEmpty.unpersist(false); allNodes.unpersist(false)
  }

  // ===== PageRank RDD partitionné =====
  // Même algo que pageRankRDD mais avec un partitioner
  // pour minimiser les shuffles  
  def pageRankRDDPartitioned(
      spark: SparkSession,
      parsed: RDD[(String, List[String])],
      beta: Double,
      nbIter: Int,
      numParts: Int,
      st: StorageLevel
  ): Unit = {
    import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
    val sc = spark.sparkContext
    val P = new HashPartitioner(if (numParts > 0) numParts else sc.defaultParallelism * 2)

    // Ensemble des nœuds = src ∪ dest
    val sources  = parsed.keys
    val dests    = parsed.flatMap { case (_, outs) => outs }
    val allNodes = (sources union dests).distinct().persist(st)

    // (id, (outdeg, outs[])) sur le même partitioner
    val outMap: RDD[(String, (Int, Array[String]))] = parsed
      .reduceByKey(P, _ ++ _)            // merge local, on dédupl déjà en amont
      .mapValues(_.distinct.toArray)     // sécurité: dédup global (léger sur petits outs)
      .mapValues(arr => (arr.length, arr))
      .partitionBy(P)
      .persist(st)

    // Tous les nœuds doivent exister dans la table d’adjacence (dangling = outdeg 0)
    val nodesWithEmpty: RDD[(String, (Int, Array[String]))] =
      allNodes.map(id => (id, (0, Array.empty[String])))
        .partitionBy(P)
        .leftOuterJoin(outMap)
        .mapValues {
          case ((_, _), Some((deg, outs))) => (deg, outs)
          case _                           => (0, Array.empty[String])
        }
        .persist(st)

    val n = allNodes.count().toDouble
    var ranks = allNodes.map(id => (id, 1.0 / n)).partitionBy(P).persist(st)

    (1 to nbIter).foreach { _ =>
      // masse des dangling
      val danglingMass = nodesWithEmpty.join(ranks)
        .filter { case (_, ((deg, _), _)) => deg == 0 }
        .map { case (_, ((_, _), r)) => r }
        .sum()

      // contributions
      val contribs = nodesWithEmpty.join(ranks)
        .flatMap { case (_, ((deg, outs), r)) =>
          if (deg == 0) Iterator.empty else outs.iterator.map(d => (d, r / deg))
        }
        .reduceByKey(P, _ + _) // garde P pour le join suivant

      val base = ((1.0 - beta) / n) + (beta * danglingMass / n)

      // On construit un RDD “base” conservant le partitioner
      val baseRDD = ranks.mapValues(_ => base)

      val newRanks = baseRDD
        .leftOuterJoin(contribs)
        .mapValues { case (b, cOpt) => b + beta * cOpt.getOrElse(0.0) }
        .partitionBy(P)
        .persist(st)

      val sumRanks = newRanks.values.sum()
      ranks.unpersist(false)
      ranks = newRanks.mapValues(_ / sumRanks).partitionBy(P).persist(st)
    }

    // matérialisation (et éventuellement export)
    val _ = ranks.count()
    if (_currentExportTopN > 0 && _currentExportDir.nonEmpty) {
      exportTopNRDD(
        spark, ranks,
        _currentDataset, "rdd_partitioned",
        _currentBeta, _currentIters,
        _currentExportTopN, _currentExportDir
      )
    }

    nodesWithEmpty.unpersist(false); allNodes.unpersist(false)
  }


  // ===== PageRank DataFrame =====
  // edges: DataFrame(src, dest) avec dest distinct par src
  // st: StorageLevel pour les persistences
  // Approche DataFrame
  // On gère les dangling nodes en redistribuant leur masse uniformément
  // à chaque itération
  // On normalise les rangs à chaque itération pour éviter la dérive  
  def pageRankDF(
      spark: SparkSession,
      edges: DataFrame,
      beta: Double,
      nbIter: Int,
      numParts: Int,
      st: StorageLevel
  ): Unit = {
    import org.apache.spark.sql.functions._
    val p = if (numParts > 0) numParts else spark.sparkContext.defaultParallelism * 2

    // Fixe les partitions une fois pour toutes
    val E = edges.select(col("src"), col("dest"))
      .distinct()
      .repartition(p, col("src"))
      .persist(st)

    // Ensemble des nœuds
    val nodes = E.select(col("src").as("id"))
      .union(E.select(col("dest").as("id")))
      .distinct()
      .repartition(p, col("id"))
      .persist(st)

    // Outdegree (par src)
    val outdeg = E.groupBy(col("src")).agg(count(lit(1)).as("outdeg"))
      .withColumnRenamed("src", "id")
      .repartition(p, col("id"))
      .persist(st)

    val nodesWithDeg = nodes
      .join(outdeg, Seq("id"), "left")
      .na.fill(0, Seq("outdeg"))
      .persist(st)

    val n = nodes.count().toDouble
    var ranks = nodes
      .withColumn("rank", lit(1.0 / n))
      .persist(st)

    // Pour réduire la lignée au fil des itérations
    ranks = ranks.localCheckpoint(eager = true)

    val danglingIds = nodesWithDeg
      .filter(col("outdeg") === 0)
      .select(col("id"))
      .repartition(p, col("id"))
      .persist(st)

    danglingIds.count() // matérialise

    (1 to nbIter).foreach { _ =>
      // Masse des dangling (outdeg = 0)
      val dm = ranks
        .join(danglingIds, Seq("id"), "left_semi")
        .agg(sum(col("rank")).as("dm"))
        .select(coalesce(col("dm"), lit(0.0)).as("dm"))
        .first()

      val danglingMass = dm.getDouble(0)

      // Contributions (une seule passe groupBy dest)
      // E: déjà repartitionné par "src" et persisté
      // outdeg: seulement les src avec sorties
      val outdegPos = outdeg.filter(col("outdeg") > 0)
        .repartition(p, col("id")) // aligner sur id=src pour le join
        .withColumnRenamed("id", "src")
        .persist(st)

      // Contributions: 2 joins sur "src" puis 1 groupBy "dest"
      val contribs = E
        .join(ranks.select(col("id").as("src"), col("rank").as("rank_src")), Seq("src"), "inner")
        .join(outdegPos, Seq("src"), "inner")
        .withColumn("contrib", col("rank_src") / col("outdeg"))
        .groupBy(col("dest").as("id"))
        .agg(sum(col("contrib")).as("sumContrib"))
        .repartition(p, col("id"))   // pour aligner la prochaine jointure sur "id"
        .persist(st)

      val base = ((1.0 - beta) / n) + (beta * danglingMass / n)

      val newRanks = nodes
        .join(contribs, Seq("id"), "left")
        .select(
          col("id"),
          (lit(base) + lit(beta) * coalesce(col("sumContrib"), lit(0.0))).as("rank")
        )
        .persist(st)

      // Normalisation pour éviter la dérive numérique
      val sumRanks = newRanks.agg(sum(col("rank")).as("s"))
        .collect()(0).getAs[Double]("s")

      val newRanksNorm = newRanks
        .withColumn("rank", col("rank") / lit(sumRanks))
        .persist(st)
        .localCheckpoint(eager = true)

      contribs.unpersist(false)
      ranks.unpersist(false)
      ranks = newRanksNorm
    }

    // Matérialise le résultat
    val _ = ranks.count()

    // Export éventuel
    if (_currentExportTopN > 0 && _currentExportDir.nonEmpty) {
      exportTopNDF(
        spark,
        ranks,                       // DataFrame(id, rank)
        _currentDataset,
        "dataframe",
        _currentBeta,
        _currentIters,
        _currentExportTopN,
        _currentExportDir
      )
    }

    // Nettoyage
    nodes.unpersist(false)
    outdeg.unpersist(false)
    nodesWithDeg.unpersist(false)
    E.unpersist(false)
  }




  // ===== Run d’un algo + mesure =====
  private def runOne(
      spark: SparkSession,
      a: Args,
      approach: String
  ): BenchRecord = {
    // --- CONTEXT pour l’export TopN (doit être posé AVANT d’appeler les algos)
    _currentDataset    = a.dataset
    _currentBeta       = a.beta
    _currentIters      = a.iters
    _currentExportTopN = a.exportTopN
    _currentExportDir  = a.exportDir

    val st = storageLevelOf(a.storage)
    val startParse = nowNs()

    approach match {
      case "dataframe" | "df" =>
        val parts = if (a.partsDF > 0) a.partsDF else spark.sparkContext.defaultParallelism * 2
        val (edges, nNodes, nEdges) = parseDF(spark, a.input, a.sepRegex, parts, st)
        val parseMs = elapsedMs(startParse)
        val ((), peak) = withMemPeak(spark) {
          pageRankDF(spark, edges, a.beta, a.iters, parts, st)
          edges.unpersist(); ()
        }
        val computeMs = elapsedMs(startParse) - parseMs
        BenchRecord(a.dataset, "dataframe", nNodes, nEdges, parseMs, computeMs, parseMs + computeMs, peak)

      case "rdd_partitioned" =>
        val parts = if (a.partsRDD > 0) a.partsRDD else spark.sparkContext.defaultParallelism * 2
        val (parsed, nNodes, nEdges) = parseRDD(spark, a.input, a.sepRegex, st)
        val parseMs = elapsedMs(startParse)
        val ((), peak) = withMemPeak(spark) {
          pageRankRDDPartitioned(spark, parsed, a.beta, a.iters, parts, st)
          parsed.unpersist(false); ()
        }
        val computeMs = elapsedMs(startParse) - parseMs
        BenchRecord(a.dataset, "rdd_partitioned", nNodes, nEdges, parseMs, computeMs, parseMs + computeMs, peak)

      case "rdd" =>
        val (parsed, nNodes, nEdges) = parseRDD(spark, a.input, a.sepRegex, st)
        val parseMs = elapsedMs(startParse)
        val ((), peak) = withMemPeak(spark) {
          pageRankRDD(spark, parsed, a.beta, a.iters, st)
          parsed.unpersist(false); ()
        }
        val computeMs = elapsedMs(startParse) - parseMs
        BenchRecord(a.dataset, "rdd", nNodes, nEdges, parseMs, computeMs, parseMs + computeMs, peak)
    }
  }


  // ===== Ecriture/append résultats =====
  private def saveRecord(
      spark: SparkSession,
      rec: BenchRecord,
      outPath: String,
      format: String,
      append: Boolean
  ): Unit = {
    import spark.implicits._
    val df = Seq(rec).toDF()
    format.toLowerCase() match {
      case "parquet" =>
        df.write.mode(if (append) "append" else "overwrite").parquet(outPath)
      case _ =>
        // CSV "append": si le répertoire existe, on ajoute sans header; sinon avec header
        val exists = Files.exists(Paths.get(outPath))
        val writer = df.coalesce(1).write
          .mode(if (append) "append" else "overwrite")
          .option("header", (!exists || !append).toString)
        writer.csv(outPath)
    }
  }

  // ===== Parsing Args minimaliste =====
  private def parseArgs(args: Array[String]): Args = {
    def next(i: Int): String = if (i + 1 < args.length) args(i + 1) else ""
    var a = Args()
    var i = 0
    while (i < args.length) {
      args(i) match {
        case "--algo"        => a = a.copy(algo = next(i)); i += 1
        case "--input"       => a = a.copy(input = next(i)); i += 1
        case "--dataset"     => a = a.copy(dataset = next(i)); i += 1
        case "--beta"        => a = a.copy(beta = next(i).toDouble); i += 1
        case "--iters"       => a = a.copy(iters = next(i).toInt); i += 1
        case "--sep"         => a = a.copy(sepRegex = next(i)); i += 1
        case "--partsDF"     => a = a.copy(partsDF = next(i).toInt); i += 1
        case "--partsRDD"    => a = a.copy(partsRDD = next(i).toInt); i += 1
        case "--out"         => a = a.copy(outCsv = next(i)); i += 1
        case "--outFormat"   => a = a.copy(outFormat = next(i)); i += 1
        case "--storage"     => a = a.copy(storage = next(i)); i += 1
        case "--exportTopN" => a = a.copy(exportTopN = next(i).toInt); i += 1
        case "--exportDir"  => a = a.copy(exportDir  = next(i)); i += 1
        case "--no-append"   => a = a.copy(append = false)
        case "--quiet"       => a = a.copy(verbose = false)
        case "--help" | "-h" =>
          println(
            s"""
               |Usage: PageRankBench [options]
               |
               |  --algo rdd|rddp|df|all
               |  --input <path>                 (fichier ou dossier)
               |  --dataset <name>
               |  --beta <double>                (default 0.85)
               |  --iters <int>                  (default 5)
               |  --sep <regex>                  (default \\|)
               |  --partsDF <int>                (default 2*parallelism)
               |  --partsRDD <int>               (default 2*parallelism)
               |  --storage <StorageLevel>       MEMORY_ONLY|MEMORY_AND_DISK|...
               |  --out <path>                   (default pagerank_bench_results)
               |  --outFormat csv|parquet        (default csv)
               |  --no-append                    (overwrite cible)
               |  --quiet
               |""".stripMargin)
          sys.exit(0)
        case other =>
          System.err.println(s"Ignored arg: $other")
      }
      i += 1
    }
    a
  }

  def main(args: Array[String]): Unit = {
    val a = parseArgs(args)
    require(a.input.nonEmpty, "--input est requis (fichier ou dossier)")

    val spark = SparkSession.builder()
      .appName("PageRankBench")
      .config("spark.sql.adaptive.enabled", "true")
      .getOrCreate()

    if (a.verbose) {
      println(s"Spark defaultParallelism = ${spark.sparkContext.defaultParallelism}")
      println(s"Args: $a")
    }

    val algos: Seq[String] = a.algo.toLowerCase match {
      case "all" => Seq("rdd", "rdd_partitioned", "dataframe")
      case "rddp" => Seq("rdd_partitioned")
      case "df"   => Seq("dataframe")
      case x => Seq(x)
    }

    algos.foreach { ap =>
      if (a.verbose) println(s"=== RUN ${ap.toUpperCase} | dataset=${a.dataset} | input=${a.input} ===")
      val rec = runOne(spark, a, ap)
      if (a.verbose) println(s"RESULT: $rec")
      saveRecord(spark, rec, a.outCsv, a.outFormat, a.append)
    }

    if (a.verbose) {
      println(s"Résultats cumulés dans: ${a.outCsv} (${a.outFormat})")
    }
    spark.stop()
  }
}

