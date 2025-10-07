package pagerank

import java.io.{File, BufferedWriter, FileWriter, PrintWriter}
import org.apache.log4j.Logger

object PageRankUtils {

  // =======================
  // Logger
  // =======================
  val logger: Logger = Logger.getLogger(getClass.getName)

  /**
    * Sauvegarde un historique PageRank au format CSV
    *
    * @param history   Historique sous forme d'une séquence de snapshots Map(node -> rank)
    * @param outputDir Répertoire dans lequel écrire le fichier CSV (ex: "output/wiki/")
    * @param logger    Logger pour affichage éventuel
    */
  def exportHistoryToCSV(
      history: Seq[Map[String, Double]],
      outputDir: String,
      logger: Logger
  ): Unit = {
    val outDir = new File(outputDir)
    if (!outDir.exists()) outDir.mkdirs()

    val outFile = new File(outDir, "history.csv")

    val writer = new PrintWriter(outFile)
    try {
      // Déterminer les nœuds à partir du premier snapshot
      val nodes = history.head.keys.toSeq.sorted

      // En-tête CSV
      writer.println("Iteration," + nodes.mkString(","))

      // Données par itération
      for ((snapshot, i) <- history.zipWithIndex) {
        val line = nodes.map(n => snapshot.getOrElse(n, 0.0))
        writer.println(s"$i," + line.mkString(","))
      }

      logger.info(s"==== Historique PageRank exporté vers ${outFile.getAbsolutePath} ====")
    } finally {
      writer.close()
    }
  }

  def appendSnapshot(
      history: Option[scala.collection.mutable.ArrayBuffer[Map[String, Double]]],
      rdd: org.apache.spark.rdd.RDD[(String, Double)]
  ): Unit =
    history.foreach(_.append(rdd.collect().toMap))

  def appendSnapshot(
      history: Option[scala.collection.mutable.ArrayBuffer[Map[String, Double]]],
      df: org.apache.spark.sql.DataFrame
  ): Unit =
    history.foreach(_.append(df.collect().map(r => r.getString(0) -> r.getDouble(1)).toMap))

  /** Ajoute un résultat dans le fichier benchmark.csv */
  def appendBenchmark(
      method: String,
      graph: String,
      iterations: Int,
      nodes: Long,
      edges: Long,
      time: Double,
      outputDir: String
  ): Unit = {
    val file = new File(s"$outputDir/benchmark.csv")
    val header = "method;graph;iter;nodes;edges;time\n"
    val line   = f"$method;$graph;$iterations;$nodes;$edges;$time%.2f\n"

    val outDir = new File(outputDir)
    if (!outDir.exists()) outDir.mkdirs()

    val isNewFile = !file.exists()
    val writer = new BufferedWriter(new FileWriter(file, true))
    try {
      if (isNewFile) writer.write(header)
      writer.write(line)
    } finally {
      writer.close()
    }
    logger.info(f"==== Fichier benchmark exporté : $file ====")
  }

}