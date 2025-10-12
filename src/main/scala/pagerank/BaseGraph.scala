package pagerank

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import java.nio.file.Paths

/**
  * Classe de base : ne déclenche aucun chargement Spark.
  * Fournit juste le contexte commun.
  */
abstract class BaseGraph(
    val name: String,
    val path: String,
    val debug: Boolean = false
)(implicit val spark: SparkSession, val logger: Logger) {

  val fileName: String = Paths.get(path).getFileName.toString

  /** Chaque sous-classe doit fournir ses structures (RDD ou DF). */
  def describe(): Unit
}