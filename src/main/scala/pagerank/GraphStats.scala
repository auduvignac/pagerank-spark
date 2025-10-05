package pagerank

import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD

object GraphStats {
  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      System.err.println("Usage: GraphStats <input>")
      System.exit(1)
    }

    val input = args(0)

    val spark = SparkSession.builder
      .appName("Graph Stats")
      .master("local[*]")
      .getOrCreate()

    val sc = spark.sparkContext

    // Lire le fichier
    val lines: RDD[String] = sc.textFile(input)

    // Extraire les arêtes
    val edges: RDD[(String, String)] = lines.flatMap { line =>
      val parts = line.split("\\|").map(_.trim)
      val src = parts.head
      val outs = if (parts.length > 1) parts.tail else Array.empty[String]
      outs.map(dest => (src, dest))
    }

    // Compter les noeuds distincts
    val nodes = edges.flatMap { case (src, dst) => Seq(src, dst) }.distinct().count()

    // Compter les arêtes
    val edgeCount = edges.count()

    println(s"$input,$nodes,$edgeCount")

    spark.stop()
  }
}