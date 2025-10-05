package utils

import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, Suite}

/**
 * Trait utilitaire pour partager un SparkSession entre les tests.
 * Chaque suite qui hérite de SparkTestSession dispose d'un SparkSession unique
 * et propre, automatiquement arrêté à la fin de la suite.
 */
trait SparkTestSession extends BeforeAndAfterAll { this: Suite =>

    // SparkSession partagé par tous les tests d'une même suite
    @transient lazy val spark: SparkSession = SparkSession.builder()
        .appName("PageRankTest")
        .master("local[*]")
        .getOrCreate()

    // Arrêt du SparkSession après tous les tests de la suite
    override def afterAll(): Unit = {
        try {
        spark.stop()
        } finally {
        super.afterAll()
        }
    }
}