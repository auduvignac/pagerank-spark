name := "PageRankSpark"

version := "0.1"

scalaVersion := "2.12.18"   // Spark 3.5 supporte Scala 2.12 et 2.13

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.5.0",
  "org.apache.spark" %% "spark-sql"  % "3.5.0",
  "org.scalatest"    %% "scalatest"  % "3.2.18" % Test
)
