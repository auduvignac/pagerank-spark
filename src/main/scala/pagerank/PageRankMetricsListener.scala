package pagerank

import org.apache.spark.scheduler._
import org.apache.log4j.Logger
import java.io.{BufferedWriter, File, FileWriter}
import scala.collection.mutable

/**
 * SparkListener pour tracer :
 *  - jobGroup (ex. PageRank-RDD-iter-3)
 *  - stageId, name
 *  - durationSec (calculée via submission/completion)
 *  - shuffleReadMB, shuffleWriteMB
 *  - gcSec, execSec
 * + Totaux imprimables via printTotals()
 */
class PageRankMetricsListener(logger: Logger, csvPath: Option[String] = None)
  extends SparkListener {

  private case class StageData(
      jobGroup: String,
      name: String,
      durationSec: Double,
      shuffleReadMB: Double,
      shuffleWriteMB: Double,
      gcSec: Double,
      execSec: Double
  )

  private val stageBuffer = mutable.ListBuffer[StageData]()
  private val stageToJobGroup = mutable.HashMap[Int, String]()

  // Totaux pour printTotals()
  private var totalDurationMs: Long = 0L
  private var totalShuffleRead: Long = 0L
  private var totalShuffleWrite: Long = 0L
  private var totalGCTime: Long = 0L
  private var totalExecutorRunTime: Long = 0L

  // CSV writer (création dossier + header si vide)
  private val writer: Option[BufferedWriter] = csvPath.map { path =>
    val file = new File(path)
    val parentDir = file.getParentFile
    if (parentDir != null && !parentDir.exists()) parentDir.mkdirs()
    if (!file.exists()) file.createNewFile()
    val w = new BufferedWriter(new FileWriter(file, /*append*/ true))
    if (file.length() == 0)
      w.write("jobGroup,stageId,name,durationSec,shuffleReadMB,shuffleWriteMB,gcSec,execSec\n")
    w
  }

  // Associe jobGroup -> stages
  override def onJobStart(jobStart: SparkListenerJobStart): Unit = {
    val jobGroup = Option(jobStart.properties)
      .flatMap(p => Option(p.getProperty("spark.jobGroup.id")))
      .getOrElse("unknown")
    jobStart.stageIds.foreach { sid => stageToJobGroup(sid) = jobGroup }
    logger.info(s"[Metrics] Job start: jobId=${jobStart.jobId}, group=$jobGroup, stages=${jobStart.stageIds.mkString(",")}")
  }

  // À la fin d’un stage : log + CSV + cumuls
  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = {
    val info = stageCompleted.stageInfo
    val metrics = info.taskMetrics
    if (metrics == null) return

    // Durée via timestamps (Spark 3.x)
    val durationMs: Long = (for {
      start <- info.submissionTime
      end   <- info.completionTime
    } yield (end - start)).getOrElse(0L)

    // Shuffle (read/write) – objets non Option en 3.x (mais prudence null)
    val shuffleReadBytes: Long =
      Option(metrics.shuffleReadMetrics).map(m => m.remoteBytesRead + m.localBytesRead).getOrElse(0L)
    val shuffleWriteBytes: Long =
      Option(metrics.shuffleWriteMetrics).map(_.bytesWritten).getOrElse(0L)

    val jobGroup = stageToJobGroup.getOrElse(info.stageId, "unknown")

    val data = StageData(
      jobGroup = jobGroup,
      name = info.name,
      durationSec = durationMs / 1000.0,
      shuffleReadMB = shuffleReadBytes / (1024.0 * 1024.0),
      shuffleWriteMB = shuffleWriteBytes / (1024.0 * 1024.0),
      gcSec = metrics.jvmGCTime / 1000.0,
      execSec = metrics.executorRunTime / 1000.0
    )

    stageBuffer += data

    // Cumuls globaux
    totalDurationMs        += durationMs
    totalShuffleRead       += shuffleReadBytes
    totalShuffleWrite      += shuffleWriteBytes
    totalGCTime            += metrics.jvmGCTime
    totalExecutorRunTime   += metrics.executorRunTime

    // CSV
    writer.foreach { w =>
      w.write(f"${data.jobGroup},${info.stageId},${data.name},${data.durationSec}%.3f,${data.shuffleReadMB}%.3f,${data.shuffleWriteMB}%.3f,${data.gcSec}%.3f,${data.execSec}%.3f\n")
      w.flush()
    }

    // Log
    logger.info(
      f"[Metrics] [${data.jobGroup}] Stage ${info.stageId} '${data.name}': " +
      f"dur=${data.durationSec}%.1fs, shuffleR=${data.shuffleReadMB}%.1fMB, shuffleW=${data.shuffleWriteMB}%.1fMB, " +
      f"gc=${data.gcSec}%.2fs, exec=${data.execSec}%.2fs"
    )
  }

  /** Appelle ceci à la fin de l’application (ou laisse onApplicationEnd s’en charger). */
  def printTotals(): Unit = {
    logger.info(
      f"=== [Totals] Duration=${totalDurationMs/1000.0}%.1fs " +
      f"ShuffleRead=${totalShuffleRead/1e6}%.1fMB ShuffleWrite=${totalShuffleWrite/1e6}%.1fMB " +
      f"GC=${totalGCTime/1000.0}%.1fs ExecutorTime=${totalExecutorRunTime/1000.0}%.1fs ==="
    )
    writer.foreach(_.flush())
  }

  override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = {
    printTotals()                // imprime aussi à la fin
    writer.foreach(_.close())
    logger.info("[Metrics] Application finished. Metrics written.")
  }
}
