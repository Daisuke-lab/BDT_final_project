package com.bigdata2026.backend.service.realtime

import com.bigdata2026.backend.service.Feature
import com.bigdata2026.common.model.RepoStats
import com.bigdata2026.common.ws.ServerMsg
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{ConnectionFactory, Scan}
import org.apache.hadoop.hbase.util.Bytes
import zio.*
import zio.stream.ZStream

final class RepoStatsFeature extends Feature:

  def snapshot: Task[ServerMsg] =
    ZIO.attemptBlocking {
      withTable { table =>
        val scanner = table.getScanner(new Scan().setLimit(RepoStatsFeature.ScanLimit))
        try parseRows(scanner)
        finally scanner.close()
      }
    }.map(repos => ServerMsg.RepoStatsSnapshot(repos): ServerMsg)

  def liveUpdates: ZStream[Any, Throwable, ServerMsg] =
    ZStream.fromZIO(currentMaxActivity).flatMap { initSince =>
      ZStream
        .fromSchedule(Schedule.fixed(5.seconds))
        .mapAccumZIO(initSince) { (watermark, _) =>
          ZIO.attemptBlocking {
            withTable { table =>
              val scanner = table.getScanner(new Scan().setLimit(RepoStatsFeature.ScanLimit))
              val repos   = try parseRows(scanner) finally scanner.close()
              val newMax  = repos.map(_.activityCount).maxOption.getOrElse(watermark)
              (newMax, if (newMax > watermark) repos else Nil)
            }
          }
        }
        .flatMap { repos =>
          if repos.isEmpty then ZStream.empty
          else ZStream.succeed(ServerMsg.RepoStatsUpdated(repos): ServerMsg)
        }
    }

  private def currentMaxActivity: Task[Long] =
    ZIO.attemptBlocking {
      withTable { table =>
        val scanner = table.getScanner(new Scan().setLimit(RepoStatsFeature.ScanLimit))
        try parseRows(scanner).map(_.activityCount).maxOption.getOrElse(0L)
        finally scanner.close()
      }
    }.orElse(ZIO.succeed(0L))

  private val CF = Bytes.toBytes("cf")

  private def parseRows(scanner: org.apache.hadoop.hbase.client.ResultScanner): List[RepoStats] =
    def getLong(row: org.apache.hadoop.hbase.client.Result, col: String): Long =
      val bytes = row.getValue(CF, Bytes.toBytes(col))
      if bytes == null then 0L else Bytes.toString(bytes).toLong
    Iterator.continually(scanner.next())
      .takeWhile(_ != null)
      .map { row =>
        RepoStats(
          repoName         = Bytes.toString(row.getRow),
          starCount        = getLong(row, "star_count"),
          forkCount        = getLong(row, "fork_count"),
          pushCount        = getLong(row, "push_count"),
          pushWeight       = getLong(row, "push_weight"),
          activityCount    = getLong(row, "activity_count"),
          contributorCount = getLong(row, "contributor_count"),
        )
      }
      .toList
      .sortBy(_.activityCount).reverse.take(100)

  private def withTable[A](f: org.apache.hadoop.hbase.client.Table => A): A =
    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.quorum",
      sys.env.getOrElse("HBASE_ZOOKEEPER_QUORUM", "localhost"))
    conf.set("hbase.zookeeper.property.clientPort",
      sys.env.getOrElse("HBASE_ZOOKEEPER_PORT", "2182"))
    val conn  = ConnectionFactory.createConnection(conf)
    val table = conn.getTable(TableName.valueOf("repo_stats"))
    try f(table)
    finally { table.close(); conn.close() }

object RepoStatsFeature:
  val live: ULayer[RepoStatsFeature] = ZLayer.succeed(new RepoStatsFeature)
  private[realtime] val ScanLimit = 500
