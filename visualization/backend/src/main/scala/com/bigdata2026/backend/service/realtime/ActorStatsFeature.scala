package com.bigdata2026.backend.service.realtime

import com.bigdata2026.backend.service.Feature
import com.bigdata2026.common.model.ActorStats
import com.bigdata2026.common.ws.ServerMsg
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{ConnectionFactory, Scan}
import org.apache.hadoop.hbase.util.Bytes
import zio.*
import zio.stream.ZStream

final class ActorStatsFeature extends Feature:

  def snapshot: Task[ServerMsg] =
    ZIO.attemptBlocking {
      withTable { table =>
        val scanner = table.getScanner(new Scan())
        try parseRows(scanner)
        finally scanner.close()
      }
    }.map(actors => ServerMsg.ActorStatsSnapshot(actors): ServerMsg)

  def liveUpdates: ZStream[Any, Throwable, ServerMsg] =
    ZStream.fromZIO(currentMaxActivity).flatMap { initSince =>
      ZStream
        .fromSchedule(Schedule.fixed(5.seconds))
        .mapAccumZIO(initSince) { (watermark, _) =>
          ZIO.attemptBlocking {
            withTable { table =>
              val scanner = table.getScanner(new Scan())
              val actors  = try parseRows(scanner) finally scanner.close()
              val newMax  = actors.map(_.activityCount).maxOption.getOrElse(watermark)
              (newMax, if (newMax > watermark) actors else Nil)
            }
          }
        }
        .flatMap { actors =>
          if actors.isEmpty then ZStream.empty
          else ZStream.succeed(ServerMsg.ActorStatsUpdated(actors): ServerMsg)
        }
    }

  private def currentMaxActivity: Task[Long] =
    ZIO.attemptBlocking {
      withTable { table =>
        val scanner = table.getScanner(new Scan())
        try parseRows(scanner).map(_.activityCount).maxOption.getOrElse(0L)
        finally scanner.close()
      }
    }.orElse(ZIO.succeed(0L))

  private val CF = Bytes.toBytes("cf")

  private def parseRows(scanner: org.apache.hadoop.hbase.client.ResultScanner): List[ActorStats] =
    Iterator.continually(scanner.next())
      .takeWhile(_ != null)
      .map { row =>
        val bytes = row.getValue(CF, Bytes.toBytes("activity_count"))
        ActorStats(
          actorLogin    = Bytes.toString(row.getRow),
          activityCount = if bytes == null then 0L else Bytes.toString(bytes).toLong,
        )
      }
      .toList

  private def withTable[A](f: org.apache.hadoop.hbase.client.Table => A): A =
    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.quorum",
      sys.env.getOrElse("HBASE_ZOOKEEPER_QUORUM", "localhost"))
    conf.set("hbase.zookeeper.property.clientPort",
      sys.env.getOrElse("HBASE_ZOOKEEPER_PORT", "2182"))
    val conn  = ConnectionFactory.createConnection(conf)
    val table = conn.getTable(TableName.valueOf("actor_stats"))
    try f(table)
    finally { table.close(); conn.close() }

object ActorStatsFeature:
  val live: ULayer[ActorStatsFeature] = ZLayer.succeed(new ActorStatsFeature)
