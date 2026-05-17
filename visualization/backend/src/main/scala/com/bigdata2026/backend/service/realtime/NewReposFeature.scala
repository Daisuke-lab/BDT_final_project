package com.bigdata2026.backend.service.realtime

import com.bigdata2026.backend.service.Feature
import com.bigdata2026.common.model.NewRepoWindow
import com.bigdata2026.common.ws.ServerMsg
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{ConnectionFactory, Scan}
import org.apache.hadoop.hbase.util.Bytes
import zio.*
import zio.stream.ZStream

final class NewReposFeature extends Feature:

  def snapshot: Task[ServerMsg] =
    ZIO.attemptBlocking {
      withTable { table =>
        val scanner = table.getScanner(new Scan())
        try parseRows(scanner).sortBy(_.windowStart).reverse.take(24)
        finally scanner.close()
      }
    }.map(ws => ServerMsg.Snapshot(ws): ServerMsg)

  def liveUpdates: ZStream[Any, Throwable, ServerMsg] =
    ZStream.fromZIO(currentMaxTs).flatMap { initSince =>
      ZStream
        .fromSchedule(Schedule.fixed(30.seconds))
        .mapAccumZIO(initSince) { (lastSeen, _) =>
          ZIO.attemptBlocking(readSince(lastSeen)).map { rows =>
            val newMax = rows.map(_.windowStart).maxOption.getOrElse(lastSeen)
            (newMax, rows.sortBy(_.windowStart))
          }
        }
        .flatMap(rows => ZStream.fromIterable(rows.map(w => ServerMsg.NewWindow(w): ServerMsg)))
    }

  private def currentMaxTs: Task[Long] =
    ZIO.attemptBlocking {
      withTable { table =>
        val scanner = table.getScanner(new Scan())
        try parseRows(scanner).map(_.windowStart).maxOption.getOrElse(0L)
        finally scanner.close()
      }
    }.orElse(ZIO.succeed(0L))

  private def readSince(since: Long): List[NewRepoWindow] =
    withTable { table =>
      val scan =
        if since == 0L then new Scan()
        else new Scan().withStartRow(Bytes.toBytes(f"${since + 1}%016d"))
      val scanner = table.getScanner(scan)
      try parseRows(scanner)
      finally scanner.close()
    }

  private def withTable[A](f: org.apache.hadoop.hbase.client.Table => A): A =
    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.quorum",
      sys.env.getOrElse("HBASE_ZOOKEEPER_QUORUM", "localhost"))
    conf.set("hbase.zookeeper.property.clientPort",
      sys.env.getOrElse("HBASE_ZOOKEEPER_PORT", "2182"))
    val conn  = ConnectionFactory.createConnection(conf)
    val table = conn.getTable(TableName.valueOf("new_repos_per_window"))
    try f(table)
    finally { table.close(); conn.close() }

  private def parseRows(scanner: org.apache.hadoop.hbase.client.ResultScanner): List[NewRepoWindow] =
    Iterator.continually(scanner.next())
      .takeWhile(_ != null)
      .map { row =>
        val CF          = Bytes.toBytes("cf")
        val count       = Bytes.toString(row.getValue(CF, Bytes.toBytes("count"))).toLong
        val windowStart = Bytes.toString(row.getValue(CF, Bytes.toBytes("window_start"))).toLong
        val windowEnd   = Bytes.toString(row.getValue(CF, Bytes.toBytes("window_end"))).toLong
        NewRepoWindow(windowStart, windowEnd, count)
      }
      .toList

object NewReposFeature:
  val live: ULayer[NewReposFeature] = ZLayer.succeed(new NewReposFeature)
