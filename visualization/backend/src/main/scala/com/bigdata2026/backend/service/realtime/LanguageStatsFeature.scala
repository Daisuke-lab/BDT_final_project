package com.bigdata2026.backend.service.realtime

import com.bigdata2026.backend.service.Feature
import com.bigdata2026.common.model.LanguageStats
import com.bigdata2026.common.ws.ServerMsg
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{ConnectionFactory, Scan}
import org.apache.hadoop.hbase.util.Bytes
import zio.*
import zio.stream.ZStream

final class LanguageStatsFeature extends Feature:

  def snapshot: Task[ServerMsg] =
    readAll.map(langs => ServerMsg.LanguageStatsSnapshot(langs): ServerMsg)

  def liveUpdates: ZStream[Any, Throwable, ServerMsg] =
    ZStream
      .fromSchedule(Schedule.fixed(3.seconds))
      .mapZIO(_ => readAll)
      .map(langs => ServerMsg.LanguageStatsSnapshot(langs): ServerMsg)

  private def readAll: Task[List[LanguageStats]] =
    ZIO.attemptBlocking {
      val conf = HBaseConfiguration.create()
      conf.set("hbase.zookeeper.quorum",
        sys.env.getOrElse("HBASE_ZOOKEEPER_QUORUM", "localhost"))
      conf.set("hbase.zookeeper.property.clientPort",
        sys.env.getOrElse("HBASE_ZOOKEEPER_PORT", "2182"))
      val conn  = ConnectionFactory.createConnection(conf)
      try {
        val admin = conn.getAdmin
        val name  = TableName.valueOf("language_stats")
        if (!admin.tableExists(name)) { admin.close(); return ZIO.succeed(Nil) }
        admin.close()
        val table   = conn.getTable(name)
        val scanner = table.getScanner(new Scan())
        try parseRows(scanner)
        finally { scanner.close(); table.close() }
      } finally conn.close()
    }.orElse(ZIO.succeed(Nil))

  private val CF = Bytes.toBytes("cf")

  private def parseRows(scanner: org.apache.hadoop.hbase.client.ResultScanner): List[LanguageStats] =
    def str(row: org.apache.hadoop.hbase.client.Result, col: String): String =
      val b = row.getValue(CF, Bytes.toBytes(col))
      if b == null then "" else Bytes.toString(b)
    def lng(row: org.apache.hadoop.hbase.client.Result, col: String): Long =
      val s = str(row, col)
      if s.isEmpty then 0L else s.toLong
    Iterator.continually(scanner.next())
      .takeWhile(_ != null)
      .map { row =>
        LanguageStats(
          language      = Bytes.toString(row.getRow),
          tier          = str(row, "tier"),
          paradigm      = str(row, "paradigm"),
          langType      = str(row, "lang_type"),
          totalStars    = lng(row, "total_stars"),
          totalForks    = lng(row, "total_forks"),
          totalPushes   = lng(row, "total_pushes"),
          totalActivity = lng(row, "total_activity"),
          repoCount     = lng(row, "repo_count"),
        )
      }
      .toList

object LanguageStatsFeature:
  val live: ULayer[LanguageStatsFeature] = ZLayer.succeed(new LanguageStatsFeature)
