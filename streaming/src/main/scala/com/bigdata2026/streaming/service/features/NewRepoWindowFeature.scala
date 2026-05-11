package com.bigdata2026.streaming.service.features

import com.bigdata2026.streaming.config.HBaseConfig
import com.bigdata2026.streaming.model.GitHubEvent
import com.bigdata2026.streaming.service.StreamFeature
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.{ColumnFamilyDescriptorBuilder, ConnectionFactory, Put, TableDescriptorBuilder}
import org.apache.hadoop.hbase.util.Bytes
import zio._
import zio.stream.ZStream

import java.time.Instant
import java.util.concurrent.TimeUnit
import scala.util.Try

final class NewRepoWindowFeature(hbaseConfig: HBaseConfig) extends StreamFeature {
  val name = "new-repos-window"

  private val TableName_ = "new_repos_per_window"
  private val WindowMs   = 5 * 60 * 1000L
  private val GraceMs    = 60 * 1000L
  private val FlushEvery = 30.seconds
  private val CF         = Bytes.toBytes("cf")

  def process(events: ZStream[Any, Nothing, GitHubEvent]): Task[Unit] =
    for {
      _     <- ensureTableExists
      state <- Ref.make(Map.empty[Long, Long])
      _     <- ZStream.fromSchedule(Schedule.fixed(FlushEvery))
                 .mapZIO(_ => Clock.currentTime(TimeUnit.MILLISECONDS).flatMap(flushCompleted(state, _)))
                 .runDrain
                 .forkDaemon
      _     <- events
                 .filter(e => e.eventType == "CreateEvent" && e.refType == "repository")
                 .foreach { event =>
                   val ws = windowStart(event.createdAt)
                   state.update(m => m.updated(ws, m.getOrElse(ws, 0L) + 1))
                 }
    } yield ()

  private def ensureTableExists: Task[Unit] =
    ZIO.attemptBlocking {
      val conn  = ConnectionFactory.createConnection(hbaseConf)
      val admin = conn.getAdmin
      try {
        val tn = TableName.valueOf(TableName_)
        if (!admin.tableExists(tn))
          admin.createTable(
            TableDescriptorBuilder.newBuilder(tn)
              .setColumnFamily(ColumnFamilyDescriptorBuilder.of(CF))
              .build()
          )
      } finally { admin.close(); conn.close() }
    }.tapError(e => ZIO.logWarning(s"[$name] Could not ensure HBase table: ${e.getMessage}")).ignore

  private def flushCompleted(state: Ref[Map[Long, Long]], now: Long): Task[Unit] =
    for {
      snapshot  <- state.get
      completed  = snapshot.filter { case (ws, _) => ws + WindowMs + GraceMs <= now }
      _         <- ZIO.foreachDiscard(completed.toList) { case (ws, count) =>
                     writeWindow(ws, ws + WindowMs, count)
                       .tapError(e => ZIO.logError(s"[$name] HBase write failed: ${e.getMessage}"))
                       .ignore
                   }
      _         <- ZIO.when(completed.nonEmpty)(state.update(_ -- completed.keySet))
      _         <- ZIO.when(completed.nonEmpty)(
                     ZIO.logInfo(s"[$name] Flushed ${completed.size} window(s) to HBase")
                   )
    } yield ()

  private def windowStart(createdAt: String): Long = {
    val epochMs = Try(Instant.parse(createdAt).toEpochMilli).getOrElse(java.lang.System.currentTimeMillis())
    (epochMs / WindowMs) * WindowMs
  }

  private def writeWindow(ws: Long, we: Long, count: Long): Task[Unit] =
    ZIO.attemptBlocking {
      val conn  = ConnectionFactory.createConnection(hbaseConf)
      val table = conn.getTable(TableName.valueOf(TableName_))
      try {
        val put = new Put(Bytes.toBytes(f"$ws%016d"))
        put.addColumn(CF, Bytes.toBytes("count"),        Bytes.toBytes(count.toString))
        put.addColumn(CF, Bytes.toBytes("window_start"), Bytes.toBytes(ws.toString))
        put.addColumn(CF, Bytes.toBytes("window_end"),   Bytes.toBytes(we.toString))
        table.put(put)
      } finally { table.close(); conn.close() }
    }

  private def hbaseConf = {
    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.quorum",              hbaseConfig.zookeeperQuorum)
    conf.set("hbase.zookeeper.property.clientPort", hbaseConfig.zookeeperPort)
    conf
  }
}

object NewRepoWindowFeature {
  val live: URLayer[HBaseConfig, NewRepoWindowFeature] =
    ZLayer.fromZIO(ZIO.service[HBaseConfig].map(new NewRepoWindowFeature(_)))
}
