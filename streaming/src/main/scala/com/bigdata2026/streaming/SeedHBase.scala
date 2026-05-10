package com.bigdata2026.streaming

import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes
import java.time.Instant
import java.time.temporal.ChronoUnit
import scala.util.Random

object SeedHBase {
  private val TableStr = "new_repos_per_window"
  private val CF       = Bytes.toBytes("cf")

  def main(args: Array[String]): Unit = {
    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.quorum",
      sys.env.getOrElse("HBASE_ZOOKEEPER_QUORUM", "localhost"))
    conf.set("hbase.zookeeper.property.clientPort",
      sys.env.getOrElse("HBASE_ZOOKEEPER_PORT", "2182"))
    conf.set("hbase.client.retries.number",      "3")
    conf.set("hbase.client.operation.timeout",   "10000")
    conf.set("hbase.rpc.timeout",                "10000")
    conf.set("zookeeper.session.timeout",        "10000")
    conf.set("zookeeper.recovery.retry",         "1")

    println(s"[seeder] Connecting to HBase at ${conf.get("hbase.zookeeper.quorum")}:${conf.get("hbase.zookeeper.property.clientPort")}...")
    val conn  = ConnectionFactory.createConnection(conf)
    val admin = conn.getAdmin
    try {
      val name = TableName.valueOf(TableStr)
      if (!admin.tableExists(name)) {
        admin.createTable(
          TableDescriptorBuilder.newBuilder(name)
            .setColumnFamily(ColumnFamilyDescriptorBuilder.of(CF))
            .build()
        )
        println(s"[seeder] Created table '$TableStr'")
      }
      val table = conn.getTable(name)
      try {
        val rng = new Random(42)
        val now = Instant.now().truncatedTo(ChronoUnit.MINUTES)
        val puts = (0 until 24).map { i =>
          val wEnd   = now.minus((23 - i) * 5L, ChronoUnit.MINUTES)
          val wStart = wEnd.minus(5L, ChronoUnit.MINUTES)
          val count  = 5 + rng.nextInt(46)
          val rowKey = f"${wStart.toEpochMilli}%016d"
          val put    = new Put(Bytes.toBytes(rowKey))
          put.addColumn(CF, Bytes.toBytes("count"),        Bytes.toBytes(count.toString))
          put.addColumn(CF, Bytes.toBytes("window_start"), Bytes.toBytes(wStart.toEpochMilli.toString))
          put.addColumn(CF, Bytes.toBytes("window_end"),   Bytes.toBytes(wEnd.toEpochMilli.toString))
          println(s"[seeder] $wStart → $wEnd  count=$count")
          put
        }
        puts.foreach(table.put)
        println(s"[seeder] Wrote ${puts.size} rows to '$TableStr'")
      } finally table.close()
    } finally { admin.close(); conn.close() }
  }
}
