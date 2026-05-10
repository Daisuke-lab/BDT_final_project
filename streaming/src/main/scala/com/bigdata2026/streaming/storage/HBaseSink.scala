package com.bigdata2026.streaming.storage

import org.apache.spark.sql.{DataFrame, ForeachWriter, Row}

/** Part 3 — Saving Processed Data to HBase.
  *
  * Skeleton for the HBase sink used by the streaming pipeline.
  * `Main` writes its processed DataFrame here, e.g.
  *   df.writeStream.foreach(new HBaseSink("events")).start()
  */
final class HBaseSink(tableName: String) extends ForeachWriter[Row] {

  def open(partitionId: Long, epochId: Long): Boolean = {
    // TODO: open HBase connection via ConnectionFactory.createConnection(conf)
    true
  }

  def process(row: Row): Unit = {
    // TODO: build a Put from `row` and submit to `tableName`
    val _ = (row, tableName)
  }

  def close(errorOrNull: Throwable): Unit = {
    // TODO: close HBase connection
  }
}

object HBaseSink {
  /** Convenience entry point for the streaming Main to call. */
  def write(df: DataFrame, table: String): Unit = {
    val _ = (df, table)
    // TODO: df.writeStream.foreach(new HBaseSink(table)).outputMode("append").start()
  }
}
