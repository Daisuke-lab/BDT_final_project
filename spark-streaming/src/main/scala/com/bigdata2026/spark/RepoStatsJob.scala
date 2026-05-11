package com.bigdata2026.spark

import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{ColumnFamilyDescriptorBuilder, ConnectionFactory, Put, TableDescriptorBuilder}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types._

object RepoStatsJob {

  private val CF        = Bytes.toBytes("cf")
  private val TableName_ = "repo_stats"

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("RepoStats")
      .master("local[*]")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    val brokers  = sys.env("KAFKA_BOOTSTRAP_SERVERS")
    val topic    = sys.env.getOrElse("KAFKA_TOPIC", "github-events")
    val protocol = sys.env.get("KAFKA_SECURITY_PROTOCOL")

    val rawReader = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", brokers)
      .option("subscribe", topic)
      .option("startingOffsets", "latest")

    val reader = protocol match {
      case Some(p) =>
        val mechanism = sys.env.getOrElse("KAFKA_SASL_MECHANISM", "SCRAM-SHA-512")
        val user      = sys.env("KAFKA_SASL_USERNAME")
        val pass      = sys.env("KAFKA_SASL_PASSWORD")
        val jaas      = s"""org.apache.kafka.common.security.scram.ScramLoginModule required username="$user" password="$pass";"""
        rawReader
          .option("kafka.security.protocol", p)
          .option("kafka.sasl.mechanism",    mechanism)
          .option("kafka.sasl.jaas.config",  jaas)
      case None => rawReader
    }

    val envelopeSchema = new StructType().add("payload", StringType)

    val eventSchema = new StructType()
      .add("event_type",   StringType)
      .add("actor_login",  StringType)
      .add("repo_name",    StringType)
      .add("trend_weight", IntegerType)

    val events = reader.load()
      .select(
        col("value").cast("string").as("raw"),
        col("timestamp").cast("long").as("event_ts")
      )
      .select(from_json(col("raw"), envelopeSchema).as("env"), col("event_ts"))
      .select(from_json(col("env.payload"), eventSchema).as("e"), col("event_ts"))
      .filter(col("e.repo_name").isNotNull && col("e.event_type").isNotNull)
      .select(
        col("e.event_type").as("event_type"),
        col("e.actor_login").as("actor_login"),
        col("e.repo_name").as("repo_name"),
        col("e.trend_weight").cast("long").as("trend_weight"),
        col("event_ts")
      )

    val stats = events.groupBy("repo_name").agg(
      sum(when(col("event_type") === "WatchEvent", 1L).otherwise(0L)).as("star_count"),
      sum(when(col("event_type") === "ForkEvent",  1L).otherwise(0L)).as("fork_count"),
      sum(when(col("event_type") === "PushEvent",  1L).otherwise(0L)).as("push_count"),
      sum(when(col("event_type") === "PushEvent",  col("trend_weight")).otherwise(0L)).as("push_weight"),
      count("*").as("activity_count"),
      approx_count_distinct("actor_login", 0.05).as("contributor_count"),
      min("event_ts").as("first_seen"),
      max("event_ts").as("last_seen"),
    )

    stats.writeStream
      .outputMode("complete")
      .trigger(Trigger.ProcessingTime("5 seconds"))
      .foreachBatch { (batch: Dataset[Row], _: Long) =>
        val rows = batch.collect()
        if (rows.nonEmpty) writeToHBase(rows)
      }
      .start()
      .awaitTermination()
  }

  private def writeToHBase(rows: Array[Row]): Unit = {
    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.quorum",
      sys.env.getOrElse("HBASE_ZOOKEEPER_QUORUM", "localhost"))
    conf.set("hbase.zookeeper.property.clientPort",
      sys.env.getOrElse("HBASE_ZOOKEEPER_PORT", "2182"))

    val conn = ConnectionFactory.createConnection(conf)
    try {
      ensureTableExists(conn)
      val table = conn.getTable(TableName.valueOf(TableName_))
      try {
        rows.foreach { row =>
          val repoName = row.getAs[String]("repo_name")
          val put      = new Put(Bytes.toBytes(repoName))
          def col_(name: String, value: Long): Unit =
            put.addColumn(CF, Bytes.toBytes(name), Bytes.toBytes(value.toString))
          col_("star_count",        row.getAs[Long]("star_count"))
          col_("fork_count",        row.getAs[Long]("fork_count"))
          col_("push_count",        row.getAs[Long]("push_count"))
          col_("push_weight",       row.getAs[Long]("push_weight"))
          col_("activity_count",    row.getAs[Long]("activity_count"))
          col_("contributor_count", row.getAs[Long]("contributor_count"))
          col_("first_seen",        row.getAs[Long]("first_seen"))
          col_("last_seen",         row.getAs[Long]("last_seen"))
          table.put(put)
        }
      } finally table.close()
    } finally conn.close()
  }

  private def ensureTableExists(conn: org.apache.hadoop.hbase.client.Connection): Unit = {
    val admin = conn.getAdmin
    try {
      val name = TableName.valueOf(TableName_)
      if (!admin.tableExists(name)) {
        val desc = TableDescriptorBuilder.newBuilder(name)
          .setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(CF).build())
          .build()
        admin.createTable(desc)
      }
    } finally admin.close()
  }
}
