package com.bigdata2026.spark

import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{ColumnFamilyDescriptorBuilder, ConnectionFactory, Put, TableDescriptorBuilder}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types._

object RepoStatsJob {

  private val CF           = Bytes.toBytes("cf")
  private val RepoTable    = "repo_stats"
  private val LangTable    = "language_stats"
  private val ActorTable   = "actor_stats"

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("RepoStats")
      .master("local[*]")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    val brokers   = sys.env("KAFKA_BOOTSTRAP_SERVERS")
    val topic     = sys.env.getOrElse("KAFKA_TOPIC", "github-events")
    val protocol  = sys.env.get("KAFKA_SECURITY_PROTOCOL")
    val hdfsUrl   = sys.env.getOrElse("HDFS_NAMENODE_URL", "hdfs://namenode:9000")

    // ── Part 5: Load static reference datasets from HDFS ────────────────────
    // language_rankings.csv → (language, tier, paradigm, type) — language metadata
    val langMeta = spark.read
      .option("header", "true")
      .csv(s"$hdfsUrl/data/language_rankings.csv")
      .cache()

    langMeta.createOrReplaceTempView("language_rankings")

    println(s"[RepoStats] Loaded ${langMeta.count()} language metadata rows from HDFS")

    // ── Kafka source ─────────────────────────────────────────────────────────
    val rawReader = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", brokers)
      .option("subscribe", topic)
      .option("startingOffsets", "earliest")

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
      .add("language",     StringType)

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
        col("e.language").as("language"),
        col("event_ts")
      )

    // ── Streaming aggregation per repo (Part 2 + 3) ──────────────────────────
    val stats = events.groupBy("repo_name").agg(
      sum(when(col("event_type") === "WatchEvent", 1L).otherwise(0L)).as("star_count"),
      sum(when(col("event_type") === "ForkEvent",  1L).otherwise(0L)).as("fork_count"),
      sum(when(col("event_type") === "PushEvent",  1L).otherwise(0L)).as("push_count"),
      sum(when(col("event_type") === "PushEvent",  col("trend_weight")).otherwise(0L)).as("push_weight"),
      count("*").as("activity_count"),
      approx_count_distinct("actor_login", 0.05).as("contributor_count"),
      max(when(col("language") =!= lit(""), col("language"))).as("event_language"),
    )

    val actorStats = events.groupBy("actor_login").agg(count("*").as("activity_count"))

    stats.writeStream
      .outputMode("complete")
      .trigger(Trigger.ProcessingTime("5 seconds"))
      .foreachBatch { (batch: Dataset[Row], _: Long) =>
        val rows = batch.collect()
        if (rows.nonEmpty) {
          try writeRepoStatsToHBase(rows)
          catch { case e: Exception => println(s"[RepoStats] HBase write failed: ${e.getMessage}") }
          try enrichAndWriteLangStats(langMeta, batch, rows.length)
          catch { case e: Exception => println(s"[LangStats] enrichment failed: ${e.getMessage}") }
        }
      }
      .start()

    actorStats.writeStream
      .outputMode("complete")
      .trigger(Trigger.ProcessingTime("5 seconds"))
      .foreachBatch { (batch: Dataset[Row], _: Long) =>
        val rows = batch.collect().filter(r => r.getAs[String]("actor_login") != null)
        if (rows.nonEmpty) {
          try writeActorStatsToHBase(rows)
          catch { case e: Exception => println(s"[ActorStats] HBase write failed: ${e.getMessage}") }
        }
      }
      .start()

    spark.streams.awaitAnyTermination()
  }

  // ── Part 5: Spark SQL enrichment — join streaming data with language metadata
  // Spark Structured Streaming clones the SparkSession for each streaming query,
  // so we re-register DataFrames as temp views in the batch's session.
  // Static DataFrame is re-wrapped using its cached RDD (no HDFS re-read).
  private def enrichAndWriteLangStats(
    langMeta:  DataFrame,
    batch:     Dataset[Row],
    repoCount: Int,
  ): Unit = {
    val sess = batch.sparkSession
    sess.createDataFrame(langMeta.rdd, langMeta.schema).createOrReplaceTempView("language_rankings")
    batch.createOrReplaceTempView("repo_stats_batch")

    val langStats = sess.sql("""
      SELECT
        COALESCE(NULLIF(r.event_language, ''), 'Unknown') AS language,
        COALESCE(l.tier, '3')           AS tier,
        COALESCE(l.paradigm, 'unknown') AS paradigm,
        COALESCE(l.type, 'unknown')     AS lang_type,
        SUM(r.star_count)               AS total_stars,
        SUM(r.fork_count)               AS total_forks,
        SUM(r.push_count)               AS total_pushes,
        SUM(r.activity_count)           AS total_activity,
        COUNT(*)                        AS repo_count
      FROM repo_stats_batch r
      LEFT JOIN language_rankings l ON NULLIF(r.event_language, '') = l.language
      WHERE r.event_language IS NOT NULL AND r.event_language != ''
      GROUP BY COALESCE(NULLIF(r.event_language, ''), 'Unknown'), l.tier, l.paradigm, l.type
    """)

    val langRows = langStats.collect()
    println(s"[LangStats] enriched ${langRows.length} language rows from $repoCount repos")
    writeLangStatsToHBase(langRows)
  }

  private def writeRepoStatsToHBase(rows: Array[Row]): Unit = {
    withHBase { conn =>
      ensureTable(conn, RepoTable)
      val table = conn.getTable(TableName.valueOf(RepoTable))
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
          table.put(put)
        }
      } finally table.close()
    }
  }

  private def writeActorStatsToHBase(rows: Array[Row]): Unit = {
    withHBase { conn =>
      ensureTable(conn, ActorTable)
      val table = conn.getTable(TableName.valueOf(ActorTable))
      try {
        rows.foreach { row =>
          val actorLogin = row.getAs[String]("actor_login")
          val put        = new Put(Bytes.toBytes(actorLogin))
          put.addColumn(CF, Bytes.toBytes("activity_count"),
            Bytes.toBytes(row.getAs[Long]("activity_count").toString))
          table.put(put)
        }
      } finally table.close()
    }
  }

  private def writeLangStatsToHBase(rows: Array[Row]): Unit = {
    if (rows.isEmpty) return
    withHBase { conn =>
      ensureTable(conn, LangTable)
      val table = conn.getTable(TableName.valueOf(LangTable))
      try {
        rows.foreach { row =>
          val lang = row.getAs[String]("language")
          val put  = new Put(Bytes.toBytes(lang))
          def col_(name: String, value: String): Unit =
            put.addColumn(CF, Bytes.toBytes(name), Bytes.toBytes(value))
          col_("tier",           Option(row.getAs[String]("tier")).getOrElse("3"))
          col_("paradigm",       Option(row.getAs[String]("paradigm")).getOrElse("unknown"))
          col_("lang_type",      Option(row.getAs[String]("lang_type")).getOrElse("unknown"))
          col_("total_stars",    row.getAs[Long]("total_stars").toString)
          col_("total_forks",    row.getAs[Long]("total_forks").toString)
          col_("total_pushes",   row.getAs[Long]("total_pushes").toString)
          col_("total_activity", row.getAs[Long]("total_activity").toString)
          col_("repo_count",     row.getAs[Long]("repo_count").toString)
          table.put(put)
        }
      } finally table.close()
    }
  }

  private def withHBase[A](f: org.apache.hadoop.hbase.client.Connection => A): A = {
    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.quorum",
      sys.env.getOrElse("HBASE_ZOOKEEPER_QUORUM", "localhost"))
    conf.set("hbase.zookeeper.property.clientPort",
      sys.env.getOrElse("HBASE_ZOOKEEPER_PORT", "2182"))
    val conn = ConnectionFactory.createConnection(conf)
    try f(conn)
    finally conn.close()
  }

  private def ensureTable(conn: org.apache.hadoop.hbase.client.Connection, tableName: String): Unit = {
    val admin = conn.getAdmin
    try {
      val name = TableName.valueOf(tableName)
      if (!admin.tableExists(name)) {
        val desc = TableDescriptorBuilder.newBuilder(name)
          .setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(CF).build())
          .build()
        admin.createTable(desc)
      }
    } finally admin.close()
  }
}
