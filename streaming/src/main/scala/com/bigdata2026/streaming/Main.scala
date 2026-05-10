package com.bigdata2026.streaming

import com.bigdata2026.schema.GitHubEventSchema
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types._

object Main {
  def main(args: Array[String]): Unit = {

    val broker = sys.env.getOrElse("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
    val topic  = sys.env.getOrElse("KAFKA_TOPIC", GitHubEventSchema.TOPIC)

    // SparkSession is the single entry point to all Spark functionality.
    // local[*] uses all available CPU cores as worker threads — for local dev only.
    // In Docker, spark-submit passes --master externally, overriding this value.
    val spark = SparkSession.builder()
      .appName("GitHubEventStream")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    // ── Stage 1: Raw Kafka stream ──────────────────────────────────────────────
    // Spark's Kafka source always produces this fixed schema regardless of message content:
    //   key            binary  — producer-set routing key
    //   value          binary  — the actual message payload (our JSON)
    //   topic          string
    //   partition      int
    //   offset         long    — monotonically increasing per partition
    //   timestamp      timestamp — Kafka broker ingestion time (or producer-set LogAppendTime)
    //   timestampType  int
    //
    // startingOffsets="latest" means: start reading from now, skip historical messages.
    // Use "earliest" to replay everything from the beginning of the topic.
    val rawKafkaStream = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", broker)
      .option("subscribe", topic)
      .option("startingOffsets", "latest")
      .load()

    println("\n=== Stage 1: Raw Kafka Schema ===")
    rawKafkaStream.printSchema()

    // ── Stage 2: Parse JSON ────────────────────────────────────────────────────
    // value arrives as binary. Cast to String, then use from_json to parse it.
    // StructType defines only the fields we care about — extra JSON fields are ignored.
    // If a field is missing in the JSON, Spark fills it with null.
    import GitHubEventSchema._
    val githubSchema = new StructType()
      .add(F_TYPE,  StringType)
      .add(F_ACTOR, new StructType().add(F_LOGIN,     StringType))
      .add(F_REPO,  new StructType().add(F_REPO_NAME, StringType))

    val parsedEvents = rawKafkaStream
      .select(
        from_json(col("value").cast("string"), githubSchema).alias("event"),
        col("timestamp") // Kafka ingestion timestamp — used as event time in Stage 3
      )
      .select(
        col(s"event.$F_TYPE").alias("event_type"),
        col(s"event.$F_ACTOR.$F_LOGIN").alias("actor"),
        col(s"event.$F_REPO.$F_REPO_NAME").alias("repo_name"),
        col("timestamp")
      )

    println("\n=== Stage 2: Parsed Events Schema ===")
    parsedEvents.printSchema()

    // ── Stage 3: Watermark ────────────────────────────────────────────────────
    // Watermarking is required for stateful operations (windowing, stream-stream joins).
    // It tells Spark: "data more than 10 seconds late for its event-time window can be dropped."
    // Without a watermark, Spark keeps state for all windows forever — unbounded memory growth.
    val withEventTime = parsedEvents
      .withWatermark("timestamp", "10 seconds")

    println("\n=== Stage 3: With Watermark (same schema, watermark declared) ===")
    withEventTime.printSchema()

    // ── Stage 4: Windowed aggregation ─────────────────────────────────────────
    // window(timeCol, windowDuration, slideDuration) creates overlapping time buckets.
    //   windowDuration = 1 minute  → each bucket covers 60 seconds of events
    //   slideDuration  = 30 seconds → a new bucket starts every 30 seconds
    // So each event belongs to 2 windows simultaneously (60s / 30s = 2).
    //
    // groupBy(window, event_type).count() counts how many events of each type
    // arrived within each window. This is the meaningful aggregation satisfying Part 2.
    val windowedCounts = withEventTime
      .groupBy(
        window(col("timestamp"), "1 minute", "30 seconds"),
        col("event_type")
      )
      .count()
      .select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("event_type"),
        col("count")
      )
      .orderBy("window_start", "event_type")

    println("\n=== Stage 4: Windowed Counts Schema ===")
    windowedCounts.printSchema()

    // ── Write stream ───────────────────────────────────────────────────────────
    // Output modes control which rows are emitted to the sink each trigger:
    //   "append"   — only rows that are finalized (window closed past watermark). Lowest latency.
    //   "update"   — rows that changed since the last trigger. Best for monitoring aggregations.
    //   "complete" — re-emit the entire result table every trigger. Required for orderBy on streams.
    //
    // We use "complete" here so the output is sorted and easy to read in the console.
    // Trade-off: Spark keeps the full aggregation state in memory and re-prints it every trigger.
    // For large result sets, switch to "update" and remove the orderBy.
    //
    // Trigger.ProcessingTime("10 seconds") — Spark polls Kafka and flushes output every 10s.
    // awaitTermination() blocks the main thread so the JVM does not exit.
    val query = windowedCounts.writeStream
      .format("console")
      .outputMode("complete")
      .option("truncate", "false")
      .trigger(Trigger.ProcessingTime("10 seconds"))
      .start()

    println(s"\n[stream] Listening on topic '$topic' from broker '$broker'")
    println("[stream] Output every 10 seconds. Press Ctrl+C to stop.\n")

    query.awaitTermination()
  }
}
