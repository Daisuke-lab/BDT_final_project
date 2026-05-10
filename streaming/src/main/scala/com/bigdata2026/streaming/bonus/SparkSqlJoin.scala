package com.bigdata2026.streaming.bonus

import org.apache.spark.sql.{DataFrame, SparkSession}

/** Part 5 (Bonus) — Use Spark SQL to join streaming data with static datasets.
  *
  * Skeleton: load a static reference DataFrame (CSV / Parquet / Hive)
  * and join it against the Kafka-sourced streaming DataFrame on `joinKey`.
  */
object SparkSqlJoin {

  def enrich(
      spark: SparkSession,
      streamDf: DataFrame,
      staticPath: String,
      joinKey: String
  ): DataFrame = {
    val _ = (spark, staticPath, joinKey)
    // TODO:
    //   val staticDf = spark.read.parquet(staticPath)
    //   streamDf.join(staticDf, joinKey, "left")
    streamDf
  }
}
