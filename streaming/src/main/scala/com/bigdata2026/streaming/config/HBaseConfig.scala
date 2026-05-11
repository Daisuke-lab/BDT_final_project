package com.bigdata2026.streaming.config

import zio._

final case class HBaseConfig(zookeeperQuorum: String, zookeeperPort: String)

object HBaseConfig {
  val live: ULayer[HBaseConfig] = ZLayer.succeed(
    HBaseConfig(
      zookeeperQuorum = sys.env.getOrElse("HBASE_ZOOKEEPER_QUORUM", "localhost"),
      zookeeperPort   = sys.env.getOrElse("HBASE_ZOOKEEPER_PORT",   "2182")
    )
  )
}
