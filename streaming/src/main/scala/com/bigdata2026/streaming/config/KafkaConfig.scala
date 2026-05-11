package com.bigdata2026.streaming.config

import zio._

final case class KafkaConfig(
  brokers:          String,
  topic:            String,
  groupId:          String,
  username:         String,
  password:         String,
  securityProtocol: String,
  saslMechanism:    String
) {
  def isSasl: Boolean = securityProtocol.equalsIgnoreCase("SASL_SSL")

  def saslJaasConfig: String =
    s"""org.apache.kafka.common.security.scram.ScramLoginModule required """ +
    s"""username="$username" password="$password";"""
}

object KafkaConfig {
  val live: ULayer[KafkaConfig] = ZLayer.succeed(
    KafkaConfig(
      brokers          = sys.env.getOrElse("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092"),
      topic            = sys.env.getOrElse("KAFKA_TOPIC",             "github-events"),
      groupId          = sys.env.getOrElse("KAFKA_GROUP_ID",          "github-streaming"),
      username         = sys.env.getOrElse("KAFKA_SASL_USERNAME",     ""),
      password         = sys.env.getOrElse("KAFKA_SASL_PASSWORD",     ""),
      securityProtocol = sys.env.getOrElse("KAFKA_SECURITY_PROTOCOL", ""),
      saslMechanism    = sys.env.getOrElse("KAFKA_SASL_MECHANISM",    "SCRAM-SHA-512")
    )
  )
}
