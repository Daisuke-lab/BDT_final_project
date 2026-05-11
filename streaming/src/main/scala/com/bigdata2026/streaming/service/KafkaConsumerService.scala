package com.bigdata2026.streaming.service

import com.bigdata2026.streaming.config.KafkaConfig
import com.bigdata2026.streaming.model.GitHubEvent
import zio._
import zio.kafka.consumer.{Consumer, ConsumerSettings, Subscription}
import zio.kafka.serde.Serde
import zio.stream.ZStream

trait KafkaConsumerService {
  def events: ZStream[Any, Throwable, GitHubEvent]
}

object KafkaConsumerService {
  val live: URLayer[KafkaConfig, KafkaConsumerService] =
    ZLayer.fromZIO {
      ZIO.service[KafkaConfig].flatMap { kafka =>
        ZIO.logInfo(
          s"KafkaConsumerService: brokers=${kafka.brokers} topic=${kafka.topic} sasl=${kafka.isSasl}"
        ).as {
          new KafkaConsumerService {
            def events: ZStream[Any, Throwable, GitHubEvent] =
              Consumer
                .plainStream(Subscription.topics(kafka.topic), Serde.string, Serde.string)
                .mapZIO { record =>
                  val parsed = GitHubEvent.fromEnvelopeJson(record.value)
                  val log = if (parsed.isEmpty)
                    ZIO.logWarning(s"Failed to parse envelope: ${record.value.take(120)}")
                  else ZIO.unit
                  record.offset.commit *> log.as(parsed)
                }
                .collectSome
                .provideLayer(ZLayer.scoped(Consumer.make(buildSettings(kafka))))
          }
        }
      }
    }

  private def buildSettings(kafka: KafkaConfig): ConsumerSettings = {
    val base = ConsumerSettings(kafka.brokers.split(",").toList)
      .withGroupId(kafka.groupId)
    if (kafka.isSasl)
      base
        .withProperty("security.protocol", kafka.securityProtocol)
        .withProperty("sasl.mechanism",    kafka.saslMechanism)
        .withProperty("sasl.jaas.config",  kafka.saslJaasConfig)
    else base
  }
}
