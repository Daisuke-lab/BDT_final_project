package com.bigdata2026.streaming

import com.bigdata2026.streaming.config.KafkaConfig
import com.bigdata2026.streaming.model.GitHubEvent
import com.bigdata2026.streaming.service.{KafkaConsumerService, StreamFeatureHub}
import com.bigdata2026.streaming.service.features.{ConsoleLogFeature, NewRepoWindowFeature, PushSpeedFeature}
import zio._
import zio.logging.backend.SLF4J

object Main extends ZIOAppDefault {

  override val bootstrap: ZLayer[ZIOAppArgs, Any, Any] =
    Runtime.removeDefaultLoggers >>> SLF4J.slf4j

  override def run =
    (for {
      cfg        <- ZIO.service[KafkaConfig]
      _          <- ZIO.logInfo(s"[Streaming] Connecting to Kafka: ${cfg.brokers} (SASL=${cfg.isSasl})")
      consumer   <- ZIO.service[KafkaConsumerService]
      featureHub <- ZIO.service[StreamFeatureHub]
      hub        <- Hub.bounded[GitHubEvent](1024)
      _          <- ZIO.logInfo("[Streaming] Starting consumer fiber")
      _          <- consumer.events
                      .tap(e => ZIO.logInfo(s"[Streaming] ${e.eventType} from ${e.actorLogin}"))
                      .foreach(hub.publish)
                      .tapError(e => ZIO.logError(s"[Consumer] stream failed: ${e.getClass.getSimpleName}: ${e.getMessage}"))
                      .ensuring(ZIO.logInfo("[Consumer] stream ended") *> hub.shutdown)
                      .forkDaemon
      _          <- ZIO.logInfo("[Streaming] Consumer forked, running features")
      _          <- featureHub.run(hub)
    } yield ())
      .retry(Schedule.exponential(1.second, 2.0).jittered && Schedule.recurs(10))
      .provide(
        KafkaConfig.live,
        KafkaConsumerService.live,
        ConsoleLogFeature.live,
        NewRepoWindowFeature.live,
        PushSpeedFeature.live,
        StreamFeatureHub.live,
      )
}
