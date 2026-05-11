package com.bigdata2026.streaming

import com.bigdata2026.streaming.config.{HBaseConfig, KafkaConfig}
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
      consumer   <- ZIO.service[KafkaConsumerService]
      featureHub <- ZIO.service[StreamFeatureHub]
      hub        <- Hub.bounded[GitHubEvent](1024)
      _          <- consumer.events
                      .tap(e => ZIO.logDebug(s"Received ${e.eventType} from ${e.actorLogin}"))
                      .foreach(hub.publish)
                      .ensuring(hub.shutdown)
                      .forkDaemon
      _          <- featureHub.run(hub)
    } yield ())
      .retry(Schedule.exponential(1.second, 2.0).jittered && Schedule.recurs(10))
      .provide(
        KafkaConfig.live,
        HBaseConfig.live,
        KafkaConsumerService.live,
        ConsoleLogFeature.live,
        NewRepoWindowFeature.live,
        PushSpeedFeature.live,
        StreamFeatureHub.live,
      )
}
