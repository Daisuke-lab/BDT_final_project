package com.bigdata2026.streaming.service

import com.bigdata2026.streaming.model.GitHubEvent
import com.bigdata2026.streaming.service.features.{ConsoleLogFeature, NewRepoWindowFeature, PushSpeedFeature}
import zio._
import zio.stream.ZStream

final class StreamFeatureHub(features: List[StreamFeature]) {
  def run(hub: Hub[GitHubEvent]): Task[Unit] =
    ZIO.foreachParDiscard(features) { feature =>
      feature
        .process(ZStream.fromHub(hub))
        .tapError(e => ZIO.logError(s"[${feature.name}] crashed: ${e.getMessage}"))
        .retry(Schedule.exponential(1.second))
    }
}

object StreamFeatureHub {
  val live: URLayer[ConsoleLogFeature with NewRepoWindowFeature with PushSpeedFeature, StreamFeatureHub] =
    ZLayer.fromZIO {
      for {
        console    <- ZIO.service[ConsoleLogFeature]
        newRepos   <- ZIO.service[NewRepoWindowFeature]
        pushSpeed  <- ZIO.service[PushSpeedFeature]
      } yield new StreamFeatureHub(List(console, newRepos, pushSpeed))
    }
}
