package com.bigdata2026.streaming.service.features

import com.bigdata2026.streaming.model.GitHubEvent
import com.bigdata2026.streaming.service.StreamFeature
import zio._
import zio.stream.ZStream

final class ConsoleLogFeature extends StreamFeature {
  val name = "console-log"
  def process(events: ZStream[Any, Nothing, GitHubEvent]): Task[Unit] =
    events.foreach { e =>
      val extra = if (e.eventType == "CreateEvent") s" refType=${e.refType}" else ""
      ZIO.logInfo(s"[${e.eventType}] ${e.actorLogin} → ${e.repoName} (weight=${e.trendWeight}$extra)")
    }
}

object ConsoleLogFeature {
  val live: ULayer[ConsoleLogFeature] = ZLayer.succeed(new ConsoleLogFeature)
}
