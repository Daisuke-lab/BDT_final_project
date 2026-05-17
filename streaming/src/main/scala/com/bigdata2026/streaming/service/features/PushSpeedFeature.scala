package com.bigdata2026.streaming.service.features

import com.bigdata2026.streaming.model.GitHubEvent
import com.bigdata2026.streaming.service.StreamFeature
import zio._
import zio.stream.ZStream

import java.time.Instant
import java.util.concurrent.TimeUnit
import scala.util.Try

final class PushSpeedFeature extends StreamFeature {
  val name = "push-speed"

  private val WindowMs   = 5 * 60 * 1000L
  private val GraceMs    = 60 * 1000L
  private val FlushEvery = 30.seconds

  def process(events: ZStream[Any, Nothing, GitHubEvent]): Task[Unit] =
    for {
      state <- Ref.make(Map.empty[Long, Long])
      _ <- ZStream.fromSchedule(Schedule.fixed(FlushEvery))
             .mapZIO(_ => Clock.currentTime(TimeUnit.MILLISECONDS).flatMap(logCompleted(state, _)))
             .runDrain
             .forkDaemon
      _ <- events
             .filter(_.eventType == "PushEvent")
             .foreach { event =>
               val ws = windowStart(event.createdAt)
               state.update(m => m.updated(ws, m.getOrElse(ws, 0L) + event.pushSize))
             }
    } yield ()

  private def logCompleted(state: Ref[Map[Long, Long]], now: Long): Task[Unit] =
    for {
      snapshot  <- state.get
      completed  = snapshot.filter { case (ws, _) => ws + WindowMs + GraceMs <= now }
      _         <- ZIO.foreachDiscard(completed.toList) { case (ws, commits) =>
                     ZIO.logInfo(s"[$name] window=$ws commits=$commits")
                   }
      _         <- ZIO.when(completed.nonEmpty)(state.update(_ -- completed.keySet))
    } yield ()

  private def windowStart(createdAt: String): Long = {
    val epochMs = Try(Instant.parse(createdAt).toEpochMilli).getOrElse(java.lang.System.currentTimeMillis())
    (epochMs / WindowMs) * WindowMs
  }
}

object PushSpeedFeature {
  val live: ULayer[PushSpeedFeature] = ZLayer.succeed(new PushSpeedFeature)
}
