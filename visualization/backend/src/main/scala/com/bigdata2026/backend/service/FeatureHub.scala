package com.bigdata2026.backend.service

import com.bigdata2026.backend.service.realtime.NewReposFeature
import com.bigdata2026.common.ws.ServerMsg
import zio.*
import zio.stream.ZStream

final class FeatureHub(features: List[Feature]):

  def allSnapshots: Task[List[ServerMsg]] =
    ZIO.foreach(features)(_.snapshot)

  def allUpdates: ZStream[Any, Throwable, ServerMsg] =
    features.map(resilient) match
      case Nil          => ZStream.empty
      case head :: tail => tail.foldLeft(head)(_ merge _)

  private def resilient(f: Feature): ZStream[Any, Throwable, ServerMsg] =
    f.liveUpdates
      .tapError(e => ZIO.logError(s"[${f.getClass.getSimpleName}] ${e.getMessage}"))
      .retry(Schedule.fixed(5.seconds))

object FeatureHub:
  // Add new features here as they are implemented
  val live: URLayer[NewReposFeature, FeatureHub] =
    ZLayer.fromZIO {
      ZIO.service[NewReposFeature].map { newRepos =>
        new FeatureHub(List(newRepos))
      }
    }
