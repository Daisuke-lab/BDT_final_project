package com.bigdata2026.streaming.service

import com.bigdata2026.streaming.model.GitHubEvent
import zio._
import zio.stream.ZStream

trait StreamFeature {
  def name: String
  def process(events: ZStream[Any, Nothing, GitHubEvent]): Task[Unit]
}
