package com.bigdata2026.backend.service

import com.bigdata2026.common.ws.ServerMsg
import zio.*
import zio.stream.ZStream

trait Feature:
  def snapshot: Task[ServerMsg]
  def liveUpdates: ZStream[Any, Throwable, ServerMsg]
