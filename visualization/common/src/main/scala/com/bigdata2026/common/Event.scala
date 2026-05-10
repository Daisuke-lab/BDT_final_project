package com.bigdata2026.common

import zio.json.*

final case class Event(id: String, source: String, payload: String, timestamp: Long)

object Event:
  given JsonCodec[Event] = DeriveJsonCodec.gen[Event]
