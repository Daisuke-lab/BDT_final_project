package com.bigdata2026.common.model

import zio.json.*

final case class PushSpeedWindow(windowStart: Long, windowEnd: Long, pushCount: Long)

object PushSpeedWindow:
  given JsonCodec[PushSpeedWindow] = DeriveJsonCodec.gen
