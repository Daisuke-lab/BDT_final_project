package com.bigdata2026.common.model

import zio.json.*

final case class NewRepoWindow(windowStart: Long, windowEnd: Long, count: Long)

object NewRepoWindow:
  given JsonCodec[NewRepoWindow] = DeriveJsonCodec.gen
