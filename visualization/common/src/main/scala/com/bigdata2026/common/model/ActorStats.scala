package com.bigdata2026.common.model

import zio.json.*

final case class ActorStats(actorLogin: String, activityCount: Long)

object ActorStats:
  given JsonCodec[ActorStats] = DeriveJsonCodec.gen
