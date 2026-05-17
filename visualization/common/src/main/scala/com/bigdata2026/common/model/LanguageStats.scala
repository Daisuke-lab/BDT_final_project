package com.bigdata2026.common.model

import zio.json.*

final case class LanguageStats(
  language:      String,
  tier:          String,
  paradigm:      String,
  langType:      String,
  totalStars:    Long,
  totalForks:    Long,
  totalPushes:   Long,
  totalActivity: Long,
  repoCount:     Long,
)

object LanguageStats:
  given JsonCodec[LanguageStats] = DeriveJsonCodec.gen
