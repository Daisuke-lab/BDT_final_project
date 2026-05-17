package com.bigdata2026.common.model

import zio.json.*

final case class RepoStats(
  repoName:         String,
  starCount:        Long,
  forkCount:        Long,
  pushCount:        Long,
  pushWeight:       Long,
  activityCount:    Long,
  contributorCount: Long,
)

object RepoStats:
  given JsonCodec[RepoStats] = DeriveJsonCodec.gen
