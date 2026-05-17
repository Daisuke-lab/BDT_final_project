package com.bigdata2026.common.ws

import com.bigdata2026.common.model.{ActorStats, LanguageStats, NewRepoWindow, PushSpeedWindow, RepoStats}
import zio.json.*
import zio.json.jsonDiscriminator

@jsonDiscriminator("type")
sealed trait ServerMsg

object ServerMsg:
  final case class Snapshot(windows: List[NewRepoWindow])            extends ServerMsg
  final case class NewWindow(window: NewRepoWindow)                   extends ServerMsg
  final case class PushSpeedSnapshot(windows: List[PushSpeedWindow])  extends ServerMsg
  final case class PushSpeedNewWindow(window: PushSpeedWindow)        extends ServerMsg
  final case class RepoStatsSnapshot(repos: List[RepoStats])          extends ServerMsg
  final case class RepoStatsUpdated(repos: List[RepoStats])               extends ServerMsg
  final case class LanguageStatsSnapshot(languages: List[LanguageStats])  extends ServerMsg
  final case class ActorStatsSnapshot(actors: List[ActorStats])           extends ServerMsg
  final case class ActorStatsUpdated(actors: List[ActorStats])            extends ServerMsg

  given JsonCodec[Snapshot]              = DeriveJsonCodec.gen
  given JsonCodec[NewWindow]             = DeriveJsonCodec.gen
  given JsonCodec[PushSpeedSnapshot]     = DeriveJsonCodec.gen
  given JsonCodec[PushSpeedNewWindow]    = DeriveJsonCodec.gen
  given JsonCodec[RepoStatsSnapshot]     = DeriveJsonCodec.gen
  given JsonCodec[RepoStatsUpdated]      = DeriveJsonCodec.gen
  given JsonCodec[LanguageStatsSnapshot] = DeriveJsonCodec.gen
  given JsonCodec[ActorStatsSnapshot]    = DeriveJsonCodec.gen
  given JsonCodec[ActorStatsUpdated]     = DeriveJsonCodec.gen
  given JsonCodec[ServerMsg]             = DeriveJsonCodec.gen
