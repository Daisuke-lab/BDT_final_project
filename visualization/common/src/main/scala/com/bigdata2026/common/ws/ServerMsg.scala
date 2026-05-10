package com.bigdata2026.common.ws

import com.bigdata2026.common.model.NewRepoWindow
import zio.json.*
import zio.json.jsonDiscriminator

@jsonDiscriminator("type")
sealed trait ServerMsg

object ServerMsg:
  final case class Snapshot(windows: List[NewRepoWindow]) extends ServerMsg
  final case class NewWindow(window: NewRepoWindow)       extends ServerMsg

  given JsonCodec[Snapshot]  = DeriveJsonCodec.gen
  given JsonCodec[NewWindow] = DeriveJsonCodec.gen
  given JsonCodec[ServerMsg] = DeriveJsonCodec.gen
