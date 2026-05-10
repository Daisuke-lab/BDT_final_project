package com.bigdata2026.common.http

import zio.http.Root
import zio.http.codec.PathCodec

object PathDef:
  val subscribe: PathCodec[Unit] = Root / "api" / "subscribe"

object BackendApiUrl:
  private val base: String =
    sys.env.getOrElse("BACKEND_BASE_URL", "http://localhost:8080")
  val subscribeWs: String =
    base.replace("http://", "ws://").replace("https://", "wss://") + "/api/subscribe"
