package com.bigdata2026.backend

import com.bigdata2026.backend.config.CorsConfig
import com.bigdata2026.backend.http.WsRoutes
import com.bigdata2026.backend.service.FeatureHub
import com.bigdata2026.backend.service.realtime.{ConnectionManager, MessageSender, NewReposFeature}
import com.bigdata2026.common.ws.ServerMsg
import zio.*
import zio.http.*
import zio.logging.backend.SLF4J

object Main extends ZIOAppDefault:

  override val bootstrap: ZLayer[ZIOAppArgs, Any, Any] =
    Runtime.removeDefaultLoggers >>> SLF4J.slf4j

  override def run =
    (for
      hub    <- ZIO.service[FeatureHub]
      _      <- hub.allUpdates
                  .foreach { msg =>
                    ZIO.serviceWithZIO[MessageSender](_.broadcast[ServerMsg](msg))
                  }
                  .forkDaemon
      config <- ZIO.service[CorsConfig]
      _      <- Server.serve(WsRoutes.routes @@ Middleware.cors(config.toZIOHttp))
    yield ()).provide(
      Server.defaultWithPort(8080),
      CorsConfig.live,
      ConnectionManager.live,
      MessageSender.live,
      NewReposFeature.live,
      FeatureHub.live,
    )
