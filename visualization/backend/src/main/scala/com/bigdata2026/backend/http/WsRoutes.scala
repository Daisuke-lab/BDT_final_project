package com.bigdata2026.backend.http

import com.bigdata2026.backend.service.FeatureHub
import com.bigdata2026.backend.service.realtime.ConnectionManager
import com.bigdata2026.backend.service.realtime.ConnectionManager.Connection
import com.bigdata2026.common.http.PathDef
import com.bigdata2026.common.ws.ServerMsg
import zio.*
import zio.http.*
import zio.json.EncoderOps

object WsRoutes:
  val routes: Routes[ConnectionManager & FeatureHub, Nothing] = Routes(
    Method.GET / PathDef.subscribe -> handler { (_: Request) =>
      Random.nextInt.flatMap { id =>
        val app = Handler.webSocket { channel =>
          val conn = Connection.Websocket(id, channel)
          ZIO.serviceWithZIO[ConnectionManager](_.addConnection(conn)) *>
            ZIO.serviceWithZIO[FeatureHub](_.allSnapshots)
              .flatMap { msgs =>
                ZIO.foreachDiscard(msgs) { msg =>
                  channel.send(ChannelEvent.read(WebSocketFrame.text(msg.toJson)))
                }
              }
              .tapError(e => ZIO.logError(s"[ws] snapshots failed conn=$id: ${e.getMessage}"))
              .ignore *>
            channel.awaitShutdown *>
            ZIO.serviceWithZIO[ConnectionManager](_.removeConnection(id))
        }
        Response.fromSocketApp(app)
      }
    }
  )
