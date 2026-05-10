package com.bigdata2026.backend.service.realtime

import zio.*
import zio.http.{ChannelEvent, WebSocketFrame}
import zio.json.{EncoderOps, JsonEncoder}

trait MessageSender:
  def send[A: JsonEncoder](message: A, receiverId: ConnectionManager.Connection.Id): UIO[Unit]
  def broadcast[A: JsonEncoder](message: A): UIO[Unit]

object MessageSender:
  val live: URLayer[ConnectionManager, MessageSender] = ZLayer.fromZIO(
    ZIO.service[ConnectionManager].map { cm =>
      new MessageSender:
        def send[A: JsonEncoder](message: A, receiverId: ConnectionManager.Connection.Id): UIO[Unit] =
          cm.getConnection(receiverId).flatMap {
            case Some(ConnectionManager.Connection.Websocket(_, channel)) =>
              channel.send(ChannelEvent.read(WebSocketFrame.text(message.toJson))).ignore
            case _ => ZIO.unit
          }

        def broadcast[A: JsonEncoder](message: A): UIO[Unit] =
          val frame = ChannelEvent.read(WebSocketFrame.text(message.toJson))
          cm.getAll.flatMap { conns =>
            ZIO.foreachDiscard(conns) {
              case ConnectionManager.Connection.Websocket(_, channel) =>
                channel.send(frame).ignore
            }
          }
    }
  )
