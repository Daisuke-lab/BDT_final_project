package com.bigdata2026.backend.service.realtime

import zio.*
import zio.http.WebSocketChannel
import zio.stm.TMap

trait ConnectionManager:
  def addConnection(connection: ConnectionManager.Connection): UIO[Unit]
  def getConnection(id: ConnectionManager.Connection.Id): UIO[Option[ConnectionManager.Connection]]
  def getAll: UIO[Iterable[ConnectionManager.Connection]]
  def removeConnection(id: ConnectionManager.Connection.Id): UIO[Unit]

object ConnectionManager:
  object Connection:
    type Id = Int

  enum Connection:
    def id: Connection.Id
    case Websocket(id: Connection.Id, channel: WebSocketChannel)

  val live: ULayer[ConnectionManager] = ZLayer.fromZIO(
    TMap.empty[Connection.Id, Connection].commit.map { tmap =>
      new ConnectionManager:
        def addConnection(conn: Connection): UIO[Unit] =
          tmap.put(conn.id, conn).commit.unit

        def getConnection(id: Connection.Id): UIO[Option[Connection]] =
          tmap.get(id).commit

        def getAll: UIO[Iterable[Connection]] =
          tmap.values.commit

        def removeConnection(id: Connection.Id): UIO[Unit] =
          tmap.delete(id).commit.unit
    }
  )
