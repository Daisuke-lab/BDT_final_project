package com.bigdata2026.frontend

import com.bigdata2026.common.http.BackendApiUrl
import com.bigdata2026.common.model.NewRepoWindow
import com.bigdata2026.common.ws.ServerMsg
import tyrian.*
import tyrian.Html.*
import tyrian.websocket.{KeepAliveSettings, WebSocket, WebSocketConnect, WebSocketEvent}
import zio.*
import zio.interop.catz.asyncInstance
import zio.json.*
import scala.concurrent.duration.DurationInt
import scala.scalajs.js.annotation.*

// ── WebSocket lifecycle state ─────────────────────────────────────────────────

enum WebSocketStatus:
  case Disconnected
  case Connecting(ws: WebSocket[Task])
  case Connected(ws: WebSocket[Task])

  def toConnected: WebSocketStatus = this match
    case Connecting(ws) => Connected(ws)
    case other          => other

  def subscribe: Sub[Task, Msg] = this match
    case Connecting(ws) => ws.subscribe(toMsg)
    case Connected(ws)  => ws.subscribe(toMsg)
    case Disconnected   => Sub.None

  private def toMsg(event: WebSocketEvent): Msg = event match
    case WebSocketEvent.Error(e)         => Msg.WsError(e)
    case WebSocketEvent.Receive(payload) => Msg.WsReceive(payload)
    case WebSocketEvent.Open             => Msg.WsConnected
    case WebSocketEvent.Close(code, r)   =>
      if code == 1000 then Msg.WsDisconnected(r)
      else Msg.WsError(s"closed code=$code reason=$r")
    case WebSocketEvent.Heartbeat        => Msg.NoOp

// ── State ─────────────────────────────────────────────────────────────────────

final case class State(
  windows: List[NewRepoWindow],
  webSocket: WebSocketStatus,
  error: Option[String],
)

object State:
  val MaxWindows: Int = 24
  val initial: State  = State(Nil, WebSocketStatus.Disconnected, None)

// ── Messages ──────────────────────────────────────────────────────────────────

enum Msg:
  case NoOp
  case Reconnect
  case WsConnecting(ws: WebSocket[Task])
  case WsConnected
  case WsDisconnected(reason: String)
  case WsError(err: String)
  case WsReceive(data: String)
  case SnapshotLoaded(windows: List[NewRepoWindow])
  case NewWindowReceived(window: NewRepoWindow)

// ── Tyrian app ────────────────────────────────────────────────────────────────

@JSExportTopLevel("TyrianApp")
object Main extends TyrianZIOApp[Msg, State]:

  def main(args: Array[String]): Unit = launch("app")

  def router: Location => Msg = Routing.none(Msg.NoOp)

  def init(flags: Map[String, String]): (State, Cmd[Task, Msg]) =
    State.initial -> connectWs

  def update(state: State): Msg => (State, Cmd[Task, Msg]) =
    case Msg.NoOp                      => state -> Cmd.None
    case Msg.Reconnect                 => state -> connectWs
    case Msg.WsConnecting(ws)          => state.copy(webSocket = WebSocketStatus.Connecting(ws)) -> Cmd.None
    case Msg.WsConnected               => state.copy(webSocket = state.webSocket.toConnected, error = None) -> Cmd.None
    case Msg.WsDisconnected(_)         => state.copy(webSocket = WebSocketStatus.Disconnected) -> Cmd.None
    case Msg.WsError(e)                => state.copy(webSocket = WebSocketStatus.Disconnected, error = Some(e)) -> Cmd.None
    case Msg.SnapshotLoaded(windows)   => state.copy(windows = windows, error = None) -> Cmd.None
    case Msg.NewWindowReceived(window) =>
      state.copy(windows = (window :: state.windows).take(State.MaxWindows)) -> Cmd.None
    case Msg.WsReceive(data)           =>
      data.fromJson[ServerMsg] match
        case Right(ServerMsg.Snapshot(ws)) => state -> Cmd.Emit(Msg.SnapshotLoaded(ws))
        case Right(ServerMsg.NewWindow(w)) => state -> Cmd.Emit(Msg.NewWindowReceived(w))
        case Left(_)                       => state -> Cmd.None

  def view(state: State): Html[Msg] =
    div(cls := "min-h-screen bg-base-200 p-8")(
      div(cls := "max-w-3xl mx-auto space-y-6")(
        div(cls := "flex items-center justify-between")(
          h1(cls := "text-2xl font-bold")("New Repos / 5 min"),
          wsBadge(state.webSocket),
        ),
        state.error.fold(div()()) { e =>
          div(cls := "alert alert-error")(text(e))
        },
        if state.windows.isEmpty then
          div(cls := "card bg-base-100 shadow")(
            div(cls := "card-body items-center")(
              p(cls := "text-center opacity-50")("Waiting for data…")
            )
          )
        else
          div(cls := "card bg-base-100 shadow overflow-x-auto")(
            table(cls := "table table-zebra")(
              thead()(
                tr()(th()("Window Start"), th()("Window End"), th()("New Repos"))
              ),
              tbody()(
                state.windows.map { w =>
                  tr()(
                    td()(text(formatTs(w.windowStart))),
                    td()(text(formatTs(w.windowEnd))),
                    td()(span(cls := "badge badge-primary badge-lg")(text(w.count.toString))),
                  )
                }
              )
            )
          )
      )
    )

  def subscriptions(state: State): Sub[Task, Msg] =
    state.webSocket match
      case WebSocketStatus.Disconnected =>
        Sub.every[Task](5.seconds, "ws-retry").map(_ => Msg.Reconnect)
      case other =>
        other.subscribe

  private val connectWs: Cmd[Task, Msg] =
    WebSocket.connect[Task, Msg](
      address           = BackendApiUrl.subscribeWs,
      keepAliveSettings = KeepAliveSettings.default,
    ) {
      case WebSocketConnect.Error(err) => Msg.WsError(err)
      case WebSocketConnect.Socket(ws) => Msg.WsConnecting(ws)
    }

  private def wsBadge(status: WebSocketStatus): Html[Msg] =
    val (label, klass) = status match
      case WebSocketStatus.Connected(_)  => ("LIVE",         "badge badge-success")
      case WebSocketStatus.Connecting(_) => ("CONNECTING",   "badge badge-warning")
      case WebSocketStatus.Disconnected  => ("DISCONNECTED", "badge badge-error")
    span(cls := klass)(text(label))

  // UTC epoch-ms → "HH:MM" (zero-padded)
  private def formatTs(epochMs: Long): String =
    val s = epochMs / 1000
    val h = (s / 3600) % 24
    val m = (s / 60) % 60
    f"$h%02d:$m%02d"
