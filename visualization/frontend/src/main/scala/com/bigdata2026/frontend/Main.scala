package com.bigdata2026.frontend

import com.bigdata2026.common.http.BackendApiUrl
import com.bigdata2026.common.model.RepoStats
import com.bigdata2026.common.ws.ServerMsg
import tyrian.*
import tyrian.Html.*
import tyrian.websocket.{KeepAliveSettings, WebSocket, WebSocketConnect, WebSocketEvent}
import zio.*
import zio.interop.catz.asyncInstance
import zio.json.*
import scala.concurrent.duration.DurationInt
import scala.scalajs.js.annotation.*

// ── WebSocket lifecycle ───────────────────────────────────────────────────────

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
  repoStats: List[RepoStats],
  webSocket: WebSocketStatus,
  error:     Option[String],
)

object State:
  val initial: State = State(Nil, WebSocketStatus.Disconnected, None)

// ── Messages ──────────────────────────────────────────────────────────────────

enum Msg:
  case NoOp
  case Reconnect
  case WsConnecting(ws: WebSocket[Task])
  case WsConnected
  case WsDisconnected(reason: String)
  case WsError(err: String)
  case WsReceive(data: String)
  case RepoStatsLoaded(repos: List[RepoStats])
  case RepoStatsUpdated(repos: List[RepoStats])

// ── App ───────────────────────────────────────────────────────────────────────

@JSExportTopLevel("TyrianApp")
object Main extends TyrianZIOApp[Msg, State]:

  def main(args: Array[String]): Unit = launch("app")

  def router: Location => Msg = Routing.none(Msg.NoOp)

  def init(flags: Map[String, String]): (State, Cmd[Task, Msg]) =
    State.initial -> connectWs

  def update(state: State): Msg => (State, Cmd[Task, Msg]) =
    case Msg.NoOp                     => state -> Cmd.None
    case Msg.Reconnect                => state -> connectWs
    case Msg.WsConnecting(ws)         => state.copy(webSocket = WebSocketStatus.Connecting(ws)) -> Cmd.None
    case Msg.WsConnected              => state.copy(webSocket = state.webSocket.toConnected, error = None) -> Cmd.None
    case Msg.WsDisconnected(_)        => state.copy(webSocket = WebSocketStatus.Disconnected) -> Cmd.None
    case Msg.WsError(e)               => state.copy(webSocket = WebSocketStatus.Disconnected, error = Some(e)) -> Cmd.None
    case Msg.RepoStatsLoaded(repos)   => state.copy(repoStats = repos, error = None) -> Cmd.None
    case Msg.RepoStatsUpdated(repos)  => state.copy(repoStats = repos) -> Cmd.None
    case Msg.WsReceive(data)          =>
      data.fromJson[ServerMsg] match
        case Right(ServerMsg.RepoStatsSnapshot(rs)) => state -> Cmd.Emit(Msg.RepoStatsLoaded(rs))
        case Right(ServerMsg.RepoStatsUpdated(rs))  => state -> Cmd.Emit(Msg.RepoStatsUpdated(rs))
        case Right(_)                                => state -> Cmd.None
        case Left(_)                                 => state -> Cmd.None

  def view(state: State): Html[Msg] =
    div(cls := "min-h-screen bg-base-200")(
      navbar(state.webSocket),
      div(cls := "max-w-7xl mx-auto px-6 py-8 space-y-8")(
        state.error.fold(div()()) { e =>
          div(cls := "alert alert-error shadow")(text(e))
        },
        if state.repoStats.nonEmpty then overviewStats(state.repoStats) else div()(),
        div(cls := "grid grid-cols-1 lg:grid-cols-2 gap-6")(
          rankingCard("Top by Stars",        "Stars",        state.repoStats, _.starCount),
          rankingCard("Top by Forks",        "Forks",        state.repoStats, _.forkCount),
          rankingCard("Top by Activity",     "Events",       state.repoStats, _.activityCount),
          rankingCard("Top by Contributors", "Contributors", state.repoStats, _.contributorCount),
        ),
        rankingCard("Top by Longevity", "Active for", state.repoStats, r => r.lastSeen - r.firstSeen, fmtDuration),
      )
    )

  def subscriptions(state: State): Sub[Task, Msg] =
    state.webSocket match
      case WebSocketStatus.Disconnected =>
        Sub.every[Task](5.seconds, "ws-retry").map(_ => Msg.Reconnect)
      case other =>
        other.subscribe

  // ── WebSocket ───────────────────────────────────────────────────────────────

  private val connectWs: Cmd[Task, Msg] =
    WebSocket.connect[Task, Msg](
      address           = BackendApiUrl.subscribeWs,
      keepAliveSettings = KeepAliveSettings.default,
    ) {
      case WebSocketConnect.Error(err) => Msg.WsError(err)
      case WebSocketConnect.Socket(ws) => Msg.WsConnecting(ws)
    }

  // ── Components ──────────────────────────────────────────────────────────────

  private def navbar(status: WebSocketStatus): Html[Msg] =
    nav(cls := "navbar bg-base-100 border-b border-base-300 px-6 sticky top-0 z-10")(
      div(cls := "flex-1 flex items-center gap-3")(
        div(cls := "w-2 h-2 rounded-full bg-success animate-pulse")(),
        span(cls := "text-xl font-bold tracking-tight")("GitHub Pulse"),
        span(cls := "text-sm text-base-content/40 hidden sm:inline")("Real-time repository activity"),
      ),
      div(cls := "flex-none")(wsBadge(status)),
    )

  private def overviewStats(repos: List[RepoStats]): Html[Msg] =
    val stars        = repos.map(_.starCount).sum
    val forks        = repos.map(_.forkCount).sum
    val contributors = repos.map(_.contributorCount).sum
    val pushes       = repos.map(_.pushCount).sum
    div(cls := "stats shadow w-full bg-base-100 stats-vertical sm:stats-horizontal")(
      statBox("Repos Tracked",   repos.length.toString, "Currently monitored"),
      statBox("Total Stars",     fmtBig(stars),         "Across all repos"),
      statBox("Total Forks",     fmtBig(forks),         "Across all repos"),
      statBox("Total Pushes",    fmtBig(pushes),        "Since tracking started"),
      statBox("Active Users",    fmtBig(contributors),  "Unique contributors"),
    )

  private def statBox(title: String, value: String, desc: String): Html[Msg] =
    div(cls := "stat")(
      div(cls := "stat-title text-xs")(text(title)),
      div(cls := "stat-value text-2xl")(text(value)),
      div(cls := "stat-desc")(text(desc)),
    )

  private def rankingCard(
    title:  String,
    metric: String,
    repos:  List[RepoStats],
    sortBy: RepoStats => Long,
    fmt:    Long => String = fmtBig,
  ): Html[Msg] =
    val top = repos.sortBy(sortBy).reverse.take(10)
    div(cls := "card bg-base-100 shadow")(
      div(cls := "card-body p-0")(
        div(cls := "flex items-center justify-between px-5 py-4 border-b border-base-300")(
          h3(cls := "font-semibold text-sm tracking-wide")(text(title)),
          span(cls := "text-xs text-base-content/40")(text(metric)),
        ),
        if top.isEmpty then
          div(cls := "px-5 py-10 text-center text-sm text-base-content/30")(text("Waiting for data…"))
        else
          div(cls := "overflow-x-auto")(
            table(cls := "table table-sm table-zebra")(
              tbody()(
                top.zipWithIndex.map { case (r, i) =>
                  val rank = i + 1
                  tr()(
                    td(cls := "w-10 pr-0")(rankBadge(rank)),
                    td(cls := "font-mono text-xs max-w-48 truncate")(text(r.repoName)),
                    td(cls := "text-right font-semibold tabular-nums text-sm")(text(fmt(sortBy(r)))),
                  )
                }*
              )
            )
          )
      )
    )

  private def rankBadge(rank: Int): Html[Msg] =
    val cls_ = rank match
      case 1 => "badge badge-warning badge-sm font-bold w-7"
      case 2 => "badge badge-neutral badge-sm font-bold w-7"
      case 3 => "badge badge-accent badge-sm font-bold w-7"
      case _ => "badge badge-ghost badge-sm opacity-50 w-7"
    span(cls := cls_)(text(rank.toString))

  private def wsBadge(status: WebSocketStatus): Html[Msg] =
    val (label, cls_) = status match
      case WebSocketStatus.Connected(_)  => ("LIVE",         "badge badge-success badge-outline")
      case WebSocketStatus.Connecting(_) => ("CONNECTING",   "badge badge-warning badge-outline")
      case WebSocketStatus.Disconnected  => ("DISCONNECTED", "badge badge-error badge-outline")
    span(cls := cls_)(text(label))

  // ── Formatters ──────────────────────────────────────────────────────────────

  private def fmtBig(n: Long): String =
    if n >= 1_000_000L then
      val tenths = n / 100_000L
      s"${tenths / 10}.${tenths % 10}M"
    else if n >= 1_000L then
      val tenths = n / 100L
      s"${tenths / 10}.${tenths % 10}K"
    else n.toString

  private def fmtDuration(ms: Long): String =
    val hours = ms / 3_600_000L
    if hours >= 24 then s"${hours / 24}d ${hours % 24}h"
    else if hours > 0 then s"${hours}h"
    else s"${ms / 60_000L}m"
