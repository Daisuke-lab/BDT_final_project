package com.bigdata2026.frontend

import com.bigdata2026.common.http.BackendApiUrl
import com.bigdata2026.common.model.{ActorStats, LanguageStats, NewRepoWindow, PushSpeedWindow, RepoStats}
import com.bigdata2026.common.ws.ServerMsg
import com.bigdata2026.frontend.view.Components.*
import com.bigdata2026.frontend.view.{ContributorView, RepoStatsView, WindowedStatsView}
import tyrian.*
import tyrian.Html.*
import tyrian.websocket.{KeepAliveSettings, WebSocket, WebSocketConnect, WebSocketEvent}
import zio.*
import zio.interop.catz.asyncInstance
import zio.json.*
import scala.concurrent.duration.DurationInt
import org.scalajs.dom
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

// ── Repo sort criteria ────────────────────────────────────────────────────────

enum RepoSortKey:
  case Activity, Stars, Forks

// ── State ─────────────────────────────────────────────────────────────────────

final case class State(
  repoStats:        List[RepoStats],
  languageStats:    List[LanguageStats],
  actorStats:       List[ActorStats],
  newRepoWindows:   List[NewRepoWindow],
  pushSpeedWindows: List[PushSpeedWindow],
  repoSortKey:      RepoSortKey,
  webSocket:        WebSocketStatus,
  error:            Option[String],
)

object State:
  val initial: State = State(Nil, Nil, Nil, Nil, Nil, RepoSortKey.Activity, WebSocketStatus.Disconnected, None)

// ── Messages ──────────────────────────────────────────────────────────────────

enum Msg:
  case NoOp
  case GoToUrl(url: String)
  case Reconnect
  case WsConnecting(ws: WebSocket[Task])
  case WsConnected
  case WsDisconnected(reason: String)
  case WsError(err: String)
  case WsReceive(data: String)
  case RepoStatsLoaded(repos: List[RepoStats])
  case RepoStatsUpdated(repos: List[RepoStats])
  case LangStatsLoaded(langs: List[LanguageStats])
  case ActorStatsLoaded(actors: List[ActorStats])
  case ActorStatsUpdated(actors: List[ActorStats])
  case NewRepoWindowsLoaded(windows: List[NewRepoWindow])
  case NewRepoWindowAdded(window: NewRepoWindow)
  case PushSpeedLoaded(windows: List[PushSpeedWindow])
  case PushSpeedWindowAdded(window: PushSpeedWindow)
  case SelectRepoSort(key: RepoSortKey)

// ── App ───────────────────────────────────────────────────────────────────────

@JSExportTopLevel("TyrianApp")
object Main extends TyrianZIOApp[Msg, State]:

  def main(args: Array[String]): Unit = launch("app")

  def router: Location => Msg =
    case loc: Location.External => Msg.GoToUrl(loc.href)
    case _: Location.Internal   => Msg.NoOp

  def init(flags: Map[String, String]): (State, Cmd[Task, Msg]) =
    State.initial -> connectWs

  def update(state: State): Msg => (State, Cmd[Task, Msg]) =
    case Msg.NoOp                      => state -> Cmd.None
    case Msg.GoToUrl(url)              => state -> Cmd.SideEffect { dom.window.open(url, "_blank") }
    case Msg.Reconnect                 => state -> connectWs
    case Msg.WsConnecting(ws)          => state.copy(webSocket = WebSocketStatus.Connecting(ws)) -> Cmd.None
    case Msg.WsConnected               => state.copy(webSocket = state.webSocket.toConnected, error = None) -> Cmd.None
    case Msg.WsDisconnected(_)         => state.copy(webSocket = WebSocketStatus.Disconnected) -> Cmd.None
    case Msg.WsError(e)                => state.copy(webSocket = WebSocketStatus.Disconnected, error = Some(e)) -> Cmd.None
    case Msg.RepoStatsLoaded(repos)       => state.copy(repoStats = repos, error = None) -> Cmd.None
    case Msg.RepoStatsUpdated(repos)      => state.copy(repoStats = repos) -> Cmd.None
    case Msg.LangStatsLoaded(langs)       => state.copy(languageStats = langs) -> Cmd.None
    case Msg.ActorStatsLoaded(actors)     => state.copy(actorStats = actors) -> Cmd.None
    case Msg.ActorStatsUpdated(actors)    => state.copy(actorStats = actors) -> Cmd.None
    case Msg.NewRepoWindowsLoaded(ws)     => state.copy(newRepoWindows = ws.sortBy(_.windowStart).takeRight(24)) -> Cmd.None
    case Msg.NewRepoWindowAdded(w)        => state.copy(newRepoWindows = (state.newRepoWindows :+ w).takeRight(24)) -> Cmd.None
    case Msg.PushSpeedLoaded(ws)          => state.copy(pushSpeedWindows = ws.sortBy(_.windowStart).takeRight(24)) -> Cmd.None
    case Msg.PushSpeedWindowAdded(w)      => state.copy(pushSpeedWindows = (state.pushSpeedWindows :+ w).takeRight(24)) -> Cmd.None
    case Msg.SelectRepoSort(key)          => state.copy(repoSortKey = key) -> Cmd.None
    case Msg.WsReceive(data)           =>
      data.fromJson[ServerMsg] match
        case Right(ServerMsg.RepoStatsSnapshot(rs))     => state -> Cmd.Emit(Msg.RepoStatsLoaded(rs))
        case Right(ServerMsg.RepoStatsUpdated(rs))      => state -> Cmd.Emit(Msg.RepoStatsUpdated(rs))
        case Right(ServerMsg.LanguageStatsSnapshot(ls)) => state -> Cmd.Emit(Msg.LangStatsLoaded(ls))
        case Right(ServerMsg.ActorStatsSnapshot(a))     => state -> Cmd.Emit(Msg.ActorStatsLoaded(a))
        case Right(ServerMsg.ActorStatsUpdated(a))      => state -> Cmd.Emit(Msg.ActorStatsUpdated(a))
        case Right(ServerMsg.Snapshot(ws))              => state -> Cmd.Emit(Msg.NewRepoWindowsLoaded(ws))
        case Right(ServerMsg.NewWindow(w))              => state -> Cmd.Emit(Msg.NewRepoWindowAdded(w))
        case Right(ServerMsg.PushSpeedSnapshot(ws))     => state -> Cmd.Emit(Msg.PushSpeedLoaded(ws))
        case Right(ServerMsg.PushSpeedNewWindow(w))     => state -> Cmd.Emit(Msg.PushSpeedWindowAdded(w))
        case Right(_)                                    => state -> Cmd.None
        case Left(_)                                     => state -> Cmd.None

  def view(state: State): Html[Msg] =
    div(cls := "min-h-screen bg-base-200")(
      navbar(state.webSocket),
      div(cls := "max-w-7xl mx-auto px-6 py-8 space-y-8")(
        state.error.fold(div()()) { e =>
          div(cls := "alert alert-error shadow")(text(e))
        },
        if state.repoStats.nonEmpty then overviewStats(state.repoStats) else div()(),
        div(cls := "grid grid-cols-1 lg:grid-cols-2 gap-6")(
          RepoStatsView.render(state.repoStats, state.repoSortKey),
          ContributorView.render(state.actorStats),
        ),
        div(cls := "grid grid-cols-1 lg:grid-cols-2 gap-6")(
          WindowedStatsView.renderNewRepos(state.newRepoWindows),
          WindowedStatsView.renderPushSpeed(state.pushSpeedWindows),
        ),
        if state.languageStats.nonEmpty then languagePanel(state.languageStats) else div()(),
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

  // ── Page-level components ───────────────────────────────────────────────────

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
      statBox("Stars Observed",  fmtBig(stars),         "New stars since tracking"),
      statBox("Forks Observed",  fmtBig(forks),         "New forks since tracking"),
      statBox("Total Pushes",    fmtBig(pushes),        "Since tracking started"),
      statBox("Active Users",    fmtBig(contributors),  "Unique contributors"),
    )

  private def languagePanel(langs: List[LanguageStats]): Html[Msg] =
    val top = langs.sortBy(_.totalActivity).reverse.take(10)
    div(cls := "card bg-base-100 shadow")(
      div(cls := "card-body p-0")(
        div(cls := "flex items-center justify-between px-5 py-4 border-b border-base-300")(
          div(cls := "flex items-center gap-2")(
            h3(cls := "font-semibold text-sm tracking-wide")("Top Languages by Activity"),
            span(cls := "badge badge-info badge-sm")("Spark SQL"),
          ),
          span(cls := "text-xs text-base-content/40")("Enriched via HDFS reference data"),
        ),
        if top.isEmpty then
          div(cls := "px-5 py-10 text-center text-sm text-base-content/30")(text("Waiting for enriched data…"))
        else
          div(cls := "overflow-x-auto")(
            table(cls := "table table-sm table-zebra")(
              thead()(
                tr()(
                  th()(),
                  th()(text("Language")),
                  th()(text("Tier")),
                  th()(text("Stars")),
                  th()(text("Forks")),
                  th()(text("Pushes")),
                  th()(text("Repos")),
                )
              ),
              tbody()(
                top.zipWithIndex.map { case (l, i) =>
                  tr()(
                    td(cls := "w-10 pr-0")(rankBadge(i + 1)),
                    td()(
                      div(cls := "flex items-center gap-2")(
                        span(cls := "font-semibold text-sm")(text(l.language)),
                        span(cls := s"badge badge-xs ${paradigmBadgeCls(l.paradigm)}")(text(l.paradigm)),
                      )
                    ),
                    td()(tierBadge(l.tier)),
                    td(cls := "text-right tabular-nums")(text(fmtBig(l.totalStars))),
                    td(cls := "text-right tabular-nums")(text(fmtBig(l.totalForks))),
                    td(cls := "text-right tabular-nums")(text(fmtBig(l.totalPushes))),
                    td(cls := "text-right tabular-nums")(text(l.repoCount.toString)),
                  )
                }*
              )
            )
          )
      )
    )
