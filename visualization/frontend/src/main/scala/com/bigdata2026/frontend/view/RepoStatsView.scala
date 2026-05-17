package com.bigdata2026.frontend.view

import com.bigdata2026.common.model.RepoStats
import com.bigdata2026.frontend.{Msg, RepoSortKey}
import com.bigdata2026.frontend.view.Components.*
import tyrian.Html
import tyrian.Html.*
import zio.Task

object RepoStatsView:

  def render(repos: List[RepoStats], sortKey: RepoSortKey): Html[Msg] =
    val (sortBy, metricLabel) = sortKey match
      case RepoSortKey.Activity => ((r: RepoStats) => r.activityCount, "Events")
      case RepoSortKey.Stars    => ((r: RepoStats) => r.starCount,     "Stars")
      case RepoSortKey.Forks    => ((r: RepoStats) => r.forkCount,     "Forks")
    val top = repos.sortBy(sortBy).reverse.take(10)

    div(cls := "card bg-base-100 shadow")(
      div(cls := "card-body p-0")(
        div(cls := "flex items-center justify-between px-5 py-3 border-b border-base-300")(
          h3(cls := "font-semibold text-sm tracking-wide")("Top Repositories"),
          div(cls := "tabs tabs-boxed tabs-xs bg-base-200")(
            tabBtn("Activity", RepoSortKey.Activity, sortKey),
            tabBtn("Stars",    RepoSortKey.Stars,    sortKey),
            tabBtn("Forks",    RepoSortKey.Forks,    sortKey),
          ),
        ),
        div(cls := "px-5 pt-2 pb-0 text-right text-xs text-base-content/40")(text(metricLabel)),
        if top.isEmpty then
          div(cls := "px-5 py-10 text-center text-sm text-base-content/30")(text("Waiting for data…"))
        else
          div(cls := "overflow-x-auto")(
            table(cls := "table table-sm table-zebra")(
              tbody()(
                top.zipWithIndex.map { case (r, i) =>
                  tr()(
                    td(cls := "w-10 pr-0")(rankBadge(i + 1)),
                    td(cls := "font-mono text-xs max-w-48 truncate")(
                      tyrian.Html.a(
                        cls  := "hover:underline hover:text-primary",
                        href := s"https://github.com/${r.repoName}",
                      )(text(r.repoName))
                    ),
                    td(cls := "text-right font-semibold tabular-nums text-sm")(text(fmtBig(sortBy(r)))),
                  )
                }*
              )
            )
          )
      )
    )

  private def tabBtn(label: String, key: RepoSortKey, active: RepoSortKey): Html[Msg] =
    val activeCls = if active == key then " tab-active" else ""
    button(cls := s"tab tab-sm$activeCls", onClick(Msg.SelectRepoSort(key)))(text(label))
