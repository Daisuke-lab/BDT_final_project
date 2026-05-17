package com.bigdata2026.frontend.view

import com.bigdata2026.common.model.ActorStats
import com.bigdata2026.frontend.Msg
import com.bigdata2026.frontend.view.Components.*
import tyrian.Html
import tyrian.Html.*
import zio.Task

object ContributorView:

  def render(actors: List[ActorStats]): Html[Msg] =
    val top = actors.sortBy(_.activityCount).reverse.take(10)
    div(cls := "card bg-base-100 shadow")(
      div(cls := "card-body p-0")(
        div(cls := "flex items-center justify-between px-5 py-4 border-b border-base-300")(
          h3(cls := "font-semibold text-sm tracking-wide")("Top Active Contributors"),
          span(cls := "text-xs text-base-content/40")("Events"),
        ),
        if top.isEmpty then
          div(cls := "px-5 py-10 text-center text-sm text-base-content/30")(text("Waiting for data…"))
        else
          div(cls := "overflow-x-auto")(
            table(cls := "table table-sm table-zebra")(
              tbody()(
                top.zipWithIndex.map { case (a, i) =>
                  tr()(
                    td(cls := "w-10 pr-0")(rankBadge(i + 1)),
                    td(cls := "max-w-48 truncate")(
                      tyrian.Html.a(
                        cls  := "hover:underline hover:text-primary font-mono text-xs",
                        href := s"https://github.com/${a.actorLogin}",
                      )(text(a.actorLogin))
                    ),
                    td(cls := "text-right font-semibold tabular-nums text-sm")(text(fmtBig(a.activityCount))),
                  )
                }*
              )
            )
          )
      )
    )
