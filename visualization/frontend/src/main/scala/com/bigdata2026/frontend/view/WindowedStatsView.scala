package com.bigdata2026.frontend.view

import com.bigdata2026.common.model.{NewRepoWindow, PushSpeedWindow}
import com.bigdata2026.frontend.Msg
import tyrian.*
import tyrian.Html.*
import scala.scalajs.js

object WindowedStatsView:

  def renderNewRepos(windows: List[NewRepoWindow]): Html[Msg] =
    val recent = windows.takeRight(12)
    val maxVal = recent.map(_.count).maxOption.getOrElse(1L)
    windowCard(
      title = "New Repos / 10-min Window",
      badge = "windowed",
      empty = recent.isEmpty,
      rows  = recent.map(w => (fmtTime(w.windowStart), w.count, maxVal)),
    )

  def renderPushSpeed(windows: List[PushSpeedWindow]): Html[Msg] =
    val recent = windows.takeRight(12)
    val maxVal = recent.map(_.pushCount).maxOption.getOrElse(1L)
    windowCard(
      title = "Push Speed / 10-min Window",
      badge = "windowed",
      empty = recent.isEmpty,
      rows  = recent.map(w => (fmtTime(w.windowStart), w.pushCount, maxVal)),
    )

  private def windowCard(
    title: String,
    badge: String,
    empty: Boolean,
    rows:  List[(String, Long, Long)],   // (timeLabel, value, maxValue)
  ): Html[Msg] =
    div(cls := "card bg-base-100 shadow")(
      div(cls := "card-body p-0")(
        div(cls := "flex items-center justify-between px-5 py-4 border-b border-base-300")(
          h3(cls := "font-semibold text-sm tracking-wide")(text(title)),
          span(cls := "badge badge-ghost badge-sm")(text(badge)),
        ),
        if empty then
          div(cls := "px-5 py-10 text-center text-sm text-base-content/30")(
            text("Waiting for windowed data…")
          )
        else
          div(cls := "overflow-x-auto")(
            table(cls := "table table-sm table-zebra")(
              thead()(
                tr()(
                  th()(text("Window")),
                  th()(text("Activity")),
                  th(cls := "text-right")(text("Count")),
                )
              ),
              tbody()(
                rows.map { case (label, value, maxVal) =>
                  tr()(
                    td(cls := "text-xs font-mono text-base-content/60 whitespace-nowrap")(text(label)),
                    td(cls := "w-full")(
                      progress(
                        cls := "progress progress-primary w-full",
                        Attribute("value", value.toString),
                        Attribute("max",   maxVal.toString),
                      )()
                    ),
                    td(cls := "text-right tabular-nums text-sm font-semibold")(text(value.toString)),
                  )
                }*
              ),
            )
          )
      )
    )

  private def fmtTime(epochMs: Long): String =
    val d = new js.Date(epochMs.toDouble)
    f"${d.getHours().toInt}%02d:${d.getMinutes().toInt}%02d"
