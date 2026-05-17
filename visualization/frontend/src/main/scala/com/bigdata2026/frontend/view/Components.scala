package com.bigdata2026.frontend.view

import com.bigdata2026.frontend.{Msg, WebSocketStatus}
import tyrian.Html
import tyrian.Html.*
import zio.Task

object Components:

  def rankBadge(rank: Int): Html[Msg] =
    val cls_ = rank match
      case 1 => "badge badge-warning badge-sm font-bold w-7"
      case 2 => "badge badge-neutral badge-sm font-bold w-7"
      case 3 => "badge badge-accent badge-sm font-bold w-7"
      case _ => "badge badge-ghost badge-sm opacity-50 w-7"
    span(cls := cls_)(text(rank.toString))

  def wsBadge(status: WebSocketStatus): Html[Msg] =
    val (label, cls_) = status match
      case WebSocketStatus.Connected(_)  => ("LIVE",         "badge badge-success badge-outline")
      case WebSocketStatus.Connecting(_) => ("CONNECTING",   "badge badge-warning badge-outline")
      case WebSocketStatus.Disconnected  => ("DISCONNECTED", "badge badge-error badge-outline")
    span(cls := cls_)(text(label))

  def statBox(title: String, value: String, desc: String): Html[Msg] =
    div(cls := "stat")(
      div(cls := "stat-title text-xs")(text(title)),
      div(cls := "stat-value text-2xl")(text(value)),
      div(cls := "stat-desc")(text(desc)),
    )

  def tierBadge(tier: String): Html[Msg] =
    val (label, cls_) = tier match
      case "1" => ("Tier 1", "badge badge-success badge-sm")
      case "2" => ("Tier 2", "badge badge-warning badge-sm")
      case _   => ("Tier 3", "badge badge-ghost badge-sm")
    span(cls := cls_)(text(label))

  def paradigmBadgeCls(paradigm: String): String = paradigm match
    case "functional"      => "badge-secondary"
    case "object-oriented" => "badge-primary"
    case "systems"         => "badge-error"
    case "concurrent"      => "badge-accent"
    case _                 => "badge-ghost"

  def fmtBig(n: Long): String =
    if n >= 1_000_000L then
      val tenths = n / 100_000L
      s"${tenths / 10}.${tenths % 10}M"
    else if n >= 1_000L then
      val tenths = n / 100L
      s"${tenths / 10}.${tenths % 10}K"
    else n.toString
