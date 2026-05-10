package com.bigdata2026.frontend

import tyrian.*
import tyrian.Html.*
import zio.*
import zio.interop.catz.*
import scala.scalajs.js.annotation.*

@JSExportTopLevel("TyrianApp")
object Main extends TyrianZIOApp[Msg, Model]:

  def main(args: Array[String]): Unit = launch("app")

  def router: Location => Msg = Routing.none(Msg.NoOp)

  def init(flags: Map[String, String]): (Model, Cmd[Task, Msg]) =
    (Model(), Cmd.None)

  def update(model: Model): Msg => (Model, Cmd[Task, Msg]) =
    _ => (model, Cmd.None)

  def view(model: Model): Html[Msg] =
    div()(
      h1()(text("BigData2026 — Visualization")),
      p()(text("Frontend skeleton. Wire HBase-backed views via /api."))
    )

  def subscriptions(model: Model): Sub[Task, Msg] = Sub.None

final case class Model()

enum Msg:
  case NoOp
