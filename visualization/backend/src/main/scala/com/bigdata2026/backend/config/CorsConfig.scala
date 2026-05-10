package com.bigdata2026.backend.config

import zio.*
import zio.http.Header.{AccessControlAllowOrigin, Origin}
import zio.http.Middleware

import scala.annotation.tailrec

final case class CorsConfig(allowedOrigins: Vector[Origin]):
  def toZIOHttp: Middleware.CorsConfig = Middleware.CorsConfig(
    allowedOrigin = {
      case origin if allowedOrigins.contains(origin) =>
        Some(AccessControlAllowOrigin.Specific(origin))
      case _ => None
    }
  )

object CorsConfig:
  private val Default = "http://localhost:9876"

  val live: ULayer[CorsConfig] = ZLayer.succeed {
    val raw = sys.env.getOrElse("CORS_ALLOWED_ORIGINS", Default)
    parse(raw).getOrElse(CorsConfig(Vector.empty))
  }

  def parse(s: String): Option[CorsConfig] =
    val parts = s.split(",").map(_.trim).filter(_.nonEmpty).toList
    go(parts, Vector.empty).map(CorsConfig(_))

  @tailrec
  private def go(rem: List[String], acc: Vector[Origin]): Option[Vector[Origin]] = rem match
    case Nil    => Some(acc)
    case h :: t =>
      Origin.parse(h) match
        case Right(o) => go(t, acc :+ o)
        case Left(_)  => None
