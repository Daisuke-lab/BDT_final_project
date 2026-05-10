package com.bigdata2026.backend

import zio.*

object Main extends ZIOAppDefault:
  def run =
    Console.printLine(
      "[backend] ZIO + Tapir + HBase reader skeleton. " +
        "Register Tapir endpoints, expose them via zio-http, query HBase here."
    )
