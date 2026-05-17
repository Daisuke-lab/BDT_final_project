import org.scalajs.linker.interface.ModuleKind
import com.w47s0n.scalajscli.ScalaJsCli.autoImport._
import sbtwelcome._
import scala.Console._

// Reads infra/local.env (gitignored) as KEY=VALUE pairs.
// Falls back to empty map if the file doesn't exist so CI/CD can inject via real env vars.
def localEnv: Map[String, String] = {
  val f = new java.io.File("infra/local.env")
  if (!f.exists) Map.empty
  else {
    val src = scala.io.Source.fromFile(f)
    try src.getLines()
      .map(_.trim)
      .filterNot(l => l.isEmpty || l.startsWith("#"))
      .flatMap { line =>
        line.split("=", 2) match {
          case Array(k, v) => Some(k.trim -> v.trim)
          case _           => None
        }
      }
      .toMap
    finally src.close()
  }
}

// Local Kafka overrides — keeps GITHUB_TOKEN from local.env but forces Kafka to localhost.
def localKafkaEnv: Map[String, String] =
  localEnv.filter(_._1 == "GITHUB_TOKEN") ++ Map(
    "KAFKA_BOOTSTRAP_SERVERS" -> "localhost:29092",
    "KAFKA_SECURITY_PROTOCOL" -> "",
    "KAFKA_SASL_USERNAME"     -> "",
    "KAFKA_SASL_PASSWORD"     -> "",
    "KAFKA_TOPIC"             -> "github-events",
    "KAFKA_GROUP_ID"          -> "github-streaming"
  )

ThisBuild / organization := "com.bigdata2026"
ThisBuild / version      := "0.1.0-SNAPSHOT"

logo :=
  s"""
     |  ██████╗ ██╗ ██████╗     ██████╗  █████╗ ████████╗ █████╗
     |  ██╔══██╗██║██╔════╝     ██╔══██╗██╔══██╗╚══██╔══╝██╔══██╗
     |  ██████╔╝██║██║  ███╗    ██║  ██║███████║   ██║   ███████║
     |  ██╔══██╗██║██║   ██║    ██║  ██║██╔══██║   ██║   ██╔══██║
     |  ██████╔╝██║╚██████╔╝    ██████╔╝██║  ██║   ██║   ██║  ██║
     |  ╚═════╝ ╚═╝ ╚═════╝     ╚═════╝ ╚═╝  ╚═╝   ╚═╝   ╚═╝  ╚═╝
     |
     |  GitHub Pulse — Real-time Repository Analytics  v${version.value}
     |  Kafka · Spark Streaming · HBase · ZIO · Tyrian
     |""".stripMargin

usefulTasks := Seq(
  UsefulTask("ingestion/run",      "GitHub event producer → production Kafka").alias("p1"),
  UsefulTask("p1dev",              "GitHub event producer → local Kafka"),
  UsefulTask("sparkStreaming/run", "Spark Structured Streaming → HBase").alias("p3"),
  UsefulTask("vizBackend/reStart", "WebSocket backend on :8080").alias("bs"),
  UsefulTask("vizFrontend/dev",    "Frontend dev server on :9876").alias("fdev"),
)

logoColor := CYAN

val scala3    = "3.7.3"
val scala2    = "2.12.18"
val scala213  = "2.13.16"

val sparkVersion    = "3.5.1"
val kafkaVersion    = "3.2.3"
val hbaseVersion    = "2.1.10"
val zioVersion      = "2.1.16"
val zioHttpVersion  = "3.8.0"
val zioKafkaVersion = "2.9.0"
val tyrianVersion   = "0.14.0"

// Shared assembly settings for all JVM fat-JAR modules.
// Stable jar name (no version suffix) keeps Dockerfile COPY paths fixed.
val assemblySettings = Seq(
  assembly / assemblyJarName := s"${name.value}-assembly.jar",
  assembly / assemblyMergeStrategy := {
    case PathList("META-INF", "services", _*) => MergeStrategy.concat
    case PathList("META-INF", _*)             => MergeStrategy.discard
    case "reference.conf"                     => MergeStrategy.concat
    case "application.conf"                   => MergeStrategy.concat
    case _                                    => MergeStrategy.first
  }
)

// ── Part 1 — ingestion (Java 11) ─────────────────────────────────────────────
lazy val ingestion = project
  .in(file("ingestion"))
  .settings(
    name             := "ingestion",
    autoScalaLibrary := false,
    crossPaths       := false,
    javacOptions ++= Seq("--release", "11"),
    run / fork       := true,
    run / envVars    := localEnv,
    Compile / mainClass := Some("com.bigdata2026.ingestion.Main"),
    libraryDependencies ++= Seq(
      "org.apache.kafka"    %  "kafka-clients"    % kafkaVersion,
      "org.slf4j"           %  "slf4j-simple"     % "2.0.9",
      "com.fasterxml.jackson.core" % "jackson-databind" % "2.15.2"
    )
  )
  .settings(assemblySettings *)

// ── Part 4 — visualization ────────────────────────────────────────────────────

lazy val vizCommon = crossProject(JVMPlatform, JSPlatform)
  .crossType(CrossType.Pure)
  .in(file("visualization/common"))
  .settings(
    name         := "viz-common",
    scalaVersion := scala3,
    libraryDependencies ++= Seq(
      "dev.zio" %%% "zio-json" % "0.7.36",
      "dev.zio" %%% "zio-http" % zioHttpVersion
    )
  )
lazy val vizCommonJVM = vizCommon.jvm
lazy val vizCommonJS  = vizCommon.js

lazy val vizBackend = project
  .in(file("visualization/backend"))
  .dependsOn(vizCommonJVM)
  .settings(
    name                := "viz-backend",
    scalaVersion        := scala3,
    run / fork          := true,
    run / envVars       := Map("CORS_ALLOWED_ORIGINS" -> "http://localhost:9876"),
    Compile / mainClass := Some("com.bigdata2026.backend.Main"),
    libraryDependencies ++= Seq(
      "dev.zio"          %% "zio"                 % zioVersion,
      "dev.zio"          %% "zio-http"            % zioHttpVersion,
      "dev.zio"          %% "zio-logging"         % "2.4.0",
      "dev.zio"          %% "zio-logging-slf4j2"  % "2.4.0",
      "org.apache.hbase" %  "hbase-client"        % hbaseVersion,
      "ch.qos.logback"   %  "logback-classic"     % "1.5.16"
    )
  )
  .settings(assemblySettings *)

lazy val vizFrontend = project
  .in(file("visualization/frontend"))
  .enablePlugins(ScalaJSPlugin)
  .dependsOn(vizCommonJS)
  .settings(
    name                            := "viz-frontend",
    scalaVersion                    := scala3,
    scalaJSUseMainModuleInitializer := true,
    Compile / mainClass             := Some("com.bigdata2026.frontend.Main"),
    scalaJSLinkerConfig ~= (_.withModuleKind(ModuleKind.ESModule)),
    libraryDependencies ++= Seq(
      "io.indigoengine"            %%% "tyrian-zio"       % tyrianVersion,
      "dev.zio"                    %%% "zio-interop-cats" % "23.1.0.3",
      "com.softwaremill.quicklens" %%% "quicklens"        % "1.9.7"
    )
  )
  .settings(
    jsTool := JSToolConfig(
      installPackagesCommand = Cmd.npm.install.withPrefix("visualization/frontend"),
      dev = DevConfig(
        command = Cmd.npm.run("dev").withPrefix("visualization/frontend"),
        startupMessage = "Starting Vite dev server...",
        successMessage = "Dev server ready!  http://localhost:9876"
      ),
      build = BuildConfig(
        command = Cmd.npm.build.withPrefix("visualization/frontend"),
        startupMessage = "Building for production...",
        successMessage = "Build complete!"
      )
    )
  )

// ── Part 5 — Spark Structured Streaming (Scala 2.13) ─────────────────────────
lazy val sparkStreaming = project
  .in(file("spark-streaming"))
  .settings(
    name             := "spark-streaming",
    scalaVersion     := scala213,
    run / fork       := true,
    run / javaOptions ++= Seq(
      "-Dorg.slf4j.simpleLogger.defaultLogLevel=warn",
      "--add-opens=java.base/java.lang=ALL-UNNAMED",
      "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
      "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED",
      "--add-opens=java.base/java.io=ALL-UNNAMED",
      "--add-opens=java.base/java.net=ALL-UNNAMED",
      "--add-opens=java.base/java.nio=ALL-UNNAMED",
      "--add-opens=java.base/java.util=ALL-UNNAMED",
      "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED",
      "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED",
      "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
      "--add-opens=java.base/sun.nio.cs=ALL-UNNAMED",
      "--add-opens=java.base/sun.security.action=ALL-UNNAMED",
      "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED",
      "--add-opens=java.security.jgss/sun.security.krb5=ALL-UNNAMED",
    ),
    run / envVars    := localEnv,
    Compile / mainClass := Some("com.bigdata2026.spark.RepoStatsJob"),
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core"           % sparkVersion,
      "org.apache.spark" %% "spark-sql"            % sparkVersion,
      "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion,
      ("org.apache.hbase"  % "hbase-client"         % hbaseVersion)
        .excludeAll(ExclusionRule(organization = "org.apache.hadoop")),
      "org.slf4j"         % "slf4j-simple"         % "2.0.9"
    )
  )
  .settings(assemblySettings *)

// ── Root aggregator ───────────────────────────────────────────────────────────
addCommandAlias("p1", "ingestion/run")

// p1dev: ingestion → local Kafka (keeps GITHUB_TOKEN from local.env, overrides Kafka to localhost)
commands += Command.command("p1dev") { state =>
  val env    = localKafkaEnv
  val envStr = env.map { case (k, v) => s""""$k" -> "$v"""" }.mkString(", ")
  s"set ingestion/run/envVars := Map($envStr)" :: "ingestion/run" :: state
}
addCommandAlias("p3", "sparkStreaming/run")
addCommandAlias("bs", "vizBackend/reStart")
addCommandAlias("bx", "vizBackend/reStop")
addCommandAlias("fdev", "vizFrontend/dev")
addCommandAlias("fdst", "vizFrontend/publishDist")

lazy val root = project
  .in(file("."))
  .aggregate(ingestion, sparkStreaming, vizCommonJVM, vizCommonJS, vizBackend, vizFrontend)
  .settings(
    name           := "bigdata2026-final",
    publish / skip := true
  )
