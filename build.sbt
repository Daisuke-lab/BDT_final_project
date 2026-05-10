import org.scalajs.linker.interface.ModuleKind

ThisBuild / organization := "com.bigdata2026"
ThisBuild / version      := "0.1.0-SNAPSHOT"

val scala3 = "3.7.3"
val scala2 = "2.12.18"

val sparkVersion  = "3.1.2"
val kafkaVersion  = "3.2.3"
val hbaseVersion  = "2.1.10"
val zioVersion    = "2.1.16"
val tapirVersion  = "1.11.40"
val tyrianVersion = "0.14.0"

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
    name := "ingestion",
    autoScalaLibrary := false,
    crossPaths       := false,
    javacOptions ++= Seq("--release", "11"),
    Compile / mainClass := Some("com.bigdata2026.ingestion.Main"),
    libraryDependencies ++= Seq(
      "org.apache.kafka" %  "kafka-clients" % kafkaVersion,
      "org.slf4j"        %  "slf4j-simple"  % "2.0.9"
    )
  )
  .settings(assemblySettings*)

// ── Part 2/3/5 — streaming (Scala 2.12 + Spark 3.1.2) ───────────────────────
lazy val streaming = project
  .in(file("streaming"))
  .settings(
    name         := "streaming",
    scalaVersion := scala2,
    Compile / mainClass := Some("com.bigdata2026.streaming.Main"),
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core"           % sparkVersion,
      "org.apache.spark" %% "spark-sql"            % sparkVersion,
      "org.apache.spark" %% "spark-streaming"      % sparkVersion,
      "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion,
      "org.apache.hbase" %  "hbase-client"         % hbaseVersion
    )
  )
  .settings(assemblySettings*)

// ── Part 4 — visualization ────────────────────────────────────────────────────

lazy val vizCommon = crossProject(JVMPlatform, JSPlatform)
  .crossType(CrossType.Pure)
  .in(file("visualization/common"))
  .settings(
    name         := "viz-common",
    scalaVersion := scala3,
    libraryDependencies ++= Seq(
      "dev.zio"                     %%% "zio-json"       % "0.7.36",
      "com.softwaremill.sttp.tapir" %%% "tapir-core"     % tapirVersion,
      "com.softwaremill.sttp.tapir" %%% "tapir-json-zio" % tapirVersion
    )
  )
lazy val vizCommonJVM = vizCommon.jvm
lazy val vizCommonJS  = vizCommon.js

lazy val vizBackend = project
  .in(file("visualization/backend"))
  .dependsOn(vizCommonJVM)
  .settings(
    name         := "viz-backend",
    scalaVersion := scala3,
    Compile / mainClass := Some("com.bigdata2026.backend.Main"),
    libraryDependencies ++= Seq(
      "dev.zio"                     %% "zio"                     % zioVersion,
      "com.softwaremill.sttp.tapir" %% "tapir-zio-http-server"   % tapirVersion,
      "com.softwaremill.sttp.tapir" %% "tapir-swagger-ui-bundle" % tapirVersion,
      "org.apache.hbase"            %  "hbase-client"            % hbaseVersion,
      "ch.qos.logback"              %  "logback-classic"         % "1.5.16"
    )
  )
  .settings(assemblySettings*)

lazy val vizFrontend = project
  .in(file("visualization/frontend"))
  .enablePlugins(ScalaJSPlugin)
  .dependsOn(vizCommonJS)
  .settings(
    name         := "viz-frontend",
    scalaVersion := scala3,
    scalaJSUseMainModuleInitializer := true,
    Compile / mainClass := Some("com.bigdata2026.frontend.Main"),
    scalaJSLinkerConfig ~= (_.withModuleKind(ModuleKind.ESModule)),
    libraryDependencies ++= Seq(
      "io.indigoengine"               %%% "tyrian-zio"       % tyrianVersion,
      "dev.zio"                       %%% "zio-interop-cats" % "23.1.0.3",
      "com.softwaremill.sttp.client4" %%% "zio"              % "4.0.0"
    )
  )

// ── Root aggregator ───────────────────────────────────────────────────────────
lazy val root = project
  .in(file("."))
  .aggregate(ingestion, streaming, vizCommonJVM, vizCommonJS, vizBackend, vizFrontend)
  .settings(
    name           := "bigdata2026-final",
    publish / skip := true
  )
