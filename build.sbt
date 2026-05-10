import org.scalajs.linker.interface.ModuleKind
import com.w47s0n.scalajscli.ScalaJsCli.autoImport._

ThisBuild / organization := "com.bigdata2026"
ThisBuild / version      := "0.1.0-SNAPSHOT"

val scala3 = "3.7.3"
val scala2 = "2.12.18"

val sparkVersion    = "3.5.1"
val kafkaVersion    = "3.2.3"
val hbaseVersion    = "2.1.10"
val zioVersion      = "2.1.16"
val zioHttpVersion  = "3.8.0"
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

// ── Shared schema contract ────────────────────────────────────────────────────
lazy val schema = project
  .in(file("schema"))
  .settings(
    name             := "schema",
    autoScalaLibrary := false,
    crossPaths       := false,
    javacOptions ++= Seq("--release", "11")
  )

// ── Part 1 — ingestion (Java 11) ─────────────────────────────────────────────
lazy val ingestion = project
  .in(file("ingestion"))
  .dependsOn(schema)
  .settings(
    name             := "ingestion",
    autoScalaLibrary := false,
    crossPaths       := false,
    javacOptions ++= Seq("--release", "11"),
    Compile / mainClass := Some("com.bigdata2026.ingestion.Main"),
    libraryDependencies ++= Seq(
      "org.apache.kafka"    %  "kafka-clients"    % kafkaVersion,
      "org.slf4j"           %  "slf4j-simple"     % "2.0.9",
      "com.fasterxml.jackson.core" % "jackson-databind" % "2.15.2"
    )
  )
  .settings(assemblySettings *)

// ── Part 2/3/5 — streaming (Scala 2.12 + Spark 3.1.2) ───────────────────────
lazy val streaming = project
  .in(file("streaming"))
  .dependsOn(schema)
  .settings(
    name                := "streaming",
    scalaVersion        := scala2,
    run / fork          := true,
    javaOptions ++= Seq(
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
      "--add-opens=java.security.jgss/sun.security.krb5=ALL-UNNAMED"
    ),
    Compile / mainClass := Some("com.bigdata2026.streaming.Main"),
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core"           % sparkVersion,
      "org.apache.spark" %% "spark-sql"            % sparkVersion,
      "org.apache.spark" %% "spark-streaming"      % sparkVersion,
      "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion,
      "org.apache.hbase"  % "hbase-client"         % hbaseVersion
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

// ── Root aggregator ───────────────────────────────────────────────────────────
addCommandAlias("p1", "ingestion/run")
addCommandAlias("p2", "streaming/run")
addCommandAlias("bs", "vizBackend/reStart")
addCommandAlias("bx", "vizBackend/reStop")
addCommandAlias("fdev", "vizFrontend/dev")
addCommandAlias("fdst", "vizFrontend/publishDist")

lazy val root = project
  .in(file("."))
  .aggregate(schema, ingestion, streaming, vizCommonJVM, vizCommonJS, vizBackend, vizFrontend)
  .settings(
    name           := "bigdata2026-final",
    publish / skip := true
  )
