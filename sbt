#!/usr/bin/env bash
# Self-bootstrapping sbt launcher — no global sbt install required.
# Downloads sbt-launch.jar once, caches it next to this script.
set -euo pipefail

SBT_VERSION="1.12.10"
URL="https://repo1.maven.org/maven2/org/scala-sbt/sbt-launch/${SBT_VERSION}/sbt-launch-${SBT_VERSION}.jar"
DIR="$(cd "$(dirname "$0")" && pwd)"
JAR="$DIR/.sbt-launch-${SBT_VERSION}.jar"

if [ ! -f "$JAR" ]; then
  echo "[sbt-bootstrap] Downloading sbt-launch ${SBT_VERSION}..."
  if command -v curl &>/dev/null; then
    curl -fsSL "$URL" -o "$JAR"
  elif command -v wget &>/dev/null; then
    wget -q "$URL" -O "$JAR"
  else
    echo "ERROR: curl or wget is required to bootstrap sbt" >&2
    exit 1
  fi
  echo "[sbt-bootstrap] Done."
fi

exec java ${JAVA_OPTS:-} -jar "$JAR" "$@"
