#!/usr/bin/env bash
# Interface web Kafka (AKHQ 0.28.0) — un simple JAR, aucune installation.
# Telecharge akhq-0.28.0-all.jar depuis les releases GitHub de tchiotludo/akhq.
set -euo pipefail

AKHQ_HOME="${AKHQ_HOME:-$HOME/dev/akhq}"
cd "$AKHQ_HOME"

exec java -Dmicronaut.config.files=application.yml -jar akhq-0.28.0-all.jar
