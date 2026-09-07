#!/usr/bin/env bash
# A LANCER UNE SEULE FOIS, avant le tout premier demarrage du broker.
# Kafka 4.x fonctionne uniquement en KRaft : le repertoire de logs doit etre
# formate avec un identifiant de cluster.
set -euo pipefail

KAFKA_HOME="${KAFKA_HOME:-$HOME/dev/kafka_2.13-4.3.1}"
cd "$KAFKA_HOME"

CLUSTER_ID="$(bin/kafka-storage.sh random-uuid)"
echo "Cluster ID : $CLUSTER_ID"

bin/kafka-storage.sh format --standalone -t "$CLUSTER_ID" -c config/server.properties
echo "Formatage termine. Tu peux lancer start-kafka.sh."
