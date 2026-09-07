#!/usr/bin/env bash
# Demarrage du broker Kafka en KRaft, mono-noeud, sans droits admin.
# A utiliser a partir de la phase 1 du TODO-KAFKA.
#
# Prerequis : Kafka 4.3.1 decompresse et KAFKA_HOME renseigne.
# Le formatage du stockage (une seule fois) est decrit dans le dossier technique.
set -euo pipefail

KAFKA_HOME="${KAFKA_HOME:-$HOME/dev/kafka_2.13-4.3.1}"

if [ ! -d "$KAFKA_HOME" ]; then
  echo "KAFKA_HOME introuvable : $KAFKA_HOME" >&2
  echo "Decompresse kafka_2.13-4.3.1.tgz puis exporte KAFKA_HOME." >&2
  exit 1
fi

echo "Demarrage du broker depuis $KAFKA_HOME (Ctrl+C pour arreter)"
exec "$KAFKA_HOME/bin/kafka-server-start.sh" "$KAFKA_HOME/config/server.properties"
