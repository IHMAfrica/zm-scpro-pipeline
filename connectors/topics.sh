#!/usr/bin/env bash
set -euo pipefail

BOOT="${KAFKA_BOOTSTRAP:-kafka:9092}"
SEC="${KAFKA_SECURITY_PROTOCOL:-PLAINTEXT}"
MECH="${KAFKA_SASL_MECHANISM:-}"
USER="${KAFKA_SASL_USER:-${KAFKA_SASL_USERNAME:-}}"
PASS="${KAFKA_SASL_PASSWORD:-}"

CMD=(kafka-topics --bootstrap-server "$BOOT")

# Build optional --command-config only for SASL modes
if [[ "$SEC" == SASL_* ]]; then
  tmpconf=$(mktemp)
  {
    echo "security.protocol=$SEC"
    [[ -n "$MECH" ]] && echo "sasl.mechanism=$MECH"
    [[ -n "$USER" ]] && echo "sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username=$USER password=$PASS;"
  } > "$tmpconf"
  trap 'rm -f "$tmpconf"' EXIT
  CMD+=("--command-config" "$tmpconf")
fi

# Compact topics for curated outputs
"${CMD[@]}" --create --if-not-exists \
  --topic carepro.curated.art_cohort --partitions 3 --replication-factor 1 \
  --config cleanup.policy=compact

"${CMD[@]}" --create --if-not-exists \
  --topic carepro.curated.all_interactions --partitions 3 --replication-factor 1 \
  --config cleanup.policy=compact

echo "Topic creation done on $BOOT ($SEC)"
