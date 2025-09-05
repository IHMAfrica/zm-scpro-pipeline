#!/usr/bin/env bash
set -euo pipefail
FLINK_LIB_DIR=${FLINK_LIB_DIR:-/opt/flink/lib}
FLINK_VERSION=1.17.1
MIRRORS=(
  "https://archive.apache.org/dist/flink/flink-${FLINK_VERSION}"
  "https://dlcdn.apache.org/flink/flink-${FLINK_VERSION}"
)
JARS=(
  "flink-sql-connector-kafka-${FLINK_VERSION}.jar"
  "lib/flink-connector-kafka-${FLINK_VERSION}.jar"
  "lib/flink-json-${FLINK_VERSION}.jar"
  "lib/flink-avro-confluent-${FLINK_VERSION}.jar"
)
mkdir -p "${FLINK_LIB_DIR}"
for j in "${JARS[@]}"; do
  for base in "${MIRRORS[@]}"; do
    url="${base}/${j}"
    file="${FLINK_LIB_DIR}/$(basename "${j}")"
    echo "Fetching ${url}"
    if curl -fsSL "${url}" -o "${file}"; then
      echo "Saved ${file}"; break
    fi
  done
done
ls -l "${FLINK_LIB_DIR}" || true
