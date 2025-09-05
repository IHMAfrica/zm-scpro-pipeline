#!/usr/bin/env bash
set -euo pipefail

dest="/opt/flink/lib"
mkdir -p "$dest"
cd "$dest"

dl() { curl -fL --retry 5 --retry-delay 2 -o "$(basename "$1")" "$1"; }

# Kafka SQL connector (Flink 1.17.x)
dl "https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/1.17.1/flink-sql-connector-kafka-1.17.1.jar"

# JDBC connector + Postgres driver
dl "https://repo1.maven.org/maven2/org/apache/flink/flink-connector-jdbc/3.1.1-1.17/flink-connector-jdbc-3.1.1-1.17.jar"
dl "https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.3/postgresql-42.7.3.jar"

# Confluent Avro registry format
dl "https://repo1.maven.org/maven2/org/apache/flink/flink-avro-confluent-registry/1.17.1/flink-avro-confluent-registry-1.17.1.jar"
