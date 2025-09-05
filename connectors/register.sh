#!/usr/bin/env bash
set -euo pipefail
curl -s -X POST -H "Content-Type: application/json" \
--data @/connectors/debezium-sqlserver-carepro.json \
http://connect:8083/connectors | jq .