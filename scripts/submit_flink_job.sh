#!/usr/bin/env bash
set -euo pipefail
/opt/scripts/fetch_jars.sh || true
/opt/scripts/wait_for_it.sh jobmanager:8081 -t 120 -- echo "Flink up."
cd /opt/jobs
zip -r /opt/jobs/flink_deps.zip sinks || true
flink run -d -py /opt/jobs/flink_job.py --pyFiles /opt/jobs/flink_deps.zip
echo "Job submitted."
tail -f /dev/null
