#!/usr/bin/env bash
set -euo pipefail
/scripts/wait-for.sh connect 8083
bash /connectors/register.sh
bash /connectors/topics.sh
# Submit PyFlink job
curl -s http://flink-jobmanager:8081/
python3 - <<'PY'
import os, subprocess, json
job_py = '/opt/flink/usrlib/job/main.py'
print('Submitting PyFlink job ...')
subprocess.check_call(['flink','run','-m','flink-jobmanager:8081','-py', job_py])
PY