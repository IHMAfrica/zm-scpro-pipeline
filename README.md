CarePro Streaming Pipeline (Local Dev)

Source → Kafka → PyFlink transform/dedup → Postgres

This project ingests records from SQL Server into Kafka, cleans/enriches them with PyFlink, de-duplicates, and upserts into Postgres — all running locally with Docker.

Architecture
+-------------+           +-------------------------+           +------------------------+
| SQL Server  | --pymssql->   producer-* (python)   --Kafka-->  |  Kafka (Confluent)     |
+-------------+           +-------------------------+           +-----------+------------+
                                                                         |
                                                                         v
                                                             +-----------+-----------+
                                                             |      PyFlink (1.17)   |
                                                             |  transform + dedup    |
                                                             +-----------+-----------+
                                                                         |
                                                                         v
                                                             +-----------+-----------+
                                                             |   Postgres (local)    |
                                                             |  public.art_cohort    |
                                                             +-----------------------+


Kafka topic: carepro.art_cohort

Flink job: /opt/flink/usrlib/job/main.py

uses transforms_art_cohort.py (and utils/schema_utils.py)

converts artnumber → a normalized key art_number = COALESCE(artnumber, '__NULL__') so upserts also work when artnumber is null.

Postgres target table: public.art_cohort

PRIMARY KEY on (patientid, art_number)

art_number is a generated column in Postgres: COALESCE(artnumber, '__NULL__')

Repository layout (important bits)
docker/
  flink/
    Dockerfile               # Flink runtime + Python + connectors + avro pin
flink/
  requirements.txt           # PyFlink + runtime deps for the job
  job/
    main.py                  # PyFlink pipeline (Kafka -> clean/dedup -> JDBC sink)
    transforms_art_cohort.py # transform logic (absolute imports)
    utils/
      schema_utils.py        # date/number/flags/cohorts helpers
producer-art/
  producer_art.py            # SQL Server -> Kafka
producer-interactions/
  producer_interactions.py   # (optional second producer)
docker-compose.yml
.env


Names/paths can vary slightly; match yours accordingly.

Prereqs

Docker Desktop (Windows/macOS) or Docker Engine (Linux)

Postgres running on your host machine (Windows in your case)

In .env/compose we point to host.docker.internal:5432

DB: carepro_messages (or your DB)

PowerShell on Windows (watch quoting/escaping; examples below are PowerShell-friendly)

Configuration

.env (example)

# Flink
FLINK_PARALLELISM=2
FLINK_CHECKPOINT_INTERVAL_MS=60000

# Kafka
KAFKA_BOOTSTRAP_SERVERS=kafka:9092
KAFKA_TOPIC_ART=carepro.art_cohort

# Postgres (host machine)
PG_HOST=host.docker.internal
PG_PORT=5432
PG_DB=carepro_messages
PG_USER=postgres
PG_PASSWORD=1234
PG_SSLMODE=disable


SASL/SSL for Kafka are optional; leave empty for local PLAINTEXT.

Build & Run
# 1) Build
docker compose build

# 2) Start infra and producers
docker compose up -d zookeeper kafka schema-registry producer-art producer-interactions

# 3) Start Flink cluster
docker compose up -d flink-jobmanager flink-taskmanager


Flink UI: http://localhost:8081

Submit the job
docker compose exec flink-jobmanager bash -lc `
  "flink run -py /opt/flink/usrlib/job/main.py -d"


If a previous run exists and you want to restart from scratch, cancel it in the UI or:

docker compose exec flink-jobmanager bash -lc "curl -s http://localhost:8081/jobs/overview"
# find JobID, then
docker compose exec flink-jobmanager bash -lc "flink cancel <job-id>"

Verify end-to-end
1) Kafka topic exists & has data
docker compose exec kafka bash -lc "kafka-topics --bootstrap-server kafka:9092 --list"
docker compose exec kafka bash -lc "kafka-topics --bootstrap-server kafka:9092 --describe --topic carepro.art_cohort"
docker compose exec kafka bash -lc "kafka-console-consumer --bootstrap-server kafka:9092 --topic carepro.art_cohort --from-beginning --max-messages 3"

2) Postgres is receiving rows

Use psql/pgAdmin, then:

-- PK is on (patientid, art_number)
\d public.art_cohort

-- Some data?
SELECT COUNT(*) FROM public.art_cohort;

-- Rows where artnumber was NULL are preserved (art_number = '__NULL__'):
SELECT patientid, artnumber, art_number
FROM public.art_cohort
WHERE artnumber IS NULL
LIMIT 10;


The Flink job auto-ensures public.art_cohort exists, adds art_number generated column, and sets the PK if missing.

Key design points (what we fixed and why)

NULL artnumber without dropping rows

Flink sink needs a deterministic key for upserts.

We don’t drop NULLs. Instead we generate art_number = COALESCE(artnumber, '__NULL__').

Postgres PK is (patientid, art_number).

ON CONFLICT alignment

Sink uses ON CONFLICT (patientid, art_number); we ensure the physical table has that exact PK.

Avro/Beam compatibility

Pin avro-python3==1.8.1 inside the Flink image to satisfy the Beam worker used by PyFlink 1.17.

Avoid mixing avro and avro-python3 versions that cause ImportError: Validate or ModuleNotFoundError.

Imports in job code

Use absolute imports in transforms_art_cohort.py (e.g., from utils.schema_utils import …).

Ensure PYTHONPATH=/opt/flink/usrlib/job in both JM/TM so Flink’s Python worker can import your modules.

Windows & PowerShell quirks

Avoid line-continuation \ in PowerShell; prefer one-liners or PowerShell backticks as shown.

Use host.docker.internal to reach your host Postgres from Docker on Windows.

If you mount paths read-only (:ro), keep them read-only in compose to avoid “read-only file system” errors.

Resources / slots

If you see NoResourceAvailableException, increase slots (e.g., taskmanager.numberOfTaskSlots) or reduce parallelism.

Troubleshooting (common errors & fixes)

“Waiting for kafka:9092” forever
Ensure KAFKA_BOOTSTRAP_SERVERS=kafka:9092 is set in producer env and your wait script resolves the host. Don’t put backslashes in PowerShell commands.

invalid interpolation format in compose
If you use $VAR inside command:, wrap the command in bash -lc and double the $ like $$VAR or read from env.

ImportError: attempted relative import with no known parent package
Ensure absolute imports and PYTHONPATH=/opt/flink/usrlib/job in JM/TM.

Avro errors (Validate, ModuleNotFoundError: avro)
Use avro-python3==1.8.1 in the Flink image and avoid installing the avro package that conflicts for PyFlink 1.17.

NOT NULL enforcement in sink
If Flink complains about nulls, either audit the pipeline or adjust sink settings. We solved artnumber specifically via art_number.

ON CONFLICT fails (no unique or exclusion constraint)
The target PK/unique index must match the ON CONFLICT columns exactly. Our job ensures the PK is (patientid, art_number).

Build details & dependencies
Docker images

Flink: flink:1.17.1-scala_2.12-java11 (customized in docker/flink/Dockerfile)

Kafka: confluentinc/cp-kafka:7.3.3

Zookeeper: confluentinc/cp-zookeeper:7.3.3

Schema Registry: confluentinc/cp-schema-registry:7.3.3

Producer base: python:3.10-slim

Build helper: curlimages/curl:8.8.0 (to fetch connector JARs)

Flink connector JARs (copied into /opt/flink/lib/)

flink-sql-connector-kafka-1.17.1.jar

flink-connector-jdbc-3.1.1-1.17.jar

postgresql-42.7.3.jar

flink-avro-confluent-registry-1.17.1.jar

OS packages installed in Flink image

python3, python3-pip, python-is-python3, curl, jq

Python packages (Flink image via flink/requirements.txt)

pyflink==1.17.*

avro-python3==1.8.1 ← critical for Beam/PyFlink 1.17 worker

psycopg2-binary ← to create/repair the Postgres table automatically

(plus any other utils you include)

Producer Python packages

pymssql==2.2.8

confluent-kafka==2.5.0

OS: netcat-openbsd (used by the wait-for script)

Operational tips

Reprocessing from beginning
Set consumer group to a new value or delete consumer offsets for the group, and use --from-beginning to sample.

Throughput tuning

Increase FLINK_PARALLELISM, slots in TaskManager, and sink.buffer-flush.*.

Consider batching in the producers (larger Kafka batches).

Observability

Flink UI: operators, backpressure, task logs.

docker compose logs -f <service>

Kafka metrics via kafka-run-class kafka.tools.JmxTool … (optional)

Security / PII

This pipeline handles patient-level data. For any non-local environment:

Use authenticated Kafka (SASL/SSL), and secure Postgres (TLS, limited network access).

Store secrets outside of .env, e.g., Docker secrets/KeyVault, etc.

Add topic-level ACLs, schema validation, and retention policies appropriately.

License

