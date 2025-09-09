# -*- coding: utf-8 -*-
"""
Kafka -> Clean -> Dedup -> Postgres (art_cohort only)
"""

import json
import hashlib
import os
from typing import Tuple, Dict, Any

from pyflink.datastream import StreamExecutionEnvironment, CheckpointingMode
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream.connectors import FlinkKafkaConsumer
from pyflink.common.typeinfo import Types
from pyflink.datastream.state import ValueStateDescriptor
from pyflink.datastream.functions import KeyedProcessFunction, RuntimeContext

# Table API for JDBC sink
from pyflink.table import StreamTableEnvironment, Schema, DataTypes
from pyflink.common import Row

from transforms_art_cohort import transform, REQUIRED_COLS

# -----------------------------------------------------------------------------
# Config
# -----------------------------------------------------------------------------
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC_ART", "carepro.art_cohort")
BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
PARALLELISM = int(os.getenv("FLINK_PARALLELISM", "2"))
CP_MS = int(os.getenv("FLINK_CHECKPOINT_INTERVAL_MS", "60000"))

PG_HOST = os.getenv("PG_HOST", "localhost")
PG_PORT = os.getenv("PG_PORT", "5432")
PG_DB = os.getenv("PG_DB", "postgres")
PG_USER = os.getenv("PG_USER", "postgres")
PG_PASSWORD = os.getenv("PG_PASSWORD", "postgres")
PG_SSLMODE = os.getenv("PG_SSLMODE", "disable")  # "disable" for local

# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------
def md5(s: str) -> str:
    return hashlib.md5(s.encode("utf-8")).hexdigest()

def bt(name: str) -> str:
    """Backtick-quote a Flink identifier (for spaces, mixed case, etc.)."""
    return f"`{name.replace('`', '``')}`"

class DedupByKeyHash(KeyedProcessFunction):
    def open(self, runtime_context: RuntimeContext):
        self.seen = runtime_context.get_state(
            ValueStateDescriptor("last_hash", Types.STRING())
        )

    def process_element(self, value: Tuple[str, str], ctx):
        key, payload = value
        h = md5(payload)
        prev = self.seen.value()
        if prev != h:
            self.seen.update(h)
            yield value

# -----------------------------------------------------------------------------
# Job
# -----------------------------------------------------------------------------
def main():
    # 1) DataStream env + Kafka source
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(PARALLELISM)
    env.enable_checkpointing(CP_MS, CheckpointingMode.EXACTLY_ONCE)

    props = {
        "bootstrap.servers": BOOTSTRAP,
        "group.id": "carepro-art-cohort-consumer",
        "auto.offset.reset": "earliest",
        "isolation.level": "read_committed",
    }

    consumer = FlinkKafkaConsumer([KAFKA_TOPIC], SimpleStringSchema(), props)
    src = env.add_source(consumer).name("kafka-art-cohort")

    # Map JSON -> cleaned dict -> project REQUIRED_COLS, build composite key
    def clean_map(s: str) -> Tuple[str, str]:
        try:
            raw: Dict[str, Any] = json.loads(s)
        except Exception:
            raw = {}
        cleaned = transform(raw)
        projected = {c: cleaned.get(c) for c in REQUIRED_COLS}
        pid = str(projected.get("patientid") or "").strip()
        artn = str(projected.get("artnumber") or "").strip()
        key = f"{pid}|{artn}"
        return (key, json.dumps(projected, default=str))

    cleaned = src.map(
        clean_map,
        output_type=Types.TUPLE([Types.STRING(), Types.STRING()])
    ).name("clean-transform")

    # Stateful dedup by (patientid|artnumber) + payload hash
    deduped = cleaned.key_by(
        lambda kv: kv[0], key_type=Types.STRING()
    ).process(
        DedupByKeyHash(),
        output_type=Types.TUPLE([Types.STRING(), Types.STRING()])
    ).name("dedup-stateful")

    # Convert (key, json) -> Row with columns in REQUIRED_COLS order
    row_type = Types.ROW_NAMED(REQUIRED_COLS, [Types.STRING()] * len(REQUIRED_COLS))

    def to_row(kv: Tuple[str, str]) -> Row:
        _, js = kv
        if isinstance(js, (bytes, bytearray)):
            js = js.decode("utf-8", errors="replace")
        d = json.loads(js)
        return Row(*[(None if d.get(c) is None else str(d.get(c))) for c in REQUIRED_COLS])

    rows_stream = deduped.map(to_row, output_type=row_type).name("json->row")

    # 2) Table API: JDBC sink to Postgres
    t_env = StreamTableEnvironment.create(env)

    # Build explicit schema to avoid alias() issues with odd field names
    sb = Schema.new_builder()
    for c in REQUIRED_COLS:
        sb.column(c, DataTypes.STRING())
    tbl_schema = sb.build()
    events_tbl = t_env.from_data_stream(rows_stream, tbl_schema)

    # Flink sink DDL with backticked identifiers
    pg_url = f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DB}"
    cols_ddl = ", ".join([f"{bt(c)} STRING" for c in REQUIRED_COLS])
    ddl = f"""
    CREATE TABLE IF NOT EXISTS art_sink (
      {cols_ddl},
      PRIMARY KEY ({bt("patientid")}, {bt("artnumber")}) NOT ENFORCED
    ) WITH (
      'connector' = 'jdbc',
      'url' = '{pg_url}',
      'table-name' = 'carepro.art_cohort',
      'driver' = 'org.postgresql.Driver',
      'username' = '{PG_USER}',
      'password' = '{PG_PASSWORD}',
      'sink.buffer-flush.max-rows' = '1000',
      'sink.buffer-flush.interval' = '3s',
      'sink.max-retries' = '3'
    )
    """
    t_env.execute_sql(ddl)

    # Optionally ensure physical PG table exists (requires psycopg2-binary in image)
    try:
        import psycopg2
        def _qident(n: str) -> str:
            return '"' + n.replace('"', '""') + '"'
        cols_sql = ", ".join(f'{_qident(c)} TEXT' for c in REQUIRED_COLS)
        pk_sql = f'PRIMARY KEY ({_qident("patientid")}, {_qident("artnumber")})'
        conn = psycopg2.connect(
            host=PG_HOST, port=PG_PORT, dbname=PG_DB,
            user=PG_USER, password=PG_PASSWORD, sslmode=PG_SSLMODE
        )
        try:
            conn.autocommit = True
            cur = conn.cursor()
            cur.execute('CREATE SCHEMA IF NOT EXISTS "carepro"')
            cur.execute(f'CREATE TABLE IF NOT EXISTS "carepro"."art_cohort" ({cols_sql}, {pk_sql})')
            cur.close()
        finally:
            conn.close()
    except Exception as e:
        print(f"[WARN] Could not ensure PG table automatically: {e}")

    # Start the sink
    stmt = t_env.create_statement_set()
    stmt.add_insert("art_sink", events_tbl)
    stmt.execute()  # detached when you run `flink run -d`

if __name__ == "__main__":
    main()
