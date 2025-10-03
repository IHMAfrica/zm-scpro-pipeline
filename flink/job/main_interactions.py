# -*- coding: utf-8 -*-
"""
Kafka -> Clean -> Dedup -> Postgres (carepro_all_interactions)
- Upsert key: interactionid (NOT ENFORCED in Flink)
- No DB DDL/constraint management in code
- Applies curate_interactions() to add service_category + year/month
"""

import json
import hashlib
import os
from typing import Tuple, Dict, Any, List

from pyflink.datastream import StreamExecutionEnvironment, CheckpointingMode
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream.connectors import FlinkKafkaConsumer
from pyflink.common.typeinfo import Types
from pyflink.datastream.state import ValueStateDescriptor
from pyflink.datastream.functions import KeyedProcessFunction, RuntimeContext

from pyflink.table import StreamTableEnvironment, Schema, DataTypes
from pyflink.table.expressions import col, lit
from pyflink.common import Row

from transforms_interactions import curate_interactions  # your SERVICE_MAP logic

# Columns normalized to snake_case; includes derived columns at the end for the sink
REQUIRED_COLS: List[str] = [
    "interactionid",
    "patientid",
    "carepropatientid",
    "artnumber",
    "sex",
    "dob",
    "firstname",
    "surname",
    "nupn",
    "useraccountid",
    "interactiontime",
    "interactiondate",
    "nextvisitdate",
    "interactionprovider",
    "facilityname",
    "careprofacilityid",
    "district",
    "province",   # normalize "Provience" -> "province" upstream
    "servicename",
    "artstartdate",
    "currentvlcopies",
    "currentvldate",
    "cd4count",
    "cd4countdate",
    "cd4percent",
    "cd4percentdate",
    "currentartregimen",
    "currentartregimendispensationdate",
    "nameoftheprescribeddrug",
    "prescriptiondate",
    "dsd",
    "lasttestresult",
    "determinetestresult",
    "biolinetestresult",
    # derived by curate_interactions()
    "service_category",
    "interaction_year",
    "interaction_month",
]

# -----------------------------------------------------------------------------
# Config via env
# -----------------------------------------------------------------------------
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC_INTERACTIONS", "carepro.all_interactions")
BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
PARALLELISM = int(os.getenv("FLINK_PARALLELISM", "2"))
CP_MS = int(os.getenv("FLINK_CHECKPOINT_INTERVAL_MS", "60000"))

PG_HOST = os.getenv("PG_HOST", "localhost")
PG_PORT = os.getenv("PG_PORT", "5432")
PG_DB = os.getenv("PG_DB", "postgres")   # set to 'carepro_messages' in env
PG_USER = os.getenv("PG_USER", "postgres")
PG_PASSWORD = os.getenv("PG_PASSWORD", "1234")

# -----------------------------------------------------------------------------
def md5(s: str) -> str:
    return hashlib.md5(s.encode("utf-8")).hexdigest()

def bt(name: str) -> str:
    return f"`{name.replace('`', '``')}`"

# SQL Server column aliases to our canonical sink names
ALIASES = {
    "interactionid": ["interactionid", "interaction_id", "interactionId", "InteractionId"],
    "patientid": ["patientid", "patient_id", "PatientId"],
    "carepropatientid": ["carepropatientid", "CareProPatientId"],
    "artnumber": ["artnumber", "ARTNumber"],
    "sex": ["sex", "Sex"],
    "dob": ["dob", "DOB"],
    "firstname": ["firstname", "FirstName"],
    "surname": ["surname", "Surname"],
    "nupn": ["nupn", "NUPN"],
    "useraccountid": ["useraccountid", "UserAccountId"],
    "interactiontime": ["interactiontime", "InteractionTime"],
    "interactiondate": ["interactiondate", "InteractionDate"],
    "nextvisitdate": ["nextvisitdate", "NextVisitDate"],
    "interactionprovider": ["interactionprovider", "InteractionProvider"],
    "facilityname": ["facilityname", "FacilityName"],
    "careprofacilityid": ["careprofacilityid", "careProFacilityId", "CareProFacilityId"],
    "district": ["district", "District"],
    "province": ["province", "Provience", "Province"],  # normalize misspelling
    "servicename": ["servicename", "ServiceName"],
    "artstartdate": ["artstartdate", "ARTStartDate"],
    "currentvlcopies": ["currentvlcopies", "CurrentVLCopies"],
    "currentvldate": ["currentvldate", "CurrentVLDate"],
    "cd4count": ["cd4count", "CD4Count"],
    "cd4countdate": ["cd4countdate", "CD4CountDate"],
    "cd4percent": ["cd4percent", "CD4Percent"],
    "cd4percentdate": ["cd4percentdate", "CD4PercentDate"],
    "currentartregimen": ["currentartregimen", "CurrentARTRegimen"],
    "currentartregimendispensationdate": [
        "currentartregimendispensationdate", "CurrentARTRegimenDispensationDate"
    ],
    "nameoftheprescribeddrug": ["nameoftheprescribeddrug", "NameofthePrescribedDrug"],
    "prescriptiondate": ["prescriptiondate", "PrescriptionDate"],
    "dsd": ["dsd", "DSD"],
    "lasttestresult": ["lasttestresult", "LastTestResult"],
    "determinetestresult": ["determinetestresult", "DetermineTestResult"],
    "biolinetestresult": ["biolinetestresult", "BiolineTestResult"],
}

REV = {}
for canon, alts in ALIASES.items():
    for a in alts:
        REV[a.lower()] = canon

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
def main():
    # 1) DataStream env + Kafka source
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(PARALLELISM)
    env.enable_checkpointing(CP_MS, CheckpointingMode.EXACTLY_ONCE)

    props = {
        "bootstrap.servers": BOOTSTRAP,
        "group.id": "carepro-all-interactions-consumer",
        "auto.offset.reset": "earliest",
        "isolation.level": "read_committed",
    }

    consumer = FlinkKafkaConsumer([KAFKA_TOPIC], SimpleStringSchema(), props)
    src = env.add_source(consumer).name("kafka-all-interactions")

    # Map JSON -> normalized dict -> JSON (project only REQUIRED_COLS base set)
    base_cols = [c for c in REQUIRED_COLS if c not in ("service_category", "interaction_year", "interaction_month")]

    def clean_map(s: str) -> Tuple[str, str]:
        try:
            raw: Dict[str, Any] = json.loads(s)
        except Exception:
            raw = {}

        # normalize keys and resolve aliases
        tmp: Dict[str, Any] = {}
        for k, v in raw.items():
            lk = str(k).lower()
            canon = REV.get(lk)
            if canon is not None:
                tmp[canon] = v

        # ensure base columns exist
        projected = {c: tmp.get(c) for c in base_cols}

        # build key (prefer interactionid; fallback helps dedup stream-side if missing)
        iid = str(projected.get("interactionid") or "").strip()
        if iid:
            key = iid
        else:
            pid = str(projected.get("patientid") or "").strip()
            dt  = str(projected.get("interactiondate") or "").strip()
            svc = str(projected.get("servicename") or "").strip()
            key = "|".join([pid, dt, svc])

        return (key, json.dumps(projected, default=str))

    cleaned = src.map(
        clean_map,
        output_type=Types.TUPLE([Types.STRING(), Types.STRING()])
    ).name("clean-transform")

    # Stateful dedup by key + payload hash
    deduped = cleaned.key_by(
        lambda kv: kv[0], key_type=Types.STRING()
    ).process(
        DedupByKeyHash(),
        output_type=Types.TUPLE([Types.STRING(), Types.STRING()])
    ).name("dedup-stateful")

    # Convert (key, json) -> Row with base columns (string-typed)
    row_type = Types.ROW_NAMED(base_cols, [Types.STRING()] * len(base_cols))
    def to_row(kv: Tuple[str, str]) -> Row:
        _, js = kv
        if isinstance(js, (bytes, bytearray)):
            js = js.decode("utf-8", errors="replace")
        d = json.loads(js)
        return Row(*[(None if d.get(c) is None else str(d.get(c))) for c in base_cols])

    rows_stream = deduped.map(to_row, output_type=row_type).name("json->row")

    # 2) Table API
    t_env = StreamTableEnvironment.create(env)

    # Base schema = base_cols
    sb = Schema.new_builder()
    for c in base_cols:
        sb.column(c, DataTypes.STRING())
    base_schema = sb.build()

    base_tbl = t_env.from_data_stream(rows_stream, base_schema)

    # Apply your curation function (adds service_category, interaction_year, interaction_month)
    curated_tbl = curate_interactions(base_tbl)
    # Cast derived ints (year/month) to STRING to keep the TEXT sink contract
    curated_tbl = (
        curated_tbl
        .add_or_replace_columns(col("service_category").cast(DataTypes.STRING()))
        .add_or_replace_columns(col("interaction_year").cast(DataTypes.STRING()))
        .add_or_replace_columns(col("interaction_month").cast(DataTypes.STRING()))
    )

    # Reorder/select columns to match REQUIRED_COLS exactly
    sel = [col(c) for c in REQUIRED_COLS]
    final_tbl = curated_tbl.select(*sel)

    # 3) JDBC sink to Postgres (public.carepro_all_interactions) with PK on interactionid only
    pg_url = f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DB}"
    cols_ddl = ", ".join([f"{bt(c)} STRING" for c in REQUIRED_COLS])

    ddl = f"""
    CREATE TABLE IF NOT EXISTS carepro_all_interactions_sink (
      {cols_ddl},
      PRIMARY KEY ({bt("interactionid")}) NOT ENFORCED
    ) WITH (
      'connector' = 'jdbc',
      'url' = '{pg_url}',
      'table-name' = 'public.carepro_all_interactions',
      'driver' = 'org.postgresql.Driver',
      'username' = '{PG_USER}',
      'password' = '{PG_PASSWORD}',
      'sink.buffer-flush.max-rows' = '1000',
      'sink.buffer-flush.interval' = '3s',
      'sink.max-retries' = '3'
    )
    """
    t_env.execute_sql(ddl)

    # Start the sink
    stmt = t_env.create_statement_set()
    stmt.add_insert("carepro_all_interactions_sink", final_tbl)
    stmt.execute()

if __name__ == "__main__":
    main()
