# -*- coding: utf-8 -*-
"""
Kafka -> Clean -> Dedup -> Postgres (art_cohort only for this job)
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

from transforms_art_cohort import transform, REQUIRED_COLS
from sinks.postgres_sink import PostgresUpsertSink


KAFKA_TOPIC = os.getenv("KAFKA_TOPIC_ART", "carepro.art_cohort")
BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
PARALLELISM = int(os.getenv("FLINK_PARALLELISM", "2"))
CP_MS = int(os.getenv("FLINK_CHECKPOINT_INTERVAL_MS", "60000"))

def md5(s: str) -> str:
    return hashlib.md5(s.encode("utf-8")).hexdigest()

class DedupByKeyHash(KeyedProcessFunction):
    def open(self, runtime_context: RuntimeContext):
        self.seen = runtime_context.get_state(ValueStateDescriptor("last_hash", Types.STRING()))
    def process_element(self, value: Tuple[str, str], ctx):
        key, payload = value
        h = md5(payload)
        prev = self.seen.value()
        if prev != h:
            self.seen.update(h)
            yield value

def main():
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

    cleaned = src.map(clean_map, output_type=Types.TUPLE([Types.STRING(), Types.STRING()])).name("clean-transform")

    deduped = cleaned.key_by(lambda kv: kv[0], key_type=Types.STRING()).process(
        DedupByKeyHash(), output_type=Types.TUPLE([Types.STRING(), Types.STRING()])
    ).name("dedup-stateful")

    sink = PostgresUpsertSink(required_cols=REQUIRED_COLS, table="carepro.art_cohort", pk=("patientid","artnumber"))
    deduped.add_sink(sink).name("postgres-upsert")

    env.execute("CarePro art_cohort cleaner → Postgres")

if __name__ == "__main__":
    main()
