import os, hashlib
from pyflink.datastream import StreamExecutionEnvironment, CheckpointingMode
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream.connectors import FlinkKafkaConsumer
from pyflink.common.typeinfo import Types
from pyflink.datastream.state import ValueStateDescriptor
from pyflink.datastream.functions import KeyedProcessFunction, RuntimeContext
from sinks.postgres_sink import PostgresUpsertSink

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPICS = [
    os.getenv("KAFKA_TOPIC_ART", "carepro.art_cohort"),
    os.getenv("KAFKA_TOPIC_INTERACTIONS", "carepro.all_interactions")
]
PARALLELISM = int(os.getenv("FLINK_PARALLELISM", "2"))
CP_MS = int(os.getenv("FLINK_CHECKPOINT_INTERVAL_MS", "60000"))

def md5(b: str) -> str: return hashlib.md5(b.encode("utf-8")).hexdigest()

class DedupByHash(KeyedProcessFunction):
    def open(self, runtime_context: RuntimeContext):
        self.last = runtime_context.get_state(ValueStateDescriptor("last_hash", Types.STRING()))
    def process_element(self, value, ctx):
        topic, key, payload = value
        h = md5(payload)
        prev = self.last.value()
        if prev != h:
            self.last.update(h)
            yield value

def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(PARALLELISM)
    env.enable_checkpointing(CP_MS, CheckpointingMode.EXACTLY_ONCE)

    props = {
        "bootstrap.servers": BOOTSTRAP,
        "group.id": "carepro-flink-consumer",
        "auto.offset.reset": "earliest",
        "isolation.level": "read_committed"
    }

    # Consume each topic separately so we can carry the topic name
    streams = []
    for t in TOPICS:
        c = FlinkKafkaConsumer([t], SimpleStringSchema(), props)
        s = env.add_source(c).map(lambda s, t=t: (t, "", s),
             output_type=Types.TUPLE([Types.STRING(), Types.STRING(), Types.STRING()])).name(f"src-{t}")
        streams.append(s)

    merged = streams[0]
    for s in streams[1:]: merged = merged.union(s)

    deduped = merged.key_by(lambda v: v[1] or "nokey", key_type=Types.STRING()) \
                    .process(DedupByHash(), output_type=Types.TUPLE([Types.STRING(), Types.STRING(), Types.STRING()]))

    deduped.add_sink(PostgresUpsertSink()).name("postgres-upsert")
    env.execute("CarePro SQLServer→Kafka→Flink→Postgres")

if __name__ == "__main__": main()
