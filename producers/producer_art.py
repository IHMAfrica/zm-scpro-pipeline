#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os, json, sys, time
from datetime import datetime
from decimal import Decimal
from typing import Any, Iterable, Dict, List, Optional

import pymssql
from confluent_kafka import Producer


# ---------- helpers ----------
def env(name: str, default: Optional[str] = None) -> Optional[str]:
    v = os.getenv(name)
    return v if v not in (None, "") else default

def get_bootstrap() -> str:
    # prefer KAFKA_BOOTSTRAP, fallback to KAFKA_BOOTSTRAP_SERVERS
    return env("KAFKA_BOOTSTRAP", env("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"))

def kafka_producer() -> Producer:
    cfg = {
        "bootstrap.servers": get_bootstrap(),
        "client.id": env("KAFKA_CLIENT_ID", "carepro-producer"),
        "compression.type": env("KAFKA_COMPRESSION_TYPE", "snappy"),
        "enable.idempotence": True,
        "acks": "all",
        "linger.ms": 50,
        "batch.num.messages": 10000,
    }
    sec = env("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT").upper()
    if sec.startswith("SASL"):
        cfg.update({
            "security.protocol": sec,
            "sasl.mechanisms": env("KAFKA_SASL_MECHANISM", "SCRAM-SHA-512"),
            "sasl.username": env("KAFKA_SASL_USER", env("KAFKA_SASL_USERNAME")),
            "sasl.password": env("KAFKA_SASL_PASSWORD"),
        })
    return Producer(cfg)

def wait_for_kafka(p: Producer, timeout_s: int = 60):
    deadline = time.time() + timeout_s
    last_err = None
    while True:
        try:
            p.list_topics(timeout=3.0)
            return
        except Exception as e:
            last_err = str(e)
            if time.time() >= deadline:
                raise RuntimeError(f"Kafka not reachable: {last_err}")
            print(f"Waiting for Kafka ... {last_err}", flush=True)
            time.sleep(2)

def mssql_conn():
    host = env("SQLSERVER_HOST", "host.docker.internal")
    port = int(env("SQLSERVER_PORT", "1433"))
    db   = env("SQLSERVER_DB", "carepro")
    user = env("SQLSERVER_USER")
    pwd  = env("SQLSERVER_PASSWORD")

    # Default to TDS 7.3 (pymssql wheels typically lack 7.4 support)
    tds = env("SQLSERVER_TDS_VERSION", "7.3")
    if tds not in (None, "", "7.0", "7.1", "7.2", "7.3"):
        tds = "7.3"

    kwargs = dict(
        server=host, port=port, user=user, password=pwd, database=db,
        as_dict=True, login_timeout=10, timeout=0
    )
    if tds:
        kwargs["tds_version"] = tds

    return pymssql.connect(**kwargs)

def json_default(o: Any):
    if isinstance(o, datetime): return o.isoformat()
    if isinstance(o, Decimal):  return str(o)
    if hasattr(o, "isoformat"):
        try: return o.isoformat()
        except Exception: pass
    return o if isinstance(o, (int,float,str,bool)) or o is None else str(o)

def read_sql(path_env: str, default_path: str) -> str:
    p = env(path_env, default_path)
    with open(p, "r", encoding="utf-8") as f:
        return f.read()

def rows_stream(cursor, arraysize: int) -> Iterable[List[Dict[str, Any]]]:
    while True:
        rows = cursor.fetchmany(arraysize)
        if not rows: break
        yield rows

def make_key(row: Dict[str, Any]) -> Optional[bytes]:
    pid = row.get("patientid") or row.get("PatientId")
    art = row.get("artnumber") or row.get("ARTNumber")
    if pid is not None and art is not None:
        return f"{pid}|{art}".encode("utf-8", "ignore")
    return None


# ---------- main ----------
def main():
    topic = env("KAFKA_TOPIC_ART", "carepro.art_cohort")
    batch_size = int(env("BATCH_SIZE", "5000"))
    sql = read_sql("SQL_ART_QUERY_FILE", "/app/sql/art_cohort.sql")

    p = kafka_producer()
    wait_for_kafka(p)

    sent = 0
    try:
        with mssql_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(sql)
                for chunk in rows_stream(cur, batch_size):
                    for row in chunk:
                        key = make_key(row)
                        val = json.dumps(row, default=json_default, ensure_ascii=False).encode("utf-8")
                        p.produce(topic, value=val, key=key)
                    p.poll(0)
                    p.flush()
                    sent += len(chunk)
                    print(f"[art_cohort] sent: {sent}", flush=True)
    except pymssql.ProgrammingError as e:
        # Friendly hint if table or schema is wrong
        msg = str(e)
        if "Invalid object name" in msg:
            print(f"ERROR: {msg}. Check that the source table exists and SQL path is correct.", file=sys.stderr)
        raise
    finally:
        p.flush()
        print(f"[art_cohort] DONE. total={sent}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
