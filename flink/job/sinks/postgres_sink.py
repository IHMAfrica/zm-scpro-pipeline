# flink/job/sinks/postgres_sink.py
from __future__ import annotations

import json
import os
from typing import Any, List, Sequence, Tuple

import psycopg2
from pyflink.datastream.functions import SinkFunction


def _qident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def _split_table(full: str) -> Tuple[str | None, str]:
    parts = full.split(".")
    if len(parts) == 2:
        return parts[0], parts[1]
    return None, parts[0]


class _LazyPgWriter:
    """Creates table if missing; batches INSERT ... ON CONFLICT ... DO UPDATE."""

    def __init__(self, table: str, required_cols: Sequence[str], pk: Sequence[str], batch_size: int):
        self.table = table
        self.required_cols = list(required_cols)
        self.pk = tuple(pk)
        self.batch_size = int(batch_size)
        self._conn = None
        self._cur = None
        self._buf: List[Tuple[Any, ...]] = []
        self._upsert_sql: str | None = None
        self._initialized = False

    # ----- public API used by the sink callable -----
    def handle(self, value: Any) -> None:
        if not self._initialized:
            self._init_once()
        record = self._to_dict(value)
        row = tuple(record.get(c) for c in self.required_cols)
        self._buf.append(row)
        if len(self._buf) >= self.batch_size:
            self._flush()

    # ----- internals -----
    def _init_once(self) -> None:
        self._connect()
        self._ensure_table()
        self._prepare_upsert()
        self._initialized = True

    def _connect(self) -> None:
        if self._conn:
            return
        host = os.getenv("PG_HOST", "localhost")
        port = int(os.getenv("PG_PORT", "5432"))
        db = os.getenv("PG_DB")
        user = os.getenv("PG_USER")
        pwd = os.getenv("PG_PASSWORD")
        ssl = os.getenv("PG_SSLMODE", "disable")
        if not all([db, user, pwd]):
            raise RuntimeError("PG_DB, PG_USER, PG_PASSWORD must be set.")
        self._conn = psycopg2.connect(
            host=host, port=port, dbname=db, user=user, password=pwd, sslmode=ssl
        )
        self._conn.autocommit = False
        self._cur = self._conn.cursor()

    def _ensure_table(self) -> None:
        schema, table = _split_table(self.table)
        if schema:
            self._cur.execute(f"CREATE SCHEMA IF NOT EXISTS {_qident(schema)}")
        cols_sql = ", ".join(f"{_qident(c)} TEXT" for c in self.required_cols)
        pk_sql = f", PRIMARY KEY ({', '.join(_qident(c) for c in self.pk)})" if self.pk else ""
        self._cur.execute(f"CREATE TABLE IF NOT EXISTS {self._full_table()} ({cols_sql}{pk_sql})")
        self._conn.commit()

    def _prepare_upsert(self) -> None:
        cols_q = ", ".join(_qident(c) for c in self.required_cols)
        placeholders = ", ".join(["%s"] * len(self.required_cols))
        pk_cols = ", ".join(_qident(c) for c in self.pk)
        upd_cols = [c for c in self.required_cols if c not in self.pk]
        if upd_cols:
            set_clause = ", ".join(f"{_qident(c)}=EXCLUDED.{_qident(c)}" for c in upd_cols)
        else:
            set_clause = ", ".join(f"{_qident(c)}={_qident(c)}" for c in self.pk)
        self._upsert_sql = (
            f"INSERT INTO {self._full_table()} ({cols_q}) VALUES ({placeholders}) "
            f"ON CONFLICT ({pk_cols}) DO UPDATE SET {set_clause}"
        )

    def _flush(self) -> None:
        if not self._buf or not self._upsert_sql:
            return
        try:
            self._cur.executemany(self._upsert_sql, self._buf)
            self._conn.commit()
            self._buf.clear()
        except Exception:
            self._conn.rollback()
            # reconnect once then retry
            try:
                self._cur.close()
            except Exception:
                pass
            try:
                self._conn.close()
            except Exception:
                pass
            self._conn = None
            self._cur = None
            self._connect()
            self._cur.executemany(self._upsert_sql, self._buf)
            self._conn.commit()
            self._buf.clear()

    def _full_table(self) -> str:
        schema, table = _split_table(self.table)
        return f"{_qident(schema)}.{_qident(table)}" if schema else _qident(table)

    @staticmethod
    def _to_dict(value: Any) -> dict:
        # stream may emit (key, json_str) or just json_str/dict/bytes
        if isinstance(value, (tuple, list)) and len(value) >= 2:
            value = value[1]
        if isinstance(value, (bytes, bytearray)):
            value = value.decode("utf-8", errors="replace")
        if isinstance(value, str):
            return json.loads(value)
        if isinstance(value, dict):
            return value
        # last resort
        return json.loads(str(value))


def make_upsert_sink(
    *,
    table: str,
    required_cols: Sequence[str],
    pk: Sequence[str],
    batch_size: int | None = None,
) -> SinkFunction:
    """
    Factory that returns a PyFlink SinkFunction wrapping a lazy Postgres writer.
    """
    writer = _LazyPgWriter(
        table=table,
        required_cols=required_cols,
        pk=pk,
        batch_size=int(batch_size or os.getenv("PG_BATCH_SIZE", "1000")),
    )

    # PyFlink 1.17 expects a plain callable; the wrapper will handle serialization.
    def _sink_callable(value, ctx=None):
        writer.handle(value)

    return SinkFunction(_sink_callable)
