# -*- coding: utf-8 -*-
"""
Rich sink that upserts into a target table with given PK and column list.
"""

import json
import os
from typing import List, Optional, Tuple
import psycopg2
from psycopg2.extras import execute_values
from pyflink.datastream.functions import RuntimeContext, RichSinkFunction


class PostgresUpsertSink(RichSinkFunction):
    def __init__(self, required_cols: List[str], table: str, pk: Tuple[str, str], batch_size: int = 500):
        self.required_cols = required_cols
        self.table = table
        self.pk = pk
        self.batch_size = batch_size

    def open(self, runtime_context: RuntimeContext):
        self.conn = psycopg2.connect(
            host=os.getenv("PG_HOST", "postgres"),
            port=int(os.getenv("PG_PORT", "5432")),
            dbname=os.getenv("PG_DB", "carepro_messages"),
            user=os.getenv("PG_USER", "postgres"),
            password=os.getenv("PG_PASSWORD", "1234"),
            sslmode=os.getenv("PG_SSLMODE", "disable"),
        )
        self.conn.autocommit = True
        self.cur = self.conn.cursor()
        self.buf: List[List[Optional[object]]] = []

        cols_quoted = [f'"{c}"' if " " in c else c for c in self.required_cols]
        cols = ", ".join(cols_quoted)
        pk_cols = ", ".join([p if " " not in p else f'"{p}"' for p in self.pk])
        excluded_updates = ", ".join(
            [f'{(f"""\"{c}\"""" if " " in c else c)}=EXCLUDED.{(f"""\"{c}\"""" if " " in c else c)}'
             for c in self.required_cols if c not in self.pk]
        )
        self.insert_sql = f"""
            INSERT INTO {self.table} ({cols})
            VALUES %s
            ON CONFLICT ({pk_cols}) DO UPDATE SET
            {excluded_updates};
        """

    def _flush(self):
        if not self.buf:
            return
        template = f'({",".join(["%s"]*len(self.required_cols))})'
        execute_values(self.cur, self.insert_sql, self.buf, template=template, page_size=self.batch_size)
        self.buf.clear()

    def invoke(self, kv, context):
        _, payload = kv
        rec = json.loads(payload)
        self.buf.append([rec.get(c) for c in self.required_cols])
        if len(self.buf) >= self.batch_size:
            self._flush()

    def close(self):
        try:
            self._flush()
        finally:
            try:
                self.cur.close()
            except Exception:
                pass
            try:
                self.conn.close()
            except Exception:
                pass
