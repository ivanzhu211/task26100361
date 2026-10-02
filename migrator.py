"""Resumable SQLite-table-to-CSV migrator (stdlib only).

A job exports one SQLite source table into a single CSV file, in primary-key
order, batch by batch.  Job state and progress live in a separate metadata
SQLite file, so a killed process can be resumed by calling RunJob again.
"""

from __future__ import annotations

import argparse
import csv
import io
import json
import os
import sqlite3
import sys
import time
from dataclasses import dataclass
from typing import Any, Optional, Sequence

VALID_TYPES = ("integer", "real", "text")
MAX_BATCH_SIZE = 1000

STATE_QUEUED = "queued"
STATE_COMPLETED = "completed"
STATE_PARTIALLY_COMPLETE = "partially_complete"


@dataclass
class ColumnSpec:
    source: str  # column name in the source table
    target: str  # column name in the CSV header
    type: str    # one of VALID_TYPES


@dataclass
class JobSpec:
    source_db: str
    table: str
    columns: list  # list[ColumnSpec]
    target_csv: str
    batch_size: int


class ConversionError(ValueError):
    """A source value cannot be converted to its declared target type."""


def _convert_value(value: Any, target_type: str) -> Any:
    """Convert one source value to its declared target type.

    Returns the Python value to hand to csv.writer (int / float / str).
    Raises ConversionError when the value does not fit the declared type.
    """
    if target_type == "text":
        return "" if value is None else str(value)
    if value is None:
        raise ConversionError("NULL is not convertible to " + target_type)
    if target_type == "integer":
        if isinstance(value, bool):
            raise ConversionError(f"{value!r} is not an integer")
        if isinstance(value, int):
            return value
        if isinstance(value, float):
            if value.is_integer():
                return int(value)
            raise ConversionError(f"{value!r} is not an integer")
        if isinstance(value, str):
            try:
                return int(value.strip(), 10)
            except ValueError:
                raise ConversionError(f"{value!r} is not an integer") from None
        raise ConversionError(f"{value!r} is not an integer")
    if target_type == "real":
        if isinstance(value, bool):
            raise ConversionError(f"{value!r} is not a real number")
        if isinstance(value, (int, float)):
            return float(value)
        if isinstance(value, str):
            try:
                return float(value.strip())
            except ValueError:
                raise ConversionError(f"{value!r} is not a real number") from None
        raise ConversionError(f"{value!r} is not a real number")
    raise ConversionError(f"unknown target type: {target_type!r}")


def _quote_ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def _key_columns(src: sqlite3.Connection, table: str) -> list:
    """Source key columns in order: declared PK columns, else ['rowid'].

    Mixed integer/text values in one column are fine: SQLite's ORDER BY and
    `>` comparisons impose a total order across storage classes.
    """
    rows = src.execute(
        f"SELECT name, pk FROM pragma_table_info({_quote_ident(table)})"
    ).fetchall()
    if not rows:
        raise KeyError(f"no such source table: {table!r}")
    pk_cols = [name for name, pk in sorted(rows, key=lambda r: r[1]) if pk > 0]
    return pk_cols if pk_cols else ["rowid"]


def _keyset_where(key_cols: list) -> tuple:
    """Return (where_sql, order_sql) for keyset pagination, no OFFSET."""
    order = ", ".join(_quote_ident(k) for k in key_cols)
    lhs = "(" + ", ".join(_quote_ident(k) for k in key_cols) + ")"
    rhs = "(" + ", ".join("?" for _ in key_cols) + ")"
    return f"{lhs} > {rhs}", f"ORDER BY {order}"


def _parse_spec(spec: Any) -> JobSpec:
    if isinstance(spec, JobSpec):
        return spec
    if not isinstance(spec, dict):
        raise TypeError("spec must be a dict or JobSpec")
    cols = [
        ColumnSpec(source=c["source"], target=c.get("target", c["source"]), type=c["type"])
        for c in spec["columns"]
    ]
    if not cols:
        raise ValueError("spec must declare at least one column")
    for c in cols:
        if c.type not in VALID_TYPES:
            raise ValueError(f"invalid column type: {c.type!r}")
    batch_size = int(spec.get("batch_size", MAX_BATCH_SIZE))
    if not 1 <= batch_size <= MAX_BATCH_SIZE:
        raise ValueError("batch_size must be between 1 and 1000")
    return JobSpec(
        source_db=spec["source_db"],
        table=spec["table"],
        columns=cols,
        target_csv=spec["target_csv"],
        batch_size=batch_size,
    )


def _spec_to_json(spec: JobSpec) -> str:
    return json.dumps({
        "source_db": spec.source_db,
        "table": spec.table,
        "columns": [vars(c) for c in spec.columns],
        "target_csv": spec.target_csv,
        "batch_size": spec.batch_size,
    })


def _connect_meta(meta_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(meta_path)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute(
        """CREATE TABLE IF NOT EXISTS jobs (
               id INTEGER PRIMARY KEY AUTOINCREMENT,
               spec TEXT NOT NULL,
               state TEXT NOT NULL,
               rows_written INTEGER NOT NULL DEFAULT 0,
               committed_offset INTEGER NOT NULL DEFAULT 0,
               committed_key TEXT,
               fail_info TEXT
           )"""
    )
    return conn


def _load_job(conn: sqlite3.Connection, job_id: int) -> sqlite3.Row:
    conn.row_factory = sqlite3.Row
    row = conn.execute("SELECT * FROM jobs WHERE id = ?", (job_id,)).fetchone()
    if row is None:
        raise KeyError(f"no such job: {job_id}")
    return row


def CreateJob(spec: Any, meta_path: str) -> int:
    """Register a job in the metadata DB; returns the new job id (queued)."""
    parsed = _parse_spec(spec)
    conn = _connect_meta(meta_path)
    try:
        cur = conn.execute(
            "INSERT INTO jobs (spec, state) VALUES (?, ?)",
            (_spec_to_json(parsed), STATE_QUEUED),
        )
        conn.commit()
        return cur.lastrowid
    finally:
        conn.close()


def RunJob(job_id: int, meta_path: str) -> dict:
    """Resume the job from its last committed progress until completed or paused."""
    meta = _connect_meta(meta_path)
    try:
        row = _load_job(meta, job_id)
        if row["state"] == STATE_COMPLETED:
            return GetJob(job_id, meta_path)

        spec = _parse_spec(json.loads(row["spec"]))
        src = sqlite3.connect(spec.source_db)
        try:
            key_cols = _key_columns(src, spec.table)
            select_cols = key_cols + [c.source for c in spec.columns
                                      if c.source not in key_cols]
            col_sql = ", ".join(_quote_ident(c) for c in select_cols)
            where_sql, order_sql = _keyset_where(key_cols)
            base_sql = f"SELECT {col_sql} FROM {_quote_ident(spec.table)}"

            offset = row["committed_offset"]
            committed_key = (json.loads(row["committed_key"])
                             if row["committed_key"] else None)
            rows_written = row["rows_written"]
            n_keys = len(key_cols)

            # Recover from a kill that happened mid-batch: the file may hold
            # a partial/uncommitted tail; drop everything past the last
            # committed offset.  O(1), never re-reads written content.
            out = open(spec.target_csv, "a+b")
            try:
                out.truncate(offset)
                if offset == 0:
                    header = _serialize_rows([[c.target for c in spec.columns]])
                    out.write(header)
                    out.flush()
                    os.fsync(out.fileno())
                    offset = len(header)
                    meta.execute(
                        "UPDATE jobs SET committed_offset = ? WHERE id = ?",
                        (offset, job_id))
                    meta.commit()

                delay_ms = float(os.environ.get("MIGRATOR_BATCH_DELAY_MS", "0"))

                while True:
                    if committed_key is None:
                        batch = src.execute(
                            f"{base_sql} {order_sql} LIMIT ?",
                            (spec.batch_size,)).fetchall()
                    else:
                        batch = src.execute(
                            f"{base_sql} WHERE {where_sql} {order_sql} LIMIT ?",
                            (*committed_key, spec.batch_size)).fetchall()
                    if not batch:
                        meta.execute(
                            "UPDATE jobs SET state = ? WHERE id = ?",
                            (STATE_COMPLETED, job_id))
                        meta.commit()
                        break

                    converted = []
                    error = None
                    for raw in batch:
                        key = list(raw[:n_keys])
                        try:
                            converted.append([
                                _convert_value(v, c.type)
                                for v, c in zip(_reorder(raw, select_cols, spec),
                                                spec.columns)
                            ])
                        except ConversionError as exc:
                            error = (key, str(exc))
                            break

                    if error is not None:
                        fail_info = {
                            "batch_first_key": list(batch[0][:n_keys]),
                            "batch_last_key": list(batch[-1][:n_keys]),
                            "error_key": error[0],
                            "error": error[1],
                        }
                        meta.execute(
                            "UPDATE jobs SET state = ?, fail_info = ? WHERE id = ?",
                            (STATE_PARTIALLY_COMPLETE, json.dumps(fail_info), job_id))
                        meta.commit()
                        break

                    data = _serialize_rows(converted)
                    out.write(data)
                    out.flush()
                    os.fsync(out.fileno())
                    if delay_ms:
                        time.sleep(delay_ms / 1000.0)
                    offset += len(data)
                    rows_written += len(converted)
                    committed_key = list(batch[-1][:n_keys])
                    meta.execute(
                        """UPDATE jobs
                           SET committed_offset = ?, committed_key = ?,
                               rows_written = ?, state = ?
                           WHERE id = ?""",
                        (offset, json.dumps(committed_key), rows_written,
                         STATE_QUEUED, job_id))
                    meta.commit()
            finally:
                out.close()
        finally:
            src.close()
        return GetJob(job_id, meta_path)
    finally:
        meta.close()


def _reorder(raw, select_cols, spec):
    """Yield source values of a raw row in spec.columns order."""
    idx = {name: i for i, name in enumerate(select_cols)}
    return [raw[idx[c.source]] for c in spec.columns]


def _serialize_rows(rows: list) -> bytes:
    buf = io.StringIO()
    csv.writer(buf, lineterminator="\n").writerows(rows)
    return buf.getvalue().encode("utf-8")


def GetJob(job_id: int, meta_path: str) -> dict:
    """Return job state, rows written, and failure info when paused."""
    conn = _connect_meta(meta_path)
    try:
        row = _load_job(conn, job_id)
        info = {
            "job_id": row["id"],
            "state": row["state"],
            "rows_written": row["rows_written"],
        }
        if row["state"] == STATE_PARTIALLY_COMPLETE and row["fail_info"]:
            info["failure"] = json.loads(row["fail_info"])
        return info
    finally:
        conn.close()


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(prog="migrator", description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)

    p_create = sub.add_parser("create", help="register a job from a spec JSON file")
    p_create.add_argument("meta")
    p_create.add_argument("spec_json", help="path to a JSON job spec")

    p_run = sub.add_parser("run", help="run/resume a job to completion or pause")
    p_run.add_argument("meta")
    p_run.add_argument("job_id", type=int)

    p_get = sub.add_parser("get", help="print job status as JSON")
    p_get.add_argument("meta")
    p_get.add_argument("job_id", type=int)

    args = parser.parse_args(argv)
    if args.command == "create":
        with open(args.spec_json, "r", encoding="utf-8") as fh:
            spec = json.load(fh)
        print(CreateJob(spec, args.meta))
        return 0
    if args.command == "run":
        info = RunJob(args.job_id, args.meta)
        print(json.dumps(info))
        return 0 if info["state"] == STATE_COMPLETED else 1
    if args.command == "get":
        print(json.dumps(GetJob(args.job_id, args.meta)))
        return 0
    return 2


if __name__ == "__main__":
    sys.exit(main())
