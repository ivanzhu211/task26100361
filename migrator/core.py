"""Core job registry and resumable migration engine.

Job state and progress live in a metadata SQLite file. The target CSV grows
in place; after each batch the file is flushed+fsynced and the committed file
offset is recorded in the metadata. On resume, the file is truncated back to
the committed offset, so a batch interrupted by SIGKILL is simply redone.
"""

import csv
import io
import json
import os
import sqlite3
import time
from pathlib import Path

VALID_TYPES = ("integer", "real", "text")
MAX_BATCH_SIZE = 1000

STATUS_QUEUED = "queued"
STATUS_COMPLETED = "completed"
STATUS_PAUSED = "partially_complete"

_SCHEMA = """
CREATE TABLE IF NOT EXISTS jobs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    spec TEXT NOT NULL,
    status TEXT NOT NULL,
    rows_written INTEGER NOT NULL DEFAULT 0,
    committed_offset INTEGER NOT NULL DEFAULT 0,
    header_written INTEGER NOT NULL DEFAULT 0,
    last_key TEXT,
    fail_batch_first_key TEXT,
    fail_batch_last_key TEXT,
    fail_row_key TEXT,
    fail_reason TEXT
);
"""


# ---------------------------------------------------------------- spec


def _validate_spec(spec):
    required = ("source_path", "table", "columns", "output_path", "batch_size")
    for key in required:
        if key not in spec:
            raise ValueError(f"spec missing required key: {key}")
    if not isinstance(spec["source_path"], str) or not spec["source_path"]:
        raise ValueError("source_path must be a non-empty string")
    if not isinstance(spec["table"], str) or not spec["table"]:
        raise ValueError("table must be a non-empty string")
    columns = spec["columns"]
    if not isinstance(columns, list) or not columns:
        raise ValueError("columns must be a non-empty list")
    for col in columns:
        if not isinstance(col, dict):
            raise ValueError("each column must be a dict")
        for key in ("source", "target", "type"):
            if key not in col:
                raise ValueError(f"column missing key: {key}")
        if col["type"] not in VALID_TYPES:
            raise ValueError(f"invalid column type: {col['type']}")
    batch_size = spec["batch_size"]
    if not isinstance(batch_size, int) or isinstance(batch_size, bool):
        raise ValueError("batch_size must be an integer")
    if not 1 <= batch_size <= MAX_BATCH_SIZE:
        raise ValueError(f"batch_size must be between 1 and {MAX_BATCH_SIZE}")
    if not isinstance(spec["output_path"], str) or not spec["output_path"]:
        raise ValueError("output_path must be a non-empty string")


# ---------------------------------------------------------------- metadata


def _connect_meta(meta_path):
    conn = sqlite3.connect(str(meta_path))
    conn.execute("PRAGMA journal_mode=WAL")
    conn.executescript(_SCHEMA)
    return conn


def create_job(meta_path, spec):
    """Register a job; return job id. Initial status: queued."""
    _validate_spec(spec)
    conn = _connect_meta(meta_path)
    try:
        cur = conn.execute(
            "INSERT INTO jobs (spec, status) VALUES (?, ?)",
            (json.dumps(spec), STATUS_QUEUED),
        )
        conn.commit()
        return cur.lastrowid
    finally:
        conn.close()


def _load_job(conn, job_id):
    row = conn.execute(
        "SELECT id, spec, status, rows_written, committed_offset,"
        "       header_written, last_key, fail_batch_first_key,"
        "       fail_batch_last_key, fail_row_key, fail_reason"
        " FROM jobs WHERE id = ?",
        (job_id,),
    ).fetchone()
    if row is None:
        raise KeyError(f"no such job: {job_id}")
    return row


def get_job(meta_path, job_id):
    """Return status, rows written, and failure info when paused."""
    conn = _connect_meta(meta_path)
    try:
        row = _load_job(conn, job_id)
    finally:
        conn.close()
    result = {
        "id": row[0],
        "status": row[2],
        "rows_written": row[3],
    }
    if row[2] == STATUS_PAUSED:
        result["failure"] = {
            "batch_first_key": json.loads(row[7]),
            "batch_last_key": json.loads(row[8]),
            "error_row_key": json.loads(row[9]),
            "reason": row[10],
        }
    return result


# ---------------------------------------------------------------- source


def _quote_ident(name):
    return '"' + name.replace('"', '""') + '"'


def _key_columns(src, table):
    """Return (key_expressions, key_labels) for ordering/pagination."""
    info = src.execute(f"PRAGMA table_info({_quote_ident(table)})").fetchall()
    if not info:
        raise ValueError(f"source table not found: {table}")
    pks = sorted((r for r in info if r[5] > 0), key=lambda r: r[5])
    if pks:
        exprs = [_quote_ident(r[1]) for r in pks]
        labels = [r[1] for r in pks]
    else:
        exprs = ["rowid"]
        labels = ["rowid"]
    return exprs, labels


def _fetch_batch(src, table, key_exprs, select_cols, last_key, batch_size):
    cols = key_exprs + select_cols
    sql = f"SELECT {', '.join(cols)} FROM {_quote_ident(table)}"
    params = []
    if last_key is not None:
        if len(key_exprs) == 1:
            sql += f" WHERE {key_exprs[0]} > ?"
        else:
            lhs = "(" + ", ".join(key_exprs) + ")"
            rhs = "(" + ", ".join("?" for _ in key_exprs) + ")"
            sql += f" WHERE {lhs} > {rhs}"
        params.extend(last_key)
    sql += " ORDER BY " + ", ".join(key_exprs)
    sql += " LIMIT ?"
    params.append(batch_size)
    return src.execute(sql, params).fetchall()


# ---------------------------------------------------------------- convert


def _convert_value(value, col_type):
    if value is None:
        if col_type == "text":
            return ""
        raise ValueError(f"NULL is not convertible to {col_type}")
    if col_type == "text":
        if isinstance(value, bytes):
            return value.decode("utf-8", "replace")
        return str(value)
    if col_type == "integer":
        if isinstance(value, bool):
            return str(int(value))
        if isinstance(value, int):
            return str(value)
        if isinstance(value, float):
            if value.is_integer():
                return str(int(value))
            raise ValueError(f"non-integral float {value!r} for integer column")
        if isinstance(value, str):
            return str(int(value.strip()))
        raise ValueError(f"cannot convert {value!r} to integer")
    if col_type == "real":
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            return repr(float(value))
        if isinstance(value, str):
            return repr(float(value.strip()))
        raise ValueError(f"cannot convert {value!r} to real")
    raise ValueError(f"unknown column type: {col_type}")


def _convert_row(row, n_keys, columns):
    values = row[n_keys:]
    out = []
    for value, col in zip(values, columns):
        out.append(_convert_value(value, col["type"]))
    return out


# ---------------------------------------------------------------- run


def _row_sleep():
    # Test hook: slows the per-row write loop so SIGKILL can land mid-batch.
    try:
        return float(os.environ.get("CSV_MIGRATOR_ROW_SLEEP", "0") or 0)
    except ValueError:
        return 0.0


def run_job(meta_path, job_id):
    """Resume execution until completed or paused on a bad batch."""
    meta = _connect_meta(meta_path)
    try:
        row = _load_job(meta, job_id)
        status = row[2]
        if status == STATUS_COMPLETED:
            return get_job(meta_path, job_id)
        spec = json.loads(row[1])
        rows_written = row[3]
        committed_offset = row[4]
        header_written = row[5]
        last_key = json.loads(row[6]) if row[6] is not None else None

        source_uri = Path(spec["source_path"]).resolve().as_uri() + "?mode=ro"
        src = sqlite3.connect(source_uri, uri=True)
        try:
            key_exprs, _labels = _key_columns(src, spec["table"])
            n_keys = len(key_exprs)
            select_cols = [_quote_ident(c["source"]) for c in spec["columns"]]
            batch_size = spec["batch_size"]

            existed = os.path.exists(spec["output_path"])
            raw = open(spec["output_path"], "r+b" if existed else "w+b")
            try:
                # Roll back any bytes from a batch interrupted mid-write.
                raw.truncate(committed_offset)
                raw.seek(committed_offset)
                text = io.TextIOWrapper(
                    raw, encoding="utf-8", newline="", write_through=True
                )
                writer = csv.writer(text)
                sleep_s = _row_sleep()

                if not header_written:
                    writer.writerow([c["target"] for c in spec["columns"]])
                    text.flush()
                    raw.flush()
                    os.fsync(raw.fileno())
                    committed_offset = raw.tell()
                    header_written = 1
                    meta.execute(
                        "UPDATE jobs SET header_written = 1,"
                        " committed_offset = ? WHERE id = ?",
                        (committed_offset, job_id),
                    )
                    meta.commit()

                while True:
                    batch = _fetch_batch(
                        src, spec["table"], key_exprs, select_cols,
                        last_key, batch_size,
                    )
                    if not batch:
                        meta.execute(
                            "UPDATE jobs SET status = ? WHERE id = ?",
                            (STATUS_COMPLETED, job_id),
                        )
                        meta.commit()
                        break
                    converted = []
                    err_idx = -1
                    err_exc = None
                    for i, r in enumerate(batch):
                        try:
                            converted.append(_convert_row(r, n_keys, spec["columns"]))
                        except (ValueError, TypeError) as exc:
                            err_idx, err_exc = i, exc
                            break
                    if err_idx >= 0:
                        meta.execute(
                            "UPDATE jobs SET status = ?,"
                            " fail_batch_first_key = ?, fail_batch_last_key = ?,"
                            " fail_row_key = ?, fail_reason = ? WHERE id = ?",
                            (
                                STATUS_PAUSED,
                                json.dumps(list(batch[0][:n_keys])),
                                json.dumps(list(batch[-1][:n_keys])),
                                json.dumps(list(batch[err_idx][:n_keys])),
                                f"{type(err_exc).__name__}: {err_exc}",
                                job_id,
                            ),
                        )
                        meta.commit()
                        break
                    for out_row in converted:
                        writer.writerow(out_row)
                        if sleep_s:
                            text.flush()
                            time.sleep(sleep_s)
                    text.flush()
                    raw.flush()
                    os.fsync(raw.fileno())
                    committed_offset = raw.tell()
                    rows_written += len(batch)
                    last_key = list(batch[-1][:n_keys])
                    meta.execute(
                        "UPDATE jobs SET rows_written = ?, committed_offset = ?,"
                        " last_key = ? WHERE id = ?",
                        (rows_written, committed_offset,
                         json.dumps(last_key), job_id),
                    )
                    meta.commit()
            finally:
                raw.close()
        finally:
            src.close()
        return get_job(meta_path, job_id)
    finally:
        meta.close()
