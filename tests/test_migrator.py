"""End-to-end tests for the resumable migrator."""

import csv
import json
import os
import random
import shutil
import signal
import sqlite3
import subprocess
import sys
import tempfile
import time
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

from migrator import create_job, get_job, run_job  # noqa: E402


def convert_value(value, col_type):
    """Independent re-implementation of the expected conversion rules."""
    if value is None:
        if col_type == "text":
            return ""
        raise ValueError("NULL")
    if col_type == "text":
        return str(value)
    if col_type == "integer":
        if isinstance(value, int):
            return str(value)
        if isinstance(value, float) and value.is_integer():
            return str(int(value))
        if isinstance(value, str):
            return str(int(value.strip()))
        raise ValueError(value)
    if col_type == "real":
        if isinstance(value, (int, float)):
            return repr(float(value))
        if isinstance(value, str):
            return repr(float(value.strip()))
        raise ValueError(value)
    raise AssertionError(col_type)


def expected_csv_rows(source_path, table, columns, order_cols):
    """Ground truth: header + every source row in PK order, converted."""
    conn = sqlite3.connect(str(source_path))
    try:
        select = ", ".join(f'"{c["source"]}"' for c in columns)
        order = ", ".join(f'"{c}"' for c in order_cols)
        rows = conn.execute(
            f"SELECT {select} FROM \"{table}\" ORDER BY {order}"
        ).fetchall()
    finally:
        conn.close()
    out = [[c["target"] for c in columns]]
    for row in rows:
        out.append([
            convert_value(v, c["type"]) for v, c in zip(row, columns)
        ])
    return out


def read_csv(path):
    with open(path, newline="", encoding="utf-8") as f:
        return list(csv.reader(f))


class MigratorTestBase(unittest.TestCase):
    def setUp(self):
        self.workdir = Path(tempfile.mkdtemp(prefix="migrator-test-"))
        self.addCleanup(shutil.rmtree, self.workdir, True)
        self.src_path = self.workdir / "src.db"
        self.meta_path = self.workdir / "meta.db"
        self.out_path = self.workdir / "out.csv"


class TestKillResume(MigratorTestBase):
    """100k rows, repeated SIGKILL mid-run, result must be exact."""

    ROWS = 100_000
    BATCH = 1000

    def _build_source(self):
        conn = sqlite3.connect(str(self.src_path))
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute(
            "CREATE TABLE t (id INTEGER PRIMARY KEY, payload TEXT, amount REAL)"
        )
        rng = random.Random(20261003)
        chunks = []
        for i in range(1, self.ROWS + 1):
            payload = (
                f'row-{i},"quoted""text",line1\nline2\r\nline3\t'
                + ("x" * rng.randint(0, 40))
            )
            chunks.append((i, payload, i * 0.5))
            if len(chunks) == 5000:
                conn.executemany("INSERT INTO t VALUES (?, ?, ?)", chunks)
                chunks.clear()
        if chunks:
            conn.executemany("INSERT INTO t VALUES (?, ?, ?)", chunks)
        conn.commit()
        conn.close()

    def test_repeated_sigkill(self):
        self._build_source()
        columns = [
            {"source": "id", "target": "ID", "type": "integer"},
            {"source": "payload", "target": "Payload", "type": "text"},
            {"source": "amount", "target": "Amount", "type": "real"},
        ]
        spec = {
            "source_path": str(self.src_path),
            "table": "t",
            "columns": columns,
            "output_path": str(self.out_path),
            "batch_size": self.BATCH,
        }
        job_id = create_job(str(self.meta_path), spec)

        env = dict(os.environ)
        env["PYTHONPATH"] = str(REPO_ROOT)
        # Slow the per-row write loop so kills frequently land mid-batch.
        env["CSV_MIGRATOR_ROW_SLEEP"] = "0.0001"

        rng = random.Random(7)
        kills = 0
        mid_batch_kills = 0
        deadline = time.time() + 600
        while True:
            self.assertLess(time.time(), deadline, "job never completed")
            proc = subprocess.Popen(
                [sys.executable, "-m", "migrator", "run",
                 str(self.meta_path), str(job_id)],
                env=env, cwd=str(self.workdir),
                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
            )
            if rng.random() < 0.85:
                time.sleep(rng.uniform(0.02, 0.4))
                if proc.poll() is None:
                    proc.send_signal(signal.SIGKILL)
                    kills += 1
            proc.wait()
            # A kill landed mid-batch if the file holds uncommitted bytes.
            committed = self._committed_offset(job_id)
            size = (self.out_path.stat().st_size
                    if self.out_path.exists() else 0)
            if size != committed:
                mid_batch_kills += 1
            status = get_job(str(self.meta_path), job_id)["status"]
            if status == "completed":
                break

        self.assertGreater(kills, 5, "test should exercise many kills")
        self.assertGreater(
            mid_batch_kills, 0,
            "some kills must land while a batch is half written",
 )

        expected = expected_csv_rows(self.src_path, "t", columns, ["id"])
        actual = read_csv(self.out_path)
        self.assertEqual(len(actual), self.ROWS + 1)
        self.assertEqual(actual, expected)
        self.assertEqual(
            get_job(str(self.meta_path), job_id)["rows_written"], self.ROWS
        )

    def _committed_offset(self, job_id):
        conn = sqlite3.connect(str(self.meta_path))
        try:
            return conn.execute(
                "SELECT committed_offset FROM jobs WHERE id = ?", (job_id,)
            ).fetchone()[0]
        finally:
            conn.close()


class TestCompositeKeyBadBatch(MigratorTestBase):
    """Composite PK with mixed int/text, a bad row, pause, fix, resume."""

    BATCH = 10

    def _build_source(self):
        conn = sqlite3.connect(str(self.src_path))
        conn.execute(
            "CREATE TABLE t (k1, k2, v TEXT, n, PRIMARY KEY (k1, k2))"
        )
        # k1 mixes integers and text; k2 mixes too. SQLite sorts all
        # numeric values before all text values within each column.
        k1_values = [1, 2, 3, "alpha", "beta", "gamma"]
        k2_values = [10, 20, "x", "y"]
        rows = []
        i = 0
        for k1 in k1_values:
            for k2 in k2_values:
                i += 1
                rows.append((k1, k2, f"value-{i}", i))
        conn.executemany("INSERT INTO t VALUES (?, ?, ?, ?)", rows)
        # Bad row: n is not convertible to integer. With BATCH=10 and 24
        # rows ordered by (k1, k2), this lands inside the third batch.
        conn.execute(
            "INSERT INTO t VALUES (?, ?, ?, ?)",
            ("delta", 5, "bad-row", "not-a-number"),
        )
        conn.commit()
        conn.close()

    def test_pause_fix_resume(self):
        self._build_source()
        columns = [
            {"source": "v", "target": "V", "type": "text"},
            {"source": "n", "target": "N", "type": "integer"},
        ]
        spec = {
            "source_path": str(self.src_path),
            "table": "t",
            "columns": columns,
            "output_path": str(self.out_path),
            "batch_size": self.BATCH,
        }
        job_id = create_job(str(self.meta_path), spec)

        result = run_job(str(self.meta_path), job_id)
        self.assertEqual(result["status"], "partially_complete")
        failure = result["failure"]
        self.assertEqual(failure["error_row_key"], ["delta", 5])
        self.assertIn("not-a-number", failure["reason"])
        # 25 rows, batches of 10: the third batch spans
        # ("delta", 5) .. ("gamma", "y"); the bad row is its first row.
        self.assertEqual(failure["batch_first_key"], ["delta", 5])
        self.assertEqual(failure["batch_last_key"], ["gamma", "y"])

        # Paused: the CSV must be a fully parseable prefix of the truth.
        conn = sqlite3.connect(str(self.src_path))
        prefix = conn.execute(
            'SELECT v, n FROM t WHERE (k1, k2) <= (?, ?) ORDER BY k1, k2',
            ("beta", "y"),
        ).fetchall()
        conn.close()
        expected_prefix = [["V", "N"]] + [
            [convert_value(v, "text"), convert_value(n, "integer")]
            for v, n in prefix
        ]
        # rows written = 20 (two full batches of 10)
        self.assertEqual(result["rows_written"], 20)
        self.assertEqual(read_csv(self.out_path), expected_prefix)

        # Fix the bad row; insert a new row inside the failed batch range;
        # delete one not-yet-written row inside that range too.
        conn = sqlite3.connect(str(self.src_path))
        conn.execute("UPDATE t SET n = 999 WHERE k1 = 'delta' AND k2 = 5")
        conn.execute(
            "INSERT INTO t VALUES ('delta', 7, 'inserted-later', 12345)"
        )
        conn.execute("DELETE FROM t WHERE k1 = 'gamma' AND k2 = 'x'")
        conn.commit()
        conn.close()

        result = run_job(str(self.meta_path), job_id)
        self.assertEqual(result["status"], "completed")

        expected = expected_csv_rows(self.src_path, "t", columns, ["k1", "k2"])
        self.assertEqual(read_csv(self.out_path), expected)
        self.assertEqual(
            get_job(str(self.meta_path), job_id)["rows_written"],
            len(expected) - 1,
        )


if __name__ == "__main__":
    unittest.main()
