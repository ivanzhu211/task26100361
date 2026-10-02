import csv
import os
import random
import signal
import sqlite3
import subprocess
import sys
import tempfile
import time
import unittest

import migrator

WORKSPACE = os.path.dirname(os.path.abspath(__file__))


def read_csv(path):
    with open(path, newline="", encoding="utf-8") as fh:
        return list(csv.reader(fh))


def sqlite_key_lt(a, b):
    """Tuple `<` under SQLite's cross-type ordering (numbers < text)."""
    for x, y in zip(a, b):
        rx = 0 if isinstance(x, (int, float)) else 1
        ry = 0 if isinstance(y, (int, float)) else 1
        if rx != ry:
            return rx < ry
        if x != y:
            return x < y
    return False


def expected_rows(db_path, table, columns):
    """Ground truth: key-ordered source rows converted per the spec,
    rendered as the strings csv.writer would emit."""
    src = sqlite3.connect(db_path)
    try:
        key_cols = migrator._key_columns(src, table)
        select = key_cols + [c["source"] for c in columns
                             if c["source"] not in key_cols]
        sql = "SELECT {} FROM {} ORDER BY {}".format(
            ", ".join(migrator._quote_ident(c) for c in select),
            migrator._quote_ident(table),
            ", ".join(migrator._quote_ident(k) for k in key_cols),
        )
        idx = {name: i for i, name in enumerate(select)}
        out = []
        for raw in src.execute(sql):
            key = tuple(raw[: len(key_cols)])
            try:
                vals = [
                    migrator._convert_value(raw[idx[c["source"]]], c["type"])
                    for c in columns
                ]
            except migrator.ConversionError:
                vals = None
            out.append((key, None if vals is None else [str(v) for v in vals]))
        return out
    finally:
        src.close()


class KillResumeTest(unittest.TestCase):
    """100k+ rows, hostile field content, random SIGKILLs in a subprocess."""

    def test_sigkill_resume(self):
        rng = random.Random(20261003)
        with tempfile.TemporaryDirectory() as tmp:
            db = os.path.join(tmp, "src.db")
            meta = os.path.join(tmp, "meta.db")
            out_csv = os.path.join(tmp, "out.csv")

            n_rows = 100_000
            src = sqlite3.connect(db)
            src.execute(
                "CREATE TABLE t (id INTEGER PRIMARY KEY, txt TEXT, val REAL)"
            )
            chunk = []
            for i in range(1, n_rows + 1):
                txt = 'row,{}\n"quoted"\r\n"x""y"\nend {}'.format(i, i % 13)
                chunk.append((i, txt, i * 0.25))
                if len(chunk) == 5000:
                    src.executemany("INSERT INTO t VALUES (?,?,?)", chunk)
                    chunk.clear()
            if chunk:
                src.executemany("INSERT INTO t VALUES (?,?,?)", chunk)
            src.commit()
            src.close()

            columns = [
                {"source": "id", "target": "ID", "type": "integer"},
                {"source": "txt", "target": "TXT", "type": "text"},
                {"source": "val", "target": "VAL", "type": "real"},
            ]
            spec = {
                "source_db": db,
                "table": "t",
                "columns": columns,
                "target_csv": out_csv,
                "batch_size": 1000,
            }
            job_id = migrator.CreateJob(spec, meta)

            env = dict(os.environ, MIGRATOR_BATCH_DELAY_MS="10")
            kills = 0
            deadline = time.time() + 600
            while True:
                self.assertLess(time.time(), deadline, "job never completed")
                proc = subprocess.Popen(
                    [sys.executable, "-m", "migrator", "run", meta, str(job_id)],
                    cwd=WORKSPACE,
                    env=env,
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                )
                time.sleep(rng.uniform(0.05, 0.35))
                if proc.poll() is None:
                    proc.send_signal(signal.SIGKILL)
                    kills += 1
                rc = proc.wait()
                if rc == 0:
                    break
                self.assertEqual(
                    rc, -signal.SIGKILL,
                    f"unexpected exit code {rc} (paused or crashed?)")

            self.assertGreaterEqual(kills, 3, "not enough SIGKILLs happened")

            info = migrator.GetJob(job_id, meta)
            self.assertEqual(info["state"], "completed")
            self.assertEqual(info["rows_written"], n_rows)

            expected = [r for _, r in expected_rows(db, "t", columns)]
            actual = read_csv(out_csv)
            self.assertEqual(actual[0], ["ID", "TXT", "VAL"])
            self.assertEqual(actual[1:], expected)


class CompositeKeyBadBatchTest(unittest.TestCase):
    """Composite PK with mixed int/text keys, a bad row, repair, insert, resume."""

    def test_bad_batch_pause_and_resume(self):
        with tempfile.TemporaryDirectory() as tmp:
            db = os.path.join(tmp, "src.db")
            meta = os.path.join(tmp, "meta.db")
            out_csv = os.path.join(tmp, "out.csv")

            src = sqlite3.connect(db)
            src.execute(
                "CREATE TABLE t (k1, k2, n INTEGER, payload TEXT, "
                "PRIMARY KEY (k1, k2))"
            )
            for i in range(1, 41):
                k1 = i if i % 2 == 0 else f"key{i:03d}"
                src.execute("INSERT INTO t VALUES (?,?,?,?)",
                            (k1, i, i * 10, f"payload {i}"))
            # Bad row: 'n' must be integer per the spec, but holds text.
            src.execute(
                "UPDATE t SET n = 'not-a-number' WHERE k1 = 'key015' AND k2 = 15"
            )
            src.commit()
            src.close()

            columns = [
                {"source": "k1", "target": "K1", "type": "text"},
                {"source": "k2", "target": "K2", "type": "integer"},
                {"source": "n", "target": "N", "type": "integer"},
                {"source": "payload", "target": "PAYLOAD", "type": "text"},
            ]
            spec = {
                "source_db": db,
                "table": "t",
                "columns": columns,
                "target_csv": out_csv,
                "batch_size": 10,
            }
            job_id = migrator.CreateJob(spec, meta)
            info = migrator.RunJob(job_id, meta)

            self.assertEqual(info["state"], "partially_complete")
            failure = info["failure"]
            self.assertEqual(failure["error_key"], ["key015", 15])
            self.assertIn("not-a-number", failure["error"])
            first_key = tuple(failure["batch_first_key"])
            last_key = tuple(failure["batch_last_key"])
            self.assertLessEqual(first_key, tuple(failure["error_key"]))
            self.assertLessEqual(tuple(failure["error_key"]), last_key)

            # While paused, the CSV must be a parseable prefix: exactly the
            # records before the failed batch.
            expected = expected_rows(db, "t", columns)
            prefix = [r for k, r in expected if sqlite_key_lt(k, first_key)]
            actual = read_csv(out_csv)
            self.assertEqual(actual[0], ["K1", "K2", "N", "PAYLOAD"])
            self.assertEqual(actual[1:], prefix)
            self.assertEqual(info["rows_written"], len(prefix))
            self.assertGreater(len(prefix), 0)

            # Fix the bad row; insert a new row inside the failed batch's
            # key range; delete one row past the failed batch.
            src = sqlite3.connect(db)
            src.execute("UPDATE t SET n = 150 WHERE k1 = 'key015' AND k2 = 15")
            src.execute("INSERT INTO t VALUES ('key016', 1600, 16000, 'new row')")
            src.execute("DELETE FROM t WHERE k1 = 'key017' AND k2 = 17")
            src.commit()
            src.close()

            info = migrator.RunJob(job_id, meta)
            self.assertEqual(info["state"], "completed")

            expected = [r for _, r in expected_rows(db, "t", columns)]
            actual = read_csv(out_csv)
            self.assertEqual(actual[0], ["K1", "K2", "N", "PAYLOAD"])
            self.assertEqual(actual[1:], expected)
            self.assertEqual(info["rows_written"], len(expected))
            self.assertIn(["key016", "1600", "16000", "new row"], actual)


if __name__ == "__main__":
    unittest.main()
