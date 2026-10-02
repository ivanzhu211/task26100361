# Resumable SQLite → CSV migrator

Stdlib-only batch migrator with crash-safe resume. See `migrator.py` docstring.

## API

- `migrator.CreateJob(spec, meta_path) -> job_id` — register a job (state `queued`).
- `migrator.RunJob(job_id, meta_path)` — resume from last committed progress
  until `completed` or `partially_complete` (bad batch paused).
- `migrator.GetJob(job_id, meta_path)` — state, rows written, failure info.

Spec: `{source_db, table, columns: [{source, target, type}], target_csv,
batch_size}` where `type` is `integer` / `real` / `text` and
`batch_size <= 1000`.

## CLI

```
python3 -m migrator create META spec.json   # prints job id
python3 -m migrator run META JOB_ID         # run/resume (safe after SIGKILL)
python3 -m migrator get META JOB_ID         # status as JSON
```

## Tests

```
python3 -m unittest test_migrator -v
```
