"""CLI entry point.

Usage:
    python -m migrator create META_PATH SPEC_JSON_FILE
    python -m migrator run    META_PATH JOB_ID
    python -m migrator get    META_PATH JOB_ID
"""

import argparse
import json
import sys

from .core import create_job, get_job, run_job


def main(argv=None):
    parser = argparse.ArgumentParser(prog="migrator")
    sub = parser.add_subparsers(dest="command", required=True)

    p_create = sub.add_parser("create", help="register a job from a spec JSON file")
    p_create.add_argument("meta_path")
    p_create.add_argument("spec_file")

    p_run = sub.add_parser("run", help="run/resume a job")
    p_run.add_argument("meta_path")
    p_run.add_argument("job_id", type=int)

    p_get = sub.add_parser("get", help="show job status")
    p_get.add_argument("meta_path")
    p_get.add_argument("job_id", type=int)

    args = parser.parse_args(argv)

    if args.command == "create":
        with open(args.spec_file, "r", encoding="utf-8") as f:
            spec = json.load(f)
        job_id = create_job(args.meta_path, spec)
        print(json.dumps({"id": job_id}))
        return 0
    if args.command == "run":
        result = run_job(args.meta_path, args.job_id)
        print(json.dumps(result))
        return 0 if result["status"] == "completed" else 1
    if args.command == "get":
        print(json.dumps(get_job(args.meta_path, args.job_id)))
        return 0
    return 2


if __name__ == "__main__":
    sys.exit(main())
