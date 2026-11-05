"""Orchestrator CLI. Run from the repo root:  python -m orchestrator <command> ...

  land     --input <local file> --dt D --source S [--hr H] [--file NAME]   put a file into landing/
  split    <meta>       landing file -> bronze parts
  process  <meta>       bronze parts -> silver (parallel, restartable)
  score    [--no-llm] [--dry-run]   all silver -> gold/dt=<today>/ + gold/latest/ -> data/out
  run      <meta>       split + process + score
  status                what is in the lake

<meta> = which landed file(s), one of:
  --dt 2026-11-01 --source shodan --file sample_assignment.json.zst [--hr 14]
  --meta meta.json      {"files": [{"dt": "2026-11-01", "source": "shodan", "file": "sample_assignment.json.zst"}]}
  --pending             every landed file that is not fully processed yet (for a cron job / S3 event)

The orchestrator builds every path from these details; storage (local folder or S3 bucket),
part size, workers and the hour switch come from config/orchestrator.yaml.
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "pipeline"))
from common import load_config, load_env  # noqa: E402

from orchestrator.process import process_files, score_and_publish  # noqa: E402
from orchestrator.split import split_file  # noqa: E402
from orchestrator.storage import (done_key, get_storage, landing_key, local_input, make_partition,  # noqa: E402
                                  partition_from_landing_key, parts_of, split_key)


def is_finished(storage, p) -> bool:
    if not storage.exists(split_key(p)):
        return False
    return all(storage.exists(done_key(p, n)) for n in parts_of(storage, p))


def landed(storage) -> list:
    return [partition_from_landing_key(k) for k in storage.list("landing/")
            if not k.endswith((".tmp", "/"))]


def partitions_from_args(args, ocfg, storage) -> list:
    use_hour = bool(ocfg["layout"]["use_hour"])
    if args.pending:
        todo = [p for p in landed(storage) if not is_finished(storage, p)]
        print(f"pending: {len(todo)} landed file(s) not fully processed")
        return todo
    if args.meta:
        meta = json.loads(Path(args.meta).read_text())
        items = meta.get("files", [meta])
    else:
        items = [{"dt": args.dt, "source": args.source, "file": args.file, "hr": args.hr}]
    try:
        return [make_partition({k: v for k, v in it.items() if v is not None}, use_hour) for it in items]
    except ValueError as e:
        sys.exit(f"bad meta: {e}")


def status(ocfg: dict) -> None:
    st = get_storage(ocfg)
    print(f"storage: {st.uri}")
    files = landed(st)
    if not files:
        print("  landing: empty")
    for p in files:
        if st.exists(split_key(p)):
            m = st.read_json(split_key(p))
            done = sum(st.exists(done_key(p, n)) for n in parts_of(st, p))
            print(f"  {p.label()}: {m['records']:,} records, {m['parts']} parts, {done} processed")
        else:
            print(f"  {p.label()}: landed, not split yet")
    print(f"  gold/latest: {'yes' if st.exists('gold/latest/accounts.parquet') else 'not yet'}")


def main() -> None:
    ap = argparse.ArgumentParser(prog="python -m orchestrator", description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("command", choices=["land", "split", "process", "score", "run", "status"])
    ap.add_argument("--dt", help="date of the file, YYYY-MM-DD")
    ap.add_argument("--source", help="data source / feed, e.g. shodan")
    ap.add_argument("--file", help="file name in landing (land: name to store it under)")
    ap.add_argument("--hr", help="hour 00-23 (only when layout.use_hour is true)")
    ap.add_argument("--meta", help="JSON file with the file(s) to handle")
    ap.add_argument("--pending", action="store_true", help="every landed file not fully processed yet")
    ap.add_argument("--input", help="land: local file to copy / upload into landing")
    ap.add_argument("--storage", choices=["local", "s3"], help="override config storage")
    ap.add_argument("--workers", type=int, help="override config process.workers")
    ap.add_argument("--part-size", type=int, help="override config split.records_per_part")
    ap.add_argument("--no-llm", action="store_true", help="score with existing LLM labels, no API calls")
    ap.add_argument("--dry-run", action="store_true", help="score: only count owner names the LLM would label")
    ap.add_argument("--force", action="store_true", help="redo a split / parts that are already done")
    args = ap.parse_args()

    load_env()                                     # AWS_* / GROQ_API_KEY from .env if present
    ocfg = load_config("orchestrator")
    if args.storage:
        ocfg["storage"] = args.storage
    if args.workers:
        ocfg["process"]["workers"] = args.workers
    if args.part_size:
        ocfg["split"]["records_per_part"] = args.part_size
    llm = ocfg["llm_step"] and not args.no_llm
    storage = get_storage(ocfg)

    if args.command == "status":
        return status(ocfg)
    if args.command == "score":
        score_and_publish(ocfg, llm, dry_run=args.dry_run)
        return
    if args.command == "land":
        if not args.input:
            sys.exit("land needs --input <local file>")
        src = local_input(args.input)
        args.file = args.file or src.name
        p = partitions_from_args(args, ocfg, storage)[0]
        print(f"land: {src} -> {storage.uri}/{landing_key(p)}")
        storage.put(src, landing_key(p))
        return

    parts = partitions_from_args(args, ocfg, storage)
    if args.command in ("split", "run"):
        for p in parts:
            split_file(storage, p, int(ocfg["split"]["records_per_part"]), int(ocfg["split"]["zstd_level"]),
                       ROOT / ocfg["process"]["work_dir"], args.force)
    if args.command in ("process", "run") and parts:
        process_files(ocfg, parts, args.force)
    if args.command == "run":
        score_and_publish(ocfg, llm)


if __name__ == "__main__":
    main()
