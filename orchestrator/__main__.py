"""Orchestrator CLI. Run from the repo root:  python -m orchestrator <command> ...

  split    --input <file or s3://bucket/key> --source <id>   big file -> bronze chunks
  process  --source <id> [--source <id> ...]                 chunks -> silver (parallel)
  score    [--no-llm]                                         all silver -> gold -> data/out
  run      --meta meta.json   (or --input + --source)         split + process + score
  status                                                      what is in the lake

meta.json (what an S3 upload event / scheduler would pass):
  {"sources": [{"source": "scan_2026_11_01", "input": "s3://my-bucket/incoming/scan.json.zst"}]}
Storage (local folder or S3) and all sizes come from config/orchestrator.yaml.
"""
from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "pipeline"))
from common import load_config, load_env  # noqa: E402

from orchestrator.process import process_sources, score_and_publish  # noqa: E402
from orchestrator.split import split_file  # noqa: E402
from orchestrator.storage import chunks_of, done_key, get_storage, meta_key  # noqa: E402

SOURCE_RE = re.compile(r"^[A-Za-z0-9_-]+$")   # safe in folder names and in "source=<id>"


def source_from_input(uri: str) -> str:
    """scan-2026-11.json.zst -> scan-2026-11 (default source id)."""
    return re.sub(r"[^A-Za-z0-9_-]", "_", uri.rstrip("/").rsplit("/", 1)[-1].split(".")[0])


def parse_meta(args) -> list[dict]:
    if args.meta:
        meta = json.loads(Path(args.meta).read_text())
        items = meta.get("sources", [meta])
    elif args.input:
        items = [{"input": args.input, "source": args.source[0] if args.source else None}]
    else:
        items = [{"source": s} for s in (args.source or [])]
    for it in items:
        it["source"] = it.get("source") or source_from_input(it["input"])
        if not SOURCE_RE.match(it["source"]):
            sys.exit(f"bad source id {it['source']!r}: use letters, digits, _ and - only")
    if not items:
        sys.exit("nothing to do: pass --meta, --input or --source")
    return items


def status(ocfg: dict) -> None:
    st = get_storage(ocfg)
    sources = sorted({k.split("source=")[1].split("/")[0] for k in st.list("bronze/", "_meta.json")})
    print(f"storage: {st.uri}")
    for s in sources:
        m = st.read_json(meta_key(s))
        chunks = list(chunks_of(st, s))
        done = sum(st.exists(done_key(s, c)) for c in chunks)
        print(f"  source={s}: {m['records']:,} records, {len(chunks)} chunks, {done} processed")
    print(f"  gold: {'yes' if st.exists('gold/accounts.parquet') else 'not yet'}")


def main() -> None:
    ap = argparse.ArgumentParser(prog="python -m orchestrator", description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("command", choices=["split", "process", "score", "run", "status"])
    ap.add_argument("--meta", help="JSON file with the source(s) to handle")
    ap.add_argument("--input", help="big input file: local path or s3://bucket/key")
    ap.add_argument("--source", action="append", help="source id (repeatable)")
    ap.add_argument("--storage", choices=["local", "s3"], help="override config storage")
    ap.add_argument("--workers", type=int, help="override config workers")
    ap.add_argument("--no-llm", action="store_true", help="score with existing LLM labels, no API calls")
    ap.add_argument("--force", action="store_true", help="redo split / chunks that are already done")
    args = ap.parse_args()

    load_env()                                     # AWS_* / GROQ_API_KEY from .env if present
    ocfg = load_config("orchestrator")
    if args.storage:
        ocfg["storage"] = args.storage
    if args.workers:
        ocfg["process"]["workers"] = args.workers
    llm = ocfg["llm_step"] and not args.no_llm

    if args.command == "status":
        return status(ocfg)
    if args.command == "score":
        score_and_publish(ocfg, llm)
        return
    items = parse_meta(args)
    storage = get_storage(ocfg)
    if args.command in ("split", "run"):
        for it in items:
            if "input" not in it:
                sys.exit(f"source {it['source']}: no input to split")
            split_file(storage, it["input"], it["source"], int(ocfg["split"]["records_per_chunk"]),
                       int(ocfg["split"]["zstd_level"]), ROOT / ocfg["process"]["work_dir"], args.force)
    if args.command in ("process", "run"):
        process_sources(ocfg, [it["source"] for it in items], args.force)
    if args.command == "run":
        score_and_publish(ocfg, llm)


if __name__ == "__main__":
    main()
