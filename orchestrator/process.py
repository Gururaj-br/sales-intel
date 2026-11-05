"""process.py: bronze chunks -> silver (4 stages per chunk, in parallel) -> gold -> app.

Per chunk (one worker each, independent of every other chunk):
  fetch bronze chunk -> ingest -> eligibility -> attribution -> findings -> put 4 silver files
  -> write manifest/source=<s>/chunk=<n>.json (the "done" marker + row counts).
A chunk with a done marker is skipped on reruns, so a crashed run just continues.

Then, once for all sources together:
  (optional) LLM labels for new owner names -> score (latest sighting per IP+port wins)
  -> gold/accounts.parquet + gold/account_findings.parquet -> copied to data/out for the app.
"""
from __future__ import annotations

import sys
import tempfile
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path

import polars as pl

from orchestrator.storage import (ROOT, STAGES, LocalStorage, chunk_name, chunks_of, done_key,
                                  get_storage, gold_key, silver_key, bronze_key)

sys.path.insert(0, str(ROOT / "pipeline"))
from attribute import attribute_part  # noqa: E402
from common import connect, load_config, sql_str  # noqa: E402
from eligibility import label_part  # noqa: E402
from findings import findings_part  # noqa: E402
from ingest import ingest_part  # noqa: E402


def process_chunk(ocfg: dict, source: str, chunk: int, force: bool = False) -> dict:
    """Run the 4 per-chunk stages. Runs inside a worker process; returns the manifest row."""
    storage = get_storage(ocfg)
    if storage.exists(done_key(source, chunk)) and not force:
        return {**storage.read_json(done_key(source, chunk)), "skipped": True}

    p = ocfg["process"]
    work = ROOT / p["work_dir"]
    work.mkdir(parents=True, exist_ok=True)
    t0 = time.time()
    with tempfile.TemporaryDirectory(dir=work) as tmp:
        tmp = Path(tmp)
        con = connect(p["duckdb_memory_per_worker"], p["duckdb_threads_per_worker"], tmp / "duckdb")
        name = chunk_name(source, chunk)
        raw = storage.get(bronze_key(source, chunk), tmp / "bronze")
        local = {s: tmp / s / f"{name}.parquet" for s in STAGES}
        for f in local.values():
            f.parent.mkdir(parents=True, exist_ok=True)

        excl, weights = load_config("exclusions"), load_config("scoring")["attribution_weight"]
        stats = ingest_part(con, raw, local["observations"].parent, load_config("pipeline"))
        elig = label_part(con, local["observations"], local["eligibility"], excl)
        attr = attribute_part(con, local["eligibility"], local["attribution"], excl, weights)
        fnd = findings_part(con, local["attribution"], local["findings"], load_config("findings"))
        con.close()

        for stage, f in local.items():
            storage.put(f, silver_key(stage, source, chunk))

    row = {"source": source, "chunk": chunk, **stats, "findings": fnd.height,
           "eligibility": elig.drop("part").to_dicts(), "attribution": attr.to_dicts(),
           "seconds_total": round(time.time() - t0, 1)}
    storage.write_json(done_key(source, chunk), row)     # written LAST = chunk is done
    return row


def process_sources(ocfg: dict, sources: list[str], force: bool = False) -> list[dict]:
    """All chunks of all sources, `workers` at a time."""
    storage = get_storage(ocfg)
    tasks = [(s, c) for s in sources for c in chunks_of(storage, s)]
    if not tasks:
        sys.exit(f"no bronze chunks for {sources} in {storage.uri}: run `split` first")
    workers = int(ocfg["process"]["workers"])
    print(f"process: {len(tasks)} chunks from {len(sources)} source(s), {workers} worker(s), storage {storage.uri}")
    rows, t0 = [], time.time()
    if workers <= 1:                                  # inline: easier to debug, used by tests
        results = (process_chunk(ocfg, s, c, force) for s, c in tasks)
    else:
        pool = ProcessPoolExecutor(max_workers=workers)
        futures = [pool.submit(process_chunk, ocfg, s, c, force) for s, c in tasks]
        results = (f.result() for f in as_completed(futures))
    for i, row in enumerate(results, 1):
        rows.append(row)
        what = "skipped (done before)" if row.get("skipped") else f"{row['rows_out']:,} rows, {row['findings']:,} findings, {row['seconds_total']}s"
        print(f"  [{i}/{len(tasks)}] {row['source']} chunk {row['chunk']:05d}: {what}", flush=True)
    if workers > 1:
        pool.shutdown()
    print(f"process: done in {time.time() - t0:.0f}s")
    write_summaries(rows)
    return rows


def write_summaries(rows: list[dict]) -> None:
    """Run totals -> logs/ (rows in/out, what was excluded, how owners were proven)."""
    logs = ROOT / "logs"
    logs.mkdir(exist_ok=True)
    keep = ["source", "chunk", "rows_raw", "rows_rejected", "rows_no_ip", "dupes_dropped", "rows_out",
            "findings", "seconds_total"]
    pl.DataFrame([{k: r.get(k) for k in keep} for r in rows]).sort("source", "chunk").write_csv(logs / "chunk_summary.csv")
    for part, cols in [("eligibility", ["status", "reason"]), ("attribution", ["status", "tier", "attr_note"])]:
        df = pl.DataFrame([x for r in rows for x in r[part]])
        (df.group_by(cols).agg(pl.col("rows").sum())
           .with_columns((pl.col("rows") / pl.col("rows").sum() * 100).round(1).alias("pct"))
           .sort("rows", descending=True).write_csv(logs / f"{part}_summary.csv"))


def score_and_publish(ocfg: dict, llm: bool) -> pl.DataFrame:
    """All findings (every source) -> optional LLM labels -> gold -> data/out."""
    import classify
    import score

    storage = get_storage(ocfg)
    work = ROOT / ocfg["process"]["work_dir"]
    work.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(dir=work) as tmp:
        tmp = Path(tmp)
        if isinstance(storage, LocalStorage):
            src = sql_str(str(storage.root / "silver" / "findings" / "**" / "*.parquet"))
        else:                                        # findings are small: download them all
            for key in storage.list("silver/findings/", ".parquet"):
                storage.get(key, tmp / "findings" / key.rsplit("/", 2)[-2])
            src = sql_str(str(tmp / "findings" / "**" / "*.parquet"))
        if llm:
            classify.run(src)                        # only names not in the cache cost money
        gold = tmp / "gold"
        acc = score.run(src, gold)
        for f in ("accounts.parquet", "account_findings.parquet"):
            storage.put(gold / f, gold_key(f))
            if ocfg.get("publish_to"):
                LocalStorage(ocfg["publish_to"]).put(gold / f, f)
    print(f"gold -> {storage.uri}/gold ; app copy -> {ocfg.get('publish_to')}")
    return acc
