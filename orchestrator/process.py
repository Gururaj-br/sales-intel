"""process.py: bronze parts -> silver (4 stages per part, in parallel) -> gold -> app.

Per part (one worker each, independent of every other part):
  fetch bronze part -> ingest -> eligibility -> attribution -> findings -> put 4 silver files
  -> write manifest/<partition>/part-NNNNN.done.json (the "done" marker + row counts).
A part with a done marker is skipped on reruns, so a crashed run just continues.

Then, once over everything in silver:
  (optional) LLM labels for new owner names -> score (latest sighting per IP+port wins)
  -> gold/dt=<run date>/ (snapshot) + gold/latest/ -> copied to data/out for the app.
"""
from __future__ import annotations

import sys
import tempfile
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path

import polars as pl

from orchestrator.storage import (ROOT, STAGES, LocalStorage, Partition, bronze_key, done_key,
                                  get_storage, gold_key, parts_of, silver_key)

sys.path.insert(0, str(ROOT / "pipeline"))
from attribute import attribute_part  # noqa: E402
from common import connect, load_config, sql_str  # noqa: E402
from eligibility import label_part  # noqa: E402
from findings import findings_part  # noqa: E402
from ingest import ingest_part  # noqa: E402


def process_part(ocfg: dict, p: Partition, n: int, force: bool = False) -> dict:
    """Run the 4 per-part stages. Runs inside a worker process; returns the manifest row."""
    storage = get_storage(ocfg)
    if storage.exists(done_key(p, n)) and not force:
        return {**storage.read_json(done_key(p, n)), "skipped": True}

    cfg = ocfg["process"]
    work = ROOT / cfg["work_dir"]
    work.mkdir(parents=True, exist_ok=True)
    t0 = time.time()
    with tempfile.TemporaryDirectory(dir=work) as tmp:
        tmp = Path(tmp)
        con = connect(cfg["duckdb_memory_per_worker"], cfg["duckdb_threads_per_worker"], tmp / "duckdb")
        # unique local name: it becomes the `part` id inside the data, so it must not repeat across files
        name = "_".join(x for x in (p.dt, p.source, p.hr, p.file_id, f"part-{n:05d}") if x)
        raw = storage.get(bronze_key(p, n), tmp / "bronze", name=f"{name}.json.zst")
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
            storage.put(f, silver_key(stage, p, n))

    row = {"partition": p.path(), "part": n, **{k: v for k, v in stats.items() if k != "part"},
           "findings": fnd.height, "eligibility": elig.drop("part").to_dicts(), "attribution": attr.to_dicts(),
           "seconds_total": round(time.time() - t0, 1)}
    storage.write_json(done_key(p, n), row)     # written LAST = part is done
    return row


def process_files(ocfg: dict, partitions: list[Partition], force: bool = False) -> list[dict]:
    """All parts of all given landed files, `workers` at a time."""
    storage = get_storage(ocfg)
    tasks = [(p, n) for p in partitions for n in parts_of(storage, p)]
    if not tasks:
        sys.exit(f"no bronze parts for {[p.label() for p in partitions]} in {storage.uri}: run `split` first")
    workers = int(ocfg["process"]["workers"])
    print(f"process: {len(tasks)} parts from {len(partitions)} file(s), {workers} worker(s), storage {storage.uri}")
    rows, t0 = [], time.time()
    if workers <= 1:                                  # inline: easier to debug, used by tests
        results = (process_part(ocfg, p, n, force) for p, n in tasks)
    else:
        pool = ProcessPoolExecutor(max_workers=workers)
        futures = [pool.submit(process_part, ocfg, p, n, force) for p, n in tasks]
        results = (f.result() for f in as_completed(futures))
    for i, row in enumerate(results, 1):
        rows.append(row)
        what = ("skipped (done before)" if row.get("skipped")
                else f"{row['rows_out']:,} rows, {row['findings']:,} findings, {row['seconds_total']}s")
        print(f"  [{i}/{len(tasks)}] {row['partition']} part-{row['part']:05d}: {what}", flush=True)
    if workers > 1:
        pool.shutdown()
    print(f"process: done in {time.time() - t0:.0f}s")
    write_summaries(rows)
    return rows


def write_summaries(rows: list[dict]) -> None:
    """Run totals -> logs/orchestrator/ (git-ignored; never overwrites the shipped logs/*.csv)."""
    out = ROOT / "logs" / "orchestrator"
    out.mkdir(parents=True, exist_ok=True)
    keep = ["partition", "part", "rows_raw", "rows_rejected", "rows_no_ip", "dupes_dropped", "rows_out",
            "findings", "seconds_total"]
    pl.DataFrame([{k: r.get(k) for k in keep} for r in rows]).sort("partition", "part").write_csv(out / "parts.csv")
    for stage, cols in [("eligibility", ["status", "reason"]), ("attribution", ["status", "tier", "attr_note"])]:
        df = pl.DataFrame([x for r in rows for x in r[stage]])
        (df.group_by(cols).agg(pl.col("rows").sum())
           .with_columns((pl.col("rows") / pl.col("rows").sum() * 100).round(1).alias("pct"))
           .sort("rows", descending=True).write_csv(out / f"{stage}.csv"))


def score_and_publish(ocfg: dict, llm: bool, run_dt: str | None = None) -> pl.DataFrame:
    """All silver findings -> optional LLM labels -> gold/dt=<run date>/ + gold/latest/ -> data/out."""
    import classify
    import score

    run_dt = run_dt or datetime.now(timezone.utc).date().isoformat()
    storage = get_storage(ocfg)
    work = ROOT / ocfg["process"]["work_dir"]
    work.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(dir=work) as tmp:
        tmp = Path(tmp)
        if isinstance(storage, LocalStorage):
            src = sql_str(str(storage.root / "silver" / "findings" / "**" / "*.parquet"))
        else:                                        # findings are small: download them all, keeping paths
            keys = storage.list("silver/findings/", ".parquet")
            for key in keys:
                storage.get(key, tmp)
            src = sql_str(str(tmp / "silver" / "findings" / "**" / "*.parquet"))
        if llm:
            classify.run(src)                        # only names not in the cache cost money
        gold = tmp / "gold"
        acc = score.run(src, gold)
        for f in ("accounts.parquet", "account_findings.parquet"):
            storage.put(gold / f, gold_key(f, run_dt))     # snapshot for this scoring day
            storage.put(gold / f, gold_key(f))             # latest
            if ocfg.get("publish_to"):
                LocalStorage(ocfg["publish_to"]).put(gold / f, f)
    print(f"gold -> {storage.uri}/gold/dt={run_dt}/ and gold/latest/ ; app copy -> {ocfg.get('publish_to')}")
    return acc
