"""classify.py: LLM labels for the owner names of accounts that would be ranked (docs/DECISIONS.md D7).

Why only those: abstained accounts never reach a rep, so labelling them changes nothing.
Labels: company | infrastructure | default | unknown (see prompts/, config/llm.yaml).
score.py then drops finding rows whose owner is infrastructure/default.

Guardrails: temperature 0, JSON answer checked (bad/missing -> "unknown", not cached),
cache by hash(prompt_version, model, name), cost per run appended to logs/llm_cost.csv.

Run order: findings.py -> classify.py -> score.py
Usage (from repo root):
  python pipeline/classify.py --dry-run     # count names + cache hits, no API call
  python pipeline/classify.py              # key from .env (see .env.example)
"""
import argparse
import csv
import hashlib
import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import polars as pl

sys.path.insert(0, str(Path(__file__).resolve().parent))
from common import ROOT, connect, load_config, load_env, sql_str  # noqa: E402
from score import score  # noqa: E402

SOURCE = {"A": "cert", "B": "org", "C": "org"}   # tier-B owner_name is the org when it agreed


def cache_key(cfg: dict, name: str) -> str:
    return hashlib.sha256(f"{cfg['prompt_version']}|{cfg['model']}|{name}".encode()).hexdigest()[:16]


def load_cache(path: Path) -> dict:
    if not path.exists():
        return {}
    with open(path) as f:
        return {r["key"]: r for r in map(json.loads, f)}


def candidates(con, findings_src: str) -> pl.DataFrame:
    """Owner names (and where they came from) of accounts that are ranked WITHOUT labels."""
    acc = score(con, findings_src, load_config("scoring"))          # pure rules, no labels
    ranked = acc.filter(pl.col("status") == "ranked").select("owner_key")
    con.register("ranked", ranked)
    return con.sql(f"""
        SELECT owner_name AS name, mode(tier) AS tier
        FROM read_parquet({findings_src}) JOIN ranked USING (owner_key)
        WHERE owner_name IS NOT NULL GROUP BY 1 ORDER BY 1
    """).pl().with_columns(pl.col("tier").replace_strict(SOURCE).alias("source")).drop("tier")


def parse(text: str, n: int, labels: list[str]) -> dict[int, dict]:
    """Model answer -> {id: {label, reason}} with only valid ids and labels. Never raises."""
    try:
        rows = json.loads(text)["results"]
    except (ValueError, KeyError, TypeError):
        return {}
    out = {}
    for r in rows if isinstance(rows, list) else []:
        if isinstance(r, dict) and isinstance(r.get("id"), int) and 0 <= r["id"] < n and r.get("label") in labels:
            out[r["id"]] = {"label": r["label"], "reason": str(r.get("reason", ""))[:80]}
    return out


def ask(client, cfg: dict, prompt: str, batch: list[dict]) -> tuple[dict, int, int]:
    """One API call for one batch. Returns (parsed answers, tokens in, tokens out)."""
    items = [{"id": i, "name": b["name"], "source": b["source"]} for i, b in enumerate(batch)]
    for attempt in range(cfg["max_retries"] + 1):
        try:
            resp = client.chat.completions.create(
                model=cfg["model"], temperature=cfg["temperature"],
                response_format={"type": "json_object"},
                messages=[{"role": "system", "content": prompt},
                          {"role": "user", "content": json.dumps({"names": items}, ensure_ascii=False)}])
            got = parse(resp.choices[0].message.content, len(batch), cfg["labels"])
            return got, resp.usage.prompt_tokens, resp.usage.completion_tokens
        except Exception as e:  # network / rate limit: retry, then give up -> unknown
            wait = min(cfg.get("max_wait_s", 60), 5 * 2 ** attempt)   # 5, 10, 20, 40 s: rides out rate limits
            print(f"  call failed ({type(e).__name__}: {str(e)[:120]}); attempt {attempt + 1}, waiting {wait}s",
                  flush=True)
            if attempt < cfg["max_retries"]:
                time.sleep(wait)
    return {}, 0, 0


def classify(names: pl.DataFrame, cfg: dict, client, cache_path: Path) -> tuple[pl.DataFrame, dict]:
    """Label every name: cache first, then the API in batches. Returns (labels, run stats)."""
    cache = load_cache(cache_path)
    todo = [r for r in names.iter_rows(named=True) if cache_key(cfg, r["name"]) not in cache]
    stats = {"names": names.height, "cache_hits": names.height - len(todo), "calls": 0, "tok_in": 0, "tok_out": 0}
    prompt = (ROOT / cfg["prompt_file"]).read_text()
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    with open(cache_path, "a") as f:
        for i in range(0, len(todo), cfg["batch_size"]):
            batch = todo[i:i + cfg["batch_size"]]
            got, t_in, t_out = ask(client, cfg, prompt, batch)
            stats["calls"] += 1
            stats["tok_in"] += t_in
            stats["tok_out"] += t_out
            for j, b in enumerate(batch):
                if j in got:   # only valid answers are cached; missing ones retry next run
                    row = {"key": cache_key(cfg, b["name"]), "name": b["name"], **got[j]}
                    cache[row["key"]] = row
                    f.write(json.dumps(row, ensure_ascii=False) + "\n")
            print(f"  batch {i // cfg['batch_size'] + 1}: {len(got)}/{len(batch)} valid", flush=True)
    out = [{"name": r["name"], "source": r["source"],
            **{k: cache.get(cache_key(cfg, r["name"]), {}).get(k) for k in ("label", "reason")}}
           for r in names.iter_rows(named=True)]
    df = pl.DataFrame(out, schema={"name": pl.Utf8, "source": pl.Utf8, "label": pl.Utf8, "reason": pl.Utf8})
    return df.with_columns(pl.col("label").fill_null("unknown")), stats


def log_cost(cfg: dict, stats: dict, path: Path) -> float:
    p = cfg["price_usd_per_mtok"]
    usd = (stats["tok_in"] * p["input"] + stats["tok_out"] * p["output"]) / 1e6
    new = not path.exists()
    with open(path, "a", newline="") as f:
        w = csv.writer(f)
        if new:
            w.writerow(["utc", "model", "prompt_version", "names", "cache_hits", "calls", "tok_in", "tok_out", "usd"])
        w.writerow([datetime.now(timezone.utc).isoformat(timespec="seconds"), cfg["model"], cfg["prompt_version"],
                    stats["names"], stats["cache_hits"], stats["calls"], stats["tok_in"], stats["tok_out"], f"{usd:.4f}"])
    return usd


def run(findings_src: str, dry_run: bool = False) -> pl.DataFrame | None:
    """Label owner names of would-be-ranked accounts in `findings_src` (SQL string / glob)."""
    cfg = load_config("llm")
    paths = {k: ROOT / v for k, v in cfg["paths"].items()}
    names = candidates(connect(), findings_src)
    cache = load_cache(paths["cache"])
    hits = sum(cache_key(cfg, n) in cache for n in names["name"])
    print(f"{names.height} names to label ({names['source'].value_counts().sort('source').rows()}), "
          f"{hits} already cached, {names.height - hits} need the API "
          f"(~{-(-(names.height - hits) // cfg['batch_size'])} calls)")
    if dry_run:
        return None

    load_env()  # GROQ_API_KEY from .env (copy .env.example); a shell export wins
    if not os.environ.get("GROQ_API_KEY"):
        sys.exit("GROQ_API_KEY not set: copy .env.example to .env and fill it in")
    from groq import Groq  # imported here so the rest of the pipeline never needs it
    labels, stats = classify(names, cfg, Groq(), paths["cache"])
    paths["labels"].parent.mkdir(parents=True, exist_ok=True)
    labels.write_parquet(paths["labels"])
    usd = log_cost(cfg, stats, paths["cost_log"])
    print(labels["label"].value_counts().sort("count", descending=True))
    print(f"calls {stats['calls']}, tokens {stats['tok_in']}/{stats['tok_out']}, ~${usd:.4f} -> {paths['labels'].relative_to(ROOT)}")
    return labels


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--dry-run", action="store_true", help="count names and cache hits; no API call")
    args = ap.parse_args()
    run(sql_str(str(ROOT / load_config("pipeline")["paths"]["findings"] / "*.parquet")), args.dry_run)


if __name__ == "__main__":
    main()
