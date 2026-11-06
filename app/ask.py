"""ask.py: the optional "Ask" box (config/ask.yaml, off by default; local only, never in the hosted app).

One question -> 2 LLM calls:
  1. the model picks ONE fixed tool + arguments (JSON, checked here; anything else -> "none")
  2. the tool runs on data/out with our own polars code (the model never writes queries)
  3. the model answers from those rows only; the app shows the rows under the answer as proof

Ranked = the stored gold status (confidence >= scoring.yaml abstain_below), not the app's slider.
"""
import csv
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

import polars as pl

ROOT = Path(__file__).resolve().parent.parent
COLS = ["name", "country", "domain", "score", "confidence", "status", "reasons", "pitch"]
COUNT_FIELDS = {"country": "country", "pitch": "pitch", "tier": "main_tier", "finding": "finding"}
NO_TOOL = ("I can only answer questions about the accounts in this app: top accounts (by country or problem), "
           "one company's details, or counts per country / problem / pitch.")


# ---------------- tools: fixed code over the gold tables ----------------
def _show(acc: pl.DataFrame) -> pl.DataFrame:
    return acc.sort(["score", "owner_key"], descending=[True, False]).select(
        "name", "country", "domain", pl.col("score").round(1), pl.col("confidence").round(2), "status",
        pl.col("reasons").list.join("; "), "pitch")


def top_accounts(acc, fnd, max_rows, country=None, finding=None, n=10) -> pl.DataFrame:
    out = acc.filter(pl.col("status") == "ranked")
    if country:
        out = out.filter(pl.col("country") == str(country).upper())
    if finding:
        out = out.filter(pl.col("owner_key").is_in(fnd.filter(pl.col("finding") == finding)["owner_key"].implode()))
    return _show(out).head(max(1, min(int(n), max_rows)))


def find_account(acc, fnd, max_rows, name="") -> pl.DataFrame:
    text = str(name).strip().lower()
    if not text:
        return _show(acc.head(0))
    hit = (pl.col("name").str.to_lowercase().str.contains(text, literal=True)
           | pl.col("domain").fill_null("").str.contains(text, literal=True))
    return _show(acc.filter(hit)).head(max_rows)


def count_by(acc, fnd, max_rows, field="country") -> pl.DataFrame:
    ranked = acc.filter(pl.col("status") == "ranked")
    if field == "finding":
        ranked = fnd.join(ranked.select("owner_key"), on="owner_key").unique(["owner_key", "finding"])
    col = COUNT_FIELDS[field]
    return (ranked.group_by(col).len("ranked accounts").rename({col: field})
            .sort(["ranked accounts", field], descending=[True, False]).head(max_rows))


TOOLS = {"top_accounts": top_accounts, "find_account": find_account, "count_by": count_by}
ARGS = {"top_accounts": {"country", "finding", "n"}, "find_account": {"name"}, "count_by": {"field"}}


def pick_tool(text: str, findings: list[str]) -> tuple[str, dict]:
    """Model answer -> (tool, args) with only known tools, known args and valid values. Never raises."""
    try:
        got = json.loads(text)
        tool, args = got["tool"], got.get("args") or {}
        if tool not in TOOLS or not isinstance(args, dict):
            return "none", {}
        args = {k: v for k, v in args.items() if k in ARGS[tool] and v not in (None, "")}
        if args.get("finding") and args["finding"] not in findings:
            return "none", {}
        if tool == "count_by" and args.get("field") not in COUNT_FIELDS:
            return "none", {}
        if "n" in args:
            args["n"] = int(args["n"])
        return tool, args
    except (ValueError, KeyError, TypeError):
        return "none", {}


# ---------------- LLM ----------------
def api_key(cfg: dict) -> str | None:
    """First env var in cfg['api_key_env'] that is set (shell first, then .env)."""
    sys.path.insert(0, str(ROOT / "pipeline"))
    from common import load_env
    load_env()
    return next((os.environ[k] for k in cfg["api_key_env"] if os.environ.get(k)), None)


def make_client(key: str):
    from groq import Groq          # imported only when Ask is on: the hosted app never needs it
    return Groq(api_key=key)


def _call(client, cfg: dict, system: str, user: str, json_mode: bool):
    kw = {"response_format": {"type": "json_object"}} if json_mode else {}
    r = client.chat.completions.create(model=cfg["model"], temperature=cfg["temperature"], **kw,
                                       messages=[{"role": "system", "content": system},
                                                 {"role": "user", "content": user}])
    return r.choices[0].message.content, r.usage.prompt_tokens, r.usage.completion_tokens


def ask(question: str, acc: pl.DataFrame, fnd: pl.DataFrame, cfg: dict, client) -> dict:
    """Answer one question. Returns {answer, tool, args, rows, tok_in, tok_out}."""
    findings = sorted(fnd["finding"].unique().to_list())
    tool_prompt = (ROOT / cfg["tool_prompt"]).read_text().replace("{findings}", ", ".join(findings))
    text, t_in, t_out = _call(client, cfg, tool_prompt, question, json_mode=True)
    tool, args = pick_tool(text, findings)
    if tool == "none":
        out = {"answer": NO_TOOL, "tool": tool, "args": {}, "rows": None, "tok_in": t_in, "tok_out": t_out}
    else:
        rows = TOOLS[tool](acc, fnd, cfg["max_rows"], **args)
        user = f"Question: {question}\n\nRows ({rows.height}):\n{json.dumps(rows.to_dicts(), ensure_ascii=False)}"
        answer, a_in, a_out = _call(client, cfg, (ROOT / cfg["answer_prompt"]).read_text(), user, json_mode=False)
        out = {"answer": answer, "tool": tool, "args": args, "rows": rows, "tok_in": t_in + a_in, "tok_out": t_out + a_out}
    log_cost(cfg, out)
    return out


def log_cost(cfg: dict, out: dict) -> None:
    p = cfg["price_usd_per_mtok"]
    usd = (out["tok_in"] * p["input"] + out["tok_out"] * p["output"]) / 1e6
    path = ROOT / cfg["cost_log"]
    new = not path.exists()
    with open(path, "a", newline="") as f:
        w = csv.writer(f)
        if new:
            w.writerow(["utc", "model", "prompt_version", "tool", "tok_in", "tok_out", "usd"])
        w.writerow([datetime.now(timezone.utc).isoformat(timespec="seconds"), cfg["model"], cfg["prompt_version"],
                    out["tool"], out["tok_in"], out["tok_out"], f"{usd:.5f}"])
