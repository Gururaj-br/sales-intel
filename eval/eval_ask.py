"""eval_ask.py: does the Ask box pick the right tool? (real API calls; key as in config/ask.yaml)

  python eval/eval_ask.py

A question passes when the tool matches and every expected argument matches (names: the expected text is
contained in the model's search text, case-insensitive; extra arguments such as n are allowed when not expected).
Answers are printed so a person can check they only use the returned rows.
"""
import sys
from pathlib import Path

import polars as pl
import yaml

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "app"))
import ask  # noqa: E402


def matches(exp: dict, tool: str, args: dict) -> bool:
    if tool != exp["tool"]:
        return False
    for k, v in exp["args"].items():
        got = args.get(k)
        if k == "name":
            if not got or str(v).lower() not in str(got).lower():
                return False
        elif str(got).upper() != str(v).upper():
            return False
    return True


def main() -> None:
    cfg = yaml.safe_load((ROOT / "config/ask.yaml").read_text())
    key = ask.api_key(cfg)
    if not key:
        sys.exit(f"no API key: set {' or '.join(cfg['api_key_env'])}")
    client = ask.make_client(key)
    acc = pl.read_parquet(ROOT / "data/out/accounts.parquet")
    fnd = pl.read_parquet(ROOT / "data/out/account_findings.parquet")
    gold = yaml.safe_load((ROOT / "eval/ask_questions.yaml").read_text())
    ok = 0
    for g in gold:
        res = ask.ask(g["q"], acc, fnd, cfg, client)
        hit = matches(g, res["tool"], res["args"])
        ok += hit
        print(f"{'PASS' if hit else 'FAIL'} | {g['q']}\n     expected {g['tool']} {g['args']} | got {res['tool']} {res['args']}")
        print("     " + res["answer"].replace("\n", "\n     ")[:600] + "\n")
    print(f"tool choice correct: {ok}/{len(gold)}  (prompt {cfg['prompt_version']}, model {cfg['model']})")


if __name__ == "__main__":
    main()
