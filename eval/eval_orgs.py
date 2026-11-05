"""eval_orgs.py: does the LLM label step beat the rules baseline? (CLAUDE.md: gold-set check)

Gold: eval/gold_orgs.csv (names behind the baseline top-60 accounts + 40 random; my labels).
  1. Label quality on gold names (gold "unknown" rows skipped):
       drop decision = label in drop_labels. Baseline (rules) never drops anything.
  2. Ranking: of the top 25 ranked accounts, how many are gold "company"? rules vs rules+LLM.
Rule (docs/LLM_USAGE.md): if the LLM does not beat the baseline, remove it and say so.

Usage (from repo root, after classify.py):  python eval/eval_orgs.py
"""
import sys
from pathlib import Path

import polars as pl

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "pipeline"))
from common import connect, load_config, sql_str  # noqa: E402
from score import score  # noqa: E402

TOP = 25


def label_quality(gold: pl.DataFrame, labels: pl.DataFrame, drop: list[str]) -> dict:
    g = gold.filter(pl.col("label") != "unknown").join(
        labels.select("name", pl.col("label").alias("llm")), on="name", how="left").with_columns(
        pl.col("llm").fill_null("unknown"))
    truth = pl.col("label").is_in(drop)
    pred = pl.col("llm").is_in(drop)
    tp = g.filter(truth & pred).height
    return {
        "gold_rows": g.height,
        "exact_label_acc": round(g.filter(pl.col("label") == pl.col("llm")).height / g.height, 3),
        "llm_unknown": g.filter(pl.col("llm") == "unknown").height,
        "drop_precision": round(tp / max(1, g.filter(pred).height), 3),   # of what it dropped, how much was junk
        "drop_recall": round(tp / max(1, g.filter(truth).height), 3),     # of the junk, how much it dropped
        "wrongly_dropped_companies": g.filter((pl.col("label") == "company") & pred)["name"].to_list(),
        "baseline_acc": round(g.filter(pl.col("label") == "company").height / g.height, 3),  # rules: all "company"
    }


def top_precision(acc: pl.DataFrame, gold: pl.DataFrame) -> dict:
    top = acc.filter(pl.col("status") == "ranked").head(TOP).join(
        gold.select("name", "label"), on="name", how="left").with_columns(pl.col("label").fill_null("not in gold"))
    counts = dict(top["label"].value_counts().rows())
    return {"company": counts.get("company", 0), **{k: v for k, v in counts.items() if k != "company"},
            "names": top["name"].to_list()}


def main() -> None:
    llm = load_config("llm")
    gold = pl.read_csv(ROOT / "eval" / "gold_orgs.csv")
    labels = pl.read_parquet(ROOT / llm["paths"]["labels"])
    drop = llm["drop_labels"]
    unreviewed = gold.filter(pl.col("reviewed") != "yes").height
    if unreviewed:
        print(f"WARNING: {unreviewed}/{gold.height} gold rows not reviewed by a human yet -> numbers are provisional\n")

    q = label_quality(gold, labels, drop)
    print("1) label quality on gold:", {k: v for k, v in q.items() if k != "wrongly_dropped_companies"})
    print("   companies the LLM would drop:", q["wrongly_dropped_companies"])

    con, cfg = connect(), load_config("scoring")
    src = sql_str(str(ROOT / load_config("pipeline")["paths"]["findings"] / "*.parquet"))
    dropped = labels.filter(pl.col("label").is_in(drop))["name"].to_list()
    base, withllm = top_precision(score(con, src, cfg), gold), top_precision(score(con, src, cfg, dropped), gold)
    print(f"\n2) top-{TOP} ranked accounts by gold label")
    print("   rules only :", {k: v for k, v in base.items() if k != "names"})
    print("   rules + LLM:", {k: v for k, v in withllm.items() if k != "names"})
    print("   rules + LLM top names:", withllm["names"])


if __name__ == "__main__":
    main()
