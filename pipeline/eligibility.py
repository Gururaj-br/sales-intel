"""eligibility.py: label every observation as excluded / needs_attribution / eligible.

Rules come from config/exclusions.yaml. First matching rule wins:
  1. tag honeypot                                   -> excluded, honeypot
  2. tag cdn, CDN/WAF org name, CDN cloud service   -> excluded, cdn_waf
  3. ISP org name                                   -> excluded, isp
  4. security vendor in org or domains (if enabled) -> excluded, security_vendor
  5. tag cloud, any cloud provider, hosting org     -> needs_attribution, cloud_hosting
  6. org on the unclassified list (exact name)      -> needs_attribution, unclassified_org
  7. anything else                                  -> eligible
Rows are labelled, never deleted (excluded rows feed later signals).

Always recomputes every part: it takes seconds, and labels must follow config edits.

Usage (from repo root):
  python pipeline/eligibility.py                  # all parts in data/parquet/observations
  python pipeline/eligibility.py --in-dir data/sample/test_obs --out-dir data/sample/test_elig
"""
import argparse
import sys
import time
from pathlib import Path

import pandera.polars as pa
import polars as pl

sys.path.insert(0, str(Path(__file__).resolve().parent))
from common import ROOT, connect, load_config, match_sql, sql_str  # noqa: E402

STATUSES = ["excluded", "needs_attribution", "eligible"]
REASONS = ["honeypot", "cdn_waf", "isp", "security_vendor", "cloud_hosting", "unclassified_org"]

SCHEMA = pa.DataFrameSchema(
    {
        "status": pa.Column(str, pa.Check.isin(STATUSES), nullable=False),
        "reason": pa.Column(str, pa.Check.isin(REASONS), nullable=True),
    },
    checks=[
        # excluded and needs_attribution must say why; eligible must not have a reason
        pa.Check(lambda d: d.lazyframe.select(
            ((pl.col("status") == "eligible") == pl.col("reason").is_null()).all()),
            error="reason must be set exactly when status != eligible"),
    ],
    strict=False,
)


def has_tag(tags: list[str]) -> str:
    """SQL: row has any of these Shodan tags."""
    if not tags:
        return "FALSE"
    return f"list_has_any(coalesce(tags, []), [{', '.join(sql_str(t) for t in tags)}])"


def any_domain(terms: list[str]) -> str:
    """SQL: any hostname domain matches a term."""
    if not terms:
        return "FALSE"
    inner = match_sql("d", terms)
    return f"len(list_filter(coalesce(domains, []), lambda d: {inner})) > 0"


def exact_org(names: list[str]) -> str:
    """SQL: org equals one of these names (case and outer spaces ignored)."""
    if not names:
        return "FALSE"
    return f"coalesce(lower(trim(org)) IN ({', '.join(sql_str(n.lower().strip()) for n in names)}), FALSE)"


def label_sql(cfg: dict) -> str:
    """Build the CASE expression from exclusions.yaml (first match wins)."""
    hard, soft, sec = cfg["hard_exclude"], cfg["needs_attribution"], cfg["security_vendors"]
    honeypot = has_tag([t for t in hard["tags"] if t == "honeypot"])
    cdn_tags = has_tag([t for t in hard["tags"] if t != "honeypot"])
    edge = ", ".join(sql_str(s) for s in hard["cloud_edge_services"]) or "NULL"
    security = (f"({match_sql('org', sec['names'])} OR {any_domain(sec['names'])})"
                if sec["enabled"] else "FALSE")
    cloud_provider = "cloud_provider IS NOT NULL" if soft["use_cloud_provider"] else "FALSE"
    return f"""
      CASE
        WHEN {honeypot} THEN ['excluded', 'honeypot']
        WHEN {cdn_tags} OR {match_sql('org', hard['cdn_waf_orgs'])}
             OR upper(cloud_service) IN ({edge}) THEN ['excluded', 'cdn_waf']
        WHEN {match_sql('org', hard['isp_orgs'])} THEN ['excluded', 'isp']
        WHEN {security} THEN ['excluded', 'security_vendor']
        WHEN {has_tag(soft['tags'])} OR {cloud_provider}
             OR {match_sql('org', soft['hosting_orgs'])} THEN ['needs_attribution', 'cloud_hosting']
        WHEN {exact_org(soft.get('unclassified_orgs', []))} THEN ['needs_attribution', 'unclassified_org']
        ELSE ['eligible', NULL]
      END"""


def label_part(con, src: Path, dst: Path, cfg: dict) -> pl.DataFrame:
    """Label one observations file; returns counts per status/reason."""
    df = con.sql(f"""
        SELECT * EXCLUDE (lbl), lbl[1] AS status, lbl[2] AS reason
        FROM (SELECT *, {label_sql(cfg)} AS lbl FROM read_parquet({sql_str(str(src))}))
    """).pl()
    SCHEMA.validate(df)
    tmp = dst.with_suffix(".parquet.tmp")
    df.write_parquet(tmp, compression="zstd")
    tmp.replace(dst)
    return df.group_by("part", "status", "reason").len("rows")


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--in-dir", type=Path, help="observations folder (default from pipeline.yaml)")
    ap.add_argument("--out-dir", type=Path, help="output folder (default from pipeline.yaml)")
    args = ap.parse_args()

    paths = load_config("pipeline")["paths"]
    in_dir = args.in_dir or ROOT / paths["observations"]
    out_dir = args.out_dir or ROOT / paths["eligibility"]
    out_dir.mkdir(parents=True, exist_ok=True)
    cfg = load_config("exclusions")

    con = connect()
    counts = []
    for src in sorted(in_dir.glob("*.parquet")):
        t0 = time.time()
        counts.append(label_part(con, src, out_dir / src.name, cfg))
        print(f"label {src.name} in {time.time() - t0:.1f}s", flush=True)
    if not counts:
        sys.exit(f"no parquet files in {in_dir}")

    summary = (pl.concat(counts).group_by("status", "reason").agg(pl.col("rows").sum())
               .with_columns((pl.col("rows") / pl.col("rows").sum() * 100).round(1).alias("pct"))
               .sort("rows", descending=True))
    print(summary)
    log = ROOT / "logs" / "eligibility_summary.csv"
    summary.write_csv(log)
    print(f"summary -> {log.relative_to(ROOT)}")


if __name__ == "__main__":
    main()
