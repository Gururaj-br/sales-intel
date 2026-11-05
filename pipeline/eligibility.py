"""eligibility.py: label every observation as excluded / needs_attribution / eligible.

Rules come from config/exclusions.yaml. First matching rule wins:
  1. tag honeypot                                   -> excluded, honeypot
  2. tag cdn, CDN/WAF org name, CDN cloud service   -> excluded, cdn_waf
  3. ISP org name                                   -> excluded, isp
  4. security vendor in org or domains             -> excluded, security_vendor
  5. tag cloud, any cloud provider, hosting org     -> needs_attribution, cloud_hosting
  6. org on the unclassified list (exact name)      -> needs_attribution, unclassified_org
  7. anything else                                  -> eligible
Rows are labelled, never deleted (excluded rows feed later signals).

Always recomputes every part: it takes seconds, and labels must follow config edits.

Run by orchestrator/process.py, one part file at a time (python -m orchestrator ...).
"""
import sys
from pathlib import Path

import pandera.polars as pa
import polars as pl

sys.path.insert(0, str(Path(__file__).resolve().parent))
from common import match_sql, sql_str  # noqa: E402

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
    security = f"({match_sql('org', sec['names'])} OR {any_domain(sec['names'])})"
    return f"""
      CASE
        WHEN {honeypot} THEN ['excluded', 'honeypot']
        WHEN {cdn_tags} OR {match_sql('org', hard['cdn_waf_orgs'])}
             OR upper(cloud_service) IN ({edge}) THEN ['excluded', 'cdn_waf']
        WHEN {match_sql('org', hard['isp_orgs'])} THEN ['excluded', 'isp']
        WHEN {security} THEN ['excluded', 'security_vendor']
        WHEN {has_tag(soft['tags'])} OR cloud_provider IS NOT NULL
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
    df.write_parquet(dst, compression="zstd")
    return df.group_by("part", "status", "reason").len("rows")
