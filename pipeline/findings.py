"""findings.py: turn config/findings.yaml rules into rows: one row per (ip, port, finding).

Only rows with an owner (tier A/B/C from attribute.py) are checked; tier D is never scored.
Rule kinds (all in findings.yaml):
  exposed_ports  port is in the list
  known_cves     row has CVEs; severity = max CVSS (+ kev_bonus if any is known-exploited), max 10
  tag_rules      Shodan tag present
  module_rules   column matches a regex / is TRUE / has any of a list

Run by orchestrator/process.py, one part file at a time (python -m orchestrator ...).
"""
import sys
from pathlib import Path

import pandera.polars as pa

sys.path.insert(0, str(Path(__file__).resolve().parent))
from common import sql_str  # noqa: E402

KEEP = ("part, ip_str, port, transport, ts, owner_key, owner_name, tier, attr_weight, attr_agree, "
        "country, b_cand AS domain")   # country/domain/agreement: shown in the app

SCHEMA = pa.DataFrameSchema(
    {
        "owner_key": pa.Column(str, nullable=False),
        "finding": pa.Column(str, nullable=False),
        "severity": pa.Column(float, pa.Check.in_range(0, 10), nullable=False),
        "evidence": pa.Column(float, pa.Check.in_range(0, 1), nullable=False),
        "attr_weight": pa.Column(float, pa.Check.in_range(0, 1), nullable=False),
    },
    unique=["ip_str", "port", "transport", "finding"],
    strict=False,
)


def rule_selects(cfg: dict) -> list[str]:
    """One SELECT per rule; each returns KEEP + finding, severity, evidence, pitch, detail."""
    def sel(name, sev, ev, pitch, where, detail="NULL"):
        return (f"SELECT {KEEP}, {sql_str(name)} AS finding, ({sev})::DOUBLE AS severity, "
                f"{float(ev)}::DOUBLE AS evidence, {sql_str(pitch)} AS pitch, ({detail})::VARCHAR AS detail "
                f"FROM src WHERE {where}")

    out = []
    for r in cfg["exposed_ports"]:
        out.append(sel(r["name"], r["severity"], r["evidence"], r["pitch"], f"port = {int(r['port'])}",
                       "coalesce(product, module)"))
    c = cfg["known_cves"]
    out.append(sel(c["name"], f"least(10, cve_max_cvss + CASE WHEN cve_n_kev > 0 THEN {float(c['kev_bonus'])} ELSE 0 END)",
                   c["evidence"], c["pitch"], "cve_max_cvss IS NOT NULL",
                   "len(cve_ids) || ' CVEs, ' || cve_n_kev || ' known-exploited'"))
    for r in cfg["tag_rules"]:
        out.append(sel(r["name"], r["severity"], r["evidence"], r["pitch"],
                       f"list_contains(coalesce(tags, []), {sql_str(r['tag'])})", "coalesce(product, module)"))
    for r in cfg["module_rules"]:
        col = r["field"]
        if "match_any" in r:
            where = f"list_has_any(coalesce({col}, []), [{', '.join(sql_str(v) for v in r['match_any'])}])"
        elif r["match"] is True:
            where = f"coalesce({col}, FALSE)"
        else:
            where = f"coalesce(regexp_matches({col}, {sql_str(r['match'])}), FALSE)"
        out.append(sel(r["name"], r["severity"], r["evidence"], r["pitch"], where, f"{col}::VARCHAR"))
    return out


def findings_part(con, src: Path, dst: Path, cfg: dict):
    """Write the findings of one attribution file; returns the polars frame."""
    con.sql(f"CREATE OR REPLACE TEMP VIEW src AS SELECT * FROM read_parquet({sql_str(str(src))}) WHERE tier <> 'D'")
    df = con.sql(" UNION ALL ".join(rule_selects(cfg))).pl()
    SCHEMA.validate(df)
    df.write_parquet(dst, compression="zstd")
    return df
