"""attribute.py: give every scorable row ONE owner, and say how sure we are.

Tiers, best first (rules and lists in config/exclusions.yaml, weights in config/scoring.yaml):
  A  cert subject O   not a factory/placeholder cert, not a CDN/hoster/ISP name
  B  hostname domain  exactly one real domain left after dropping: infra_domains,
                      CDN/hoster/ISP names, reverse-DNS hostnames (>= 3 IP numbers, IP in
                      hex, or an ISP word label such as static / dsl / cable / customer)
  C  org name         ONLY for status = eligible (for clouds the org is the landlord)
  D  no owner         never scored; attr_note says why
Excluded rows are kept with tier D (later signals use them). A tier-A owner that is a
security vendor (list in exclusions.yaml) also becomes D.

owner_key = normalized name (A, C) or the domain (B); it is the account id downstream.

Agreement (attr_agree): two INDEPENDENT proofs name the same company, and one of them is the
winning proof. Cert O == org; cert O ~ domain; domain ~ org (eligible only: for clouds the org
is the landlord). Adds scoring.yaml agreement.bonus to attr_weight. A tier-B domain that agrees
with the org takes the org's key, so service-now.com rows join the "servicenow" account.

Run by orchestrator/process.py, one part file at a time (python -m orchestrator ...).
"""
import sys
from pathlib import Path

import pandera.polars as pa
import polars as pl

sys.path.insert(0, str(Path(__file__).resolve().parent))
from common import match_sql, sql_str  # noqa: E402

TIERS = ["A", "B", "C", "D"]
NOTES = ["excluded", "security_vendor", "multiple_domains", "no_proof", "no_org"]

SCHEMA = pa.DataFrameSchema(
    {
        "tier": pa.Column(str, pa.Check.isin(TIERS), nullable=False),
        "attr_note": pa.Column(str, pa.Check.isin(NOTES), nullable=True),
        "attr_weight": pa.Column(float, pa.Check.in_range(0, 1), nullable=True),
        "attr_agree": pa.Column(bool, nullable=False),
    },
    checks=[
        pa.Check(lambda d: d.lazyframe.select(
            ((pl.col("tier") == "D") == pl.col("owner_key").is_null()).all()),
            error="owner_key must be set exactly when tier != D"),
        pa.Check(lambda d: d.lazyframe.select(
            ((pl.col("tier") == "D") == pl.col("attr_note").is_not_null()).all()),
            error="attr_note must be set exactly when tier == D"),
        pa.Check(lambda d: d.lazyframe.select(
            ((pl.col("status") == "excluded").not_() | (pl.col("tier") == "D")).all()),
            error="excluded rows must be tier D"),
        pa.Check(lambda d: d.lazyframe.select(
            ((pl.col("status") == "needs_attribution").not_() | (pl.col("tier") != "C")).all()),
            error="needs_attribution rows can never use the org name (tier C)"),
    ],
    strict=False,
)


def in_list(expr: str, values: list[str]) -> str:
    """SQL: lower(trim(expr)) is exactly one of values."""
    if not values:
        return "FALSE"
    return f"coalesce(lower(trim({expr})) IN ({', '.join(sql_str(v.lower().strip()) for v in values)}), FALSE)"


def norm_sql(expr: str, cfg: dict) -> str:
    """SQL: 'ServiceNow, Inc.' -> 'servicenow'. Lowercase, punctuation -> space, drop end suffixes."""
    s = f"regexp_replace(lower({expr}), '[^\\p{{L}}\\p{{N}}]+', ' ', 'g')"
    suffixes = cfg["org_normalize"]["strip_suffixes"]
    if suffixes:
        s = f"regexp_replace(trim({s}), '(\\s+({'|'.join(suffixes)}))+$', '')"
    return f"nullif(trim({s}), '')"


def attribute_sql(cfg: dict) -> str:
    """Build the SELECT that adds tier / owner_name / attr_note to an eligibility file."""
    hard, soft, sec, rules = cfg["hard_exclude"], cfg["needs_attribution"], cfg["security_vendors"], cfg["attribution"]
    infra_terms = hard["cdn_waf_orgs"] + hard["isp_orgs"] + soft["hosting_orgs"]

    def infra_name(col: str) -> str:          # a CDN / hoster / ISP name is never an owner
        return match_sql(col, infra_terms)

    infra_domains = ", ".join(sql_str(d) for d in cfg["infra_domains"]) or "NULL"
    words = ", ".join(sql_str(w.lower()) for w in rules.get("reverse_dns_words", [])) or "NULL"
    reverse_dns = (   # hostname made by a hoster/ISP from the IP: >= 3 IP numbers, IP in hex, or an ISP word
        "(len(list_filter(ip_nums, lambda o: list_contains(list_transform("
        "regexp_extract_all(h, '\\d+'), lambda x: TRY_CAST(x AS INTEGER)), o))) >= 3"
        " OR (ip_hex IS NOT NULL AND contains(lower(h), ip_hex))"
        f" OR list_has_any(string_split_regex(lower(h), '[.-]'), [{words}]))")
    security = match_sql("a_owner", sec["names"])

    return f"""
    WITH ip AS (   -- the IPv4 as 4 numbers and as hex, for the reverse-DNS rule
      SELECT *,
        CASE WHEN NOT coalesce(is_ipv6, FALSE)
             THEN list_transform(string_split(ip_str, '.'), lambda x: TRY_CAST(x AS INTEGER)) END AS ip_nums
      FROM read_parquet(@src@)
    ), c AS (
      SELECT * EXCLUDE (ip_nums, ip_hex),
        CASE WHEN status <> 'excluded' AND nullif(trim(cert_o), '') IS NOT NULL
              AND NOT {match_sql('cert_o', cfg['default_cert_orgs'])}
              AND NOT {in_list(norm_sql('cert_o', cfg), cfg.get('default_cert_orgs_exact', []))}
              AND NOT {in_list('cert_cn', cfg['default_cert_cns'])}
              AND NOT {infra_name('cert_o')}
             THEN trim(cert_o) END AS a_owner,
        list_distinct(list_filter(coalesce(domains, []), lambda d:
            NOT list_contains([{infra_domains}], lower(d))
            AND NOT {infra_name('d')}
            AND len(list_filter(coalesce(hostnames, []), lambda h:
                  (h = d OR ends_with(h, '.' || d)) AND NOT {reverse_dns})) > 0)) AS b_cands
      FROM (SELECT *, CASE WHEN len(ip_nums) = 4
                   THEN printf('%02x%02x%02x%02x', ip_nums[1], ip_nums[2], ip_nums[3], ip_nums[4]) END AS ip_hex
            FROM ip)
    )
    SELECT * EXCLUDE (a_owner, b_cands, lbl), lbl[1] AS tier, lbl[2] AS owner_name, lbl[3] AS attr_note,
      a_owner AS a_cand,                                                    -- every proof we had,
      CASE WHEN len(b_cands) = 1 THEN lower(b_cands[1]) END AS b_cand,     -- kept for the agreement
      CASE WHEN status = 'eligible' THEN nullif(trim(org), '') END AS c_cand  -- check and for the app
    FROM (SELECT *, CASE
        WHEN status = 'excluded'                        THEN ['D', NULL, 'excluded']
        WHEN a_owner IS NOT NULL AND {security}         THEN ['D', NULL, 'security_vendor']
        WHEN a_owner IS NOT NULL                        THEN ['A', a_owner, NULL]
        WHEN len(b_cands) = 1                           THEN ['B', lower(b_cands[1]), NULL]
        WHEN status = 'eligible' AND {norm_sql('org', cfg)} IS NOT NULL THEN ['C', trim(org), NULL]
        WHEN status = 'eligible' AND len(b_cands) = 0   THEN ['D', NULL, 'no_org']
        WHEN len(b_cands) > 1                           THEN ['D', NULL, 'multiple_domains']  -- shared hosting: abstain
        ELSE ['D', NULL, 'no_proof']
      END AS lbl FROM c)
    """


def name_eq_sql(k1: str, k2: str) -> str:
    """SQL: two normalized names are the same company (spaces ignored). Strict on purpose:
    first-word match would say 'university of florida' == 'university of wisconsin'."""
    return f"coalesce(replace({k1}, ' ', '') = replace({k2}, ' ', ''), FALSE)"


def name_dom_sql(k: str, label: str, min_len: int) -> str:
    """SQL: normalized name matches a domain's first label ('servicenow' ~ service-now.com,
    'ford motor company' ~ ford.com). Labels shorter than min_len never match (ed.ac.uk)."""
    return (f"coalesce(length({label}) >= {min_len} AND (replace({k}, ' ', '') = {label}"
            f" OR split_part({k}, ' ', 1) = {label}), FALSE)")


def attribute_part(con, src: Path, dst: Path, cfg: dict, weights: dict) -> pl.DataFrame:
    """Attribute one eligibility file; returns counts per status/tier/note."""
    body = attribute_sql(cfg).replace("@src@", sql_str(str(src)))
    w = {"A": weights["A_cert"], "B": weights["B_hostname"], "C": weights["C_org"]}
    agree = weights.get("agreement", {"bonus": 0.0, "min_label_len": 4})
    m = int(agree["min_label_len"])
    df = con.sql(f"""
        WITH k AS (
          SELECT *, {norm_sql('a_cand', cfg)} AS a_key, {norm_sql('c_cand', cfg)} AS c_key,
                 nullif(regexp_replace(split_part(b_cand, '.', 1), '[^a-z0-9]', '', 'g'), '') AS b_label
          FROM ({body})
        ), g AS (
          -- agreement must involve the WINNING proof (tier A: cert; tier B: domain)
          SELECT *, CASE tier
              WHEN 'A' THEN {name_eq_sql('a_key', 'c_key')} OR {name_dom_sql('a_key', 'b_label', m)}
              WHEN 'B' THEN {name_dom_sql('c_key', 'b_label', m)}
              ELSE FALSE END AS attr_agree
          FROM k
        )
        SELECT * EXCLUDE (a_key, c_key, b_label, owner_name),
          CASE WHEN tier = 'B' AND attr_agree THEN c_cand ELSE owner_name END AS owner_name,
          CASE WHEN tier = 'B' AND attr_agree THEN c_key       -- domain joins the org's account
               WHEN tier = 'B' THEN owner_name
               ELSE {norm_sql('owner_name', cfg)} END AS owner_key,
          -- tier D must stay NULL: DuckDB least() SKIPS NULLs, least(1.0, NULL) = 1.0
          CASE WHEN tier <> 'D' THEN
            least(1.0, CASE tier WHEN 'A' THEN {w['A']} WHEN 'B' THEN {w['B']} WHEN 'C' THEN {w['C']} END
                       + CASE WHEN attr_agree THEN {float(agree['bonus'])} ELSE 0 END) END::DOUBLE AS attr_weight
        FROM g
    """).pl()
    # an owner name that normalizes to nothing ("Inc.") has no usable key -> D
    bad = (pl.col("tier") != "D") & pl.col("owner_key").is_null()
    df = df.with_columns(
        pl.when(bad).then(pl.lit("D")).otherwise(pl.col("tier")).alias("tier"),
        pl.when(bad).then(pl.lit("no_proof")).otherwise(pl.col("attr_note")).alias("attr_note"),
        pl.when(bad).then(None).otherwise(pl.col("attr_weight")).alias("attr_weight"),
        pl.when(bad).then(False).otherwise(pl.col("attr_agree")).alias("attr_agree"),
    )
    SCHEMA.validate(df)
    df.write_parquet(dst, compression="zstd")
    return df.group_by("status", "tier", "attr_note").len("rows")
