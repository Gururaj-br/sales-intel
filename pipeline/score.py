"""score.py: roll findings up to ONE row per account (owner_key) -> data/out (gold).

Per account (config/scoring.yaml):
  score      = sum over finding types of  max severity * log2(count + 1) * recency
  recency    = 0.5 ** (age_days / half_life_days), age = newest scan in the data - newest
               sighting of that finding (the data is a snapshot, so "now" = its newest scan)
  confidence = mean attribution weight of its finding rows * mean evidence of its findings
  status     = "abstain" if confidence < abstain_below, else "ranked"
  reasons    = top N finding types by contribution, e.g. "rdp on 3 IPs (sev 9)"
  pitch      = pitch of the top reason
LLM labels (config/llm.yaml, from classify.py): finding rows whose owner name is labelled
infrastructure/default are dropped first. Missing file or use_labels: false = pure rules.

Outputs (the only things the app reads):
  data/out/accounts.parquet          one row per account, ranked first, by score
  data/out/account_findings.parquet  the finding rows behind every account (drill-down)

Run by orchestrator/process.py (python -m orchestrator score).
"""
import sys
from pathlib import Path

import pandera.polars as pa
import polars as pl

sys.path.insert(0, str(Path(__file__).resolve().parent))
from common import ROOT, connect, load_config, sql_str, top_sql  # noqa: E402

SCHEMA = pa.DataFrameSchema(
    {
        "owner_key": pa.Column(str, nullable=False, unique=True),
        "score": pa.Column(float, pa.Check.ge(0), nullable=False),
        "confidence": pa.Column(float, pa.Check.in_range(0, 1), nullable=False),
        "status": pa.Column(str, pa.Check.isin(["ranked", "abstain"]), nullable=False),
    },
    strict=False,
)


def kept_sql(src: str, drop_names: list[str] | None = None) -> str:
    """Finding rows minus owners the LLM labelled infrastructure/default.

    The same IP + port is scanned again in other parts / files: keep only its LATEST sighting
    per finding, so a repeat scan does not count as a second problem.
    """
    drop = (f"WHERE owner_name NOT IN ({', '.join(sql_str(n) for n in drop_names)})" if drop_names else "")
    return f"""SELECT * FROM read_parquet({src}, union_by_name = true) {drop}
        QUALIFY row_number() OVER (PARTITION BY ip_str, port, transport, finding ORDER BY ts DESC) = 1"""


def score_sql(src: str, cfg: dict, drop_names: list[str] | None = None) -> str:
    half = float(cfg["recency"]["half_life_days"])
    top = int(cfg["top_reasons"])
    return f"""
    WITH f AS ({kept_sql(src, drop_names)}),
    newest AS (SELECT max(ts) AS t FROM f),
    per_type AS (      -- one row per account x finding type
      SELECT owner_key, finding, any_value(pitch) AS pitch,
             count(*) AS n, count(DISTINCT ip_str) AS n_ips, max(severity) AS sev,
             date_diff('second', max(ts), (SELECT t FROM newest)) / 86400.0 AS age_days
      FROM f GROUP BY ALL
    ), contrib AS (
      SELECT *, sev * log2(n + 1) * pow(0.5, age_days / {half}) AS points FROM per_type
    ), acc AS (        -- account-level numbers from the raw finding rows
      SELECT owner_key, {top_sql('owner_name')} AS name, {top_sql('tier')} AS main_tier,
             {top_sql('country')} AS country, {top_sql('domain')} AS domain,   -- ties: alphabetical
             avg(attr_weight) AS attr_conf, avg(evidence) AS evidence,
             count(DISTINCT ip_str) AS n_ips, count(*) AS n_findings, max(ts) AS last_seen
      FROM f GROUP BY 1
    )
    SELECT acc.owner_key, acc.name, acc.main_tier, acc.country, acc.domain, acc.n_ips, acc.n_findings, acc.last_seen,
           sum(points) AS score,
           acc.attr_conf * acc.evidence AS confidence,
           list(finding || ' on ' || contrib.n_ips || ' IP' || CASE WHEN contrib.n_ips > 1 THEN 's' ELSE '' END
                || ' (sev ' || round(sev, 1) || ')' ORDER BY points DESC, finding)[1:{top}] AS reasons,
           list(pitch ORDER BY points DESC, finding)[1] AS pitch
    FROM contrib JOIN acc USING (owner_key)
    GROUP BY ALL
    """


def score(con, src: str, cfg: dict, drop_names: list[str] | None = None):
    """drop_names: owner names the LLM labelled infrastructure/default (None = pure rules)."""
    df = con.sql(f"""
        SELECT *, CASE WHEN confidence < {float(cfg['abstain_below'])} THEN 'abstain' ELSE 'ranked' END AS status
        FROM ({score_sql(src, cfg, drop_names)})
        ORDER BY status DESC, score DESC   -- 'ranked' before 'abstain'
    """).pl()
    SCHEMA.validate(df)
    return df


def llm_drop_names() -> list[str] | None:
    """Owner names the LLM labelled infrastructure/default (None = pure rules)."""
    llm = load_config("llm")
    labels_path = ROOT / llm["paths"]["labels"]
    if not (llm["use_labels"] and labels_path.exists()):
        print("LLM labels: not used (pure rules baseline)")
        return None
    lab = pl.read_parquet(labels_path)
    drop = lab.filter(pl.col("label").is_in(llm["drop_labels"]))["name"].to_list()
    print(f"LLM labels: dropping {len(drop)} owner names labelled {llm['drop_labels']}")
    return drop


def run(src: str, out_dir: Path, con=None) -> pl.DataFrame:
    """Score every finding file matched by `src` (SQL string literal / glob) -> out_dir (gold)."""
    out_dir.mkdir(parents=True, exist_ok=True)
    con = con or connect()
    drop = llm_drop_names()
    acc = score(con, src, load_config("scoring"), drop)
    acc.write_parquet(out_dir / "accounts.parquet", compression="zstd")
    con.sql(f"COPY ({kept_sql(src, drop)}) TO {sql_str(str(out_dir / 'account_findings.parquet'))} "
            "(FORMAT parquet, COMPRESSION zstd)")
    print(acc.group_by("status").len("accounts"))
    return acc
