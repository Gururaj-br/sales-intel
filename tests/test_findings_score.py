"""findings.py + score.py on tiny hand-made rows (test fixtures, not real data)."""
import math
import sys
from datetime import datetime, timedelta
from pathlib import Path

import polars as pl
import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "pipeline"))
from common import connect, load_config, sql_str  # noqa: E402
from findings import findings_part  # noqa: E402
from score import score  # noqa: E402

T0 = datetime(2026, 11, 1)
BASE = dict(part="t", transport="tcp", ts=T0, tier="A", attr_weight=0.9, owner_name="X",
            product=None, module=None, tags=None, cve_ids=None, cve_max_cvss=None, cve_n_kev=0,
            exchange_name=None, ssl_versions=None, cert_expired=None,
            attr_agree=False, country="BR", b_cand=None)
# case -> (owner_key, ip, port, extra fields)
ROWS = {
    "rdp":        ("acme", "10.0.0.1", 3389, {}),
    "cve_kev":    ("acme", "10.0.0.2", 443, dict(cve_ids=["CVE-1"], cve_max_cvss=9.5, cve_n_kev=1)),
    "cve_plain":  ("beta", "10.0.0.3", 443, dict(cve_ids=["CVE-2"], cve_max_cvss=5.0, cve_n_kev=0)),
    "eol_tag":    ("beta", "10.0.0.4", 80, dict(tags=["eol-product"])),
    "exchange":   ("gamma", "10.0.0.5", 443, dict(exchange_name="Exchange Server 2016 CU23")),
    "exchange_ok": ("gamma", "10.0.0.6", 443, dict(exchange_name="Exchange Server SE")),
    "old_tls":    ("gamma", "10.0.0.7", 443, dict(ssl_versions=["TLSv1", "-SSLv3", "TLSv1.2"])),
    "tls_ok":     ("gamma", "10.0.0.8", 443, dict(ssl_versions=["-TLSv1", "-SSLv3", "TLSv1.2"])),
    "expired":    ("gamma", "10.0.0.9", 443, dict(cert_expired=True)),
    "no_owner":   (None, "10.0.0.10", 3389, dict(tier="D", attr_weight=None)),
    "weak_owner": ("delta", "10.0.0.11", 3389, dict(tier="C", attr_weight=0.5)),
}


def make_findings(tmp_path):
    rows = [{**BASE, "case": c, "owner_key": k, "ip_str": ip, "port": p, **x} for c, (k, ip, p, x) in ROWS.items()]
    src, dst = tmp_path / "attr.parquet", tmp_path / "f.parquet"
    pl.DataFrame(rows, schema_overrides={"tags": pl.List(pl.Utf8), "cve_ids": pl.List(pl.Utf8),
                                         "ssl_versions": pl.List(pl.Utf8), "attr_weight": pl.Float64}).write_parquet(src)
    return findings_part(connect(), src, dst, load_config("findings")), dst


def hits(df, ip):
    return set(df.filter(pl.col("ip_str") == ip)["finding"])


def test_each_rule(tmp_path):
    df, _ = make_findings(tmp_path)
    assert hits(df, "10.0.0.1") == {"rdp"}
    assert hits(df, "10.0.0.2") == {"has_cve"}
    assert hits(df, "10.0.0.4") == {"eol_product"}
    assert hits(df, "10.0.0.5") == {"old_exchange"}
    assert hits(df, "10.0.0.6") == set()          # Exchange SE is supported
    assert hits(df, "10.0.0.7") == {"old_tls"}
    assert hits(df, "10.0.0.8") == set()          # "-TLSv1" means NOT supported
    assert hits(df, "10.0.0.9") == {"expired_cert"}
    assert hits(df, "10.0.0.10") == set()         # tier D: never scored


def test_cve_severity_kev_bonus_capped(tmp_path):
    df, _ = make_findings(tmp_path)
    sev = dict(zip(df["ip_str"], df["severity"]))
    assert sev["10.0.0.2"] == 10.0                # 9.5 + 2 kev bonus, capped at 10
    assert sev["10.0.0.3"] == 5.0


def test_score_and_abstain(tmp_path):
    _, f = make_findings(tmp_path)
    cfg = load_config("scoring")
    acc = {r["owner_key"]: r for r in score(connect(), sql_str(str(f)), cfg).iter_rows(named=True)}
    assert "acme" in acc and None not in acc
    # acme: rdp (sev 9, 1 hit) + has_cve (sev 10, 1 hit), same day -> recency 1
    assert acc["acme"]["score"] == pytest.approx((9 + 10) * math.log2(2))
    # confidence is computed: weight 0.9 * mean evidence of rdp 0.9 and cve 0.6
    assert acc["acme"]["confidence"] == pytest.approx(0.9 * (0.9 + 0.6) / 2)
    assert acc["acme"]["reasons"][0].startswith("has_cve")
    # delta: org-only owner (0.5) * 0.9 evidence = 0.45 < abstain_below
    assert acc["delta"]["status"] == "abstain"
    assert acc["acme"]["status"] == "ranked"


def test_recency_halves_old_findings(tmp_path):
    rows = [{**BASE, "owner_key": k, "ip_str": ip, "port": 3389, "ts": ts}
            for k, ip, ts in [("new", "10.0.1.1", T0), ("old", "10.0.1.2", T0 - timedelta(days=30))]]
    src, dst = tmp_path / "a.parquet", tmp_path / "f.parquet"
    pl.DataFrame(rows, schema_overrides={"tags": pl.List(pl.Utf8), "cve_ids": pl.List(pl.Utf8),
                                         "ssl_versions": pl.List(pl.Utf8)}).write_parquet(src)
    findings_part(connect(), src, dst, load_config("findings"))
    acc = {r["owner_key"]: r["score"] for r in score(connect(), sql_str(str(dst)), load_config("scoring")).iter_rows(named=True)}
    assert acc["old"] == pytest.approx(acc["new"] / 2)   # half_life_days = 30


def test_repeat_sighting_in_another_chunk_counts_once(tmp_path):
    """Same IP + port + finding in two chunks (rescan) = one problem, latest sighting kept."""
    for i, ts in enumerate([T0 - timedelta(days=30), T0]):
        rows = [{**BASE, "owner_key": "acme", "ip_str": "10.0.2.1", "port": 3389, "ts": ts, "part": f"c{i}"}]
        src = tmp_path / f"a{i}.parquet"
        pl.DataFrame(rows, schema_overrides={"tags": pl.List(pl.Utf8), "cve_ids": pl.List(pl.Utf8),
                                             "ssl_versions": pl.List(pl.Utf8)}).write_parquet(src)
        findings_part(connect(), src, tmp_path / f"f{i}.parquet", load_config("findings"))
    acc = score(connect(), sql_str(str(tmp_path / "f*.parquet")), load_config("scoring")).row(0, named=True)
    assert acc["n_findings"] == 1                      # not 2
    assert acc["last_seen"] == T0                      # latest sighting wins
    assert acc["score"] == pytest.approx(9 * 1.0)      # sev 9 * log2(1+1) * recency 1


def test_ties_are_broken_the_same_way_every_run(tmp_path):
    """Two name spellings / countries seen equally often: alphabetically first wins, every time."""
    rows = [{**BASE, "owner_key": "acme", "ip_str": ip, "port": 3389, "owner_name": name, "country": cc}
            for ip, name, cc in [("10.0.3.1", "Acme Ltd.", "TH"), ("10.0.3.2", "Acme Ltd", "JP")]]
    src, dst = tmp_path / "a.parquet", tmp_path / "f.parquet"
    pl.DataFrame(rows, schema_overrides={"tags": pl.List(pl.Utf8), "cve_ids": pl.List(pl.Utf8),
                                         "ssl_versions": pl.List(pl.Utf8)}).write_parquet(src)
    findings_part(connect(), src, dst, load_config("findings"))
    for _ in range(5):
        acc = score(connect(), sql_str(str(dst)), load_config("scoring")).row(0, named=True)
        assert (acc["name"], acc["country"]) == ("Acme Ltd", "JP")
