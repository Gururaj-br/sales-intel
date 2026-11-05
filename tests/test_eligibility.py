"""eligibility.py rules on tiny hand-made rows (test fixtures, not real data)."""
import copy
import sys
from pathlib import Path

import polars as pl
import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "pipeline"))
from common import connect, load_config  # noqa: E402
from eligibility import label_part  # noqa: E402

# name -> (org, tags, cloud_provider, cloud_service, domains, expected status, expected reason)
CASES = {
    "honeypot_wins_over_cloud": ("Aliyun Computing", ["honeypot", "cloud"], "Alibaba Cloud", None, [], "excluded", "honeypot"),
    "cdn_tag":                  ("Some Hoster", ["cdn"], None, None, [], "excluded", "cdn_waf"),
    "cdn_org_without_tag":      ("Incapsula Inc", None, None, None, [], "excluded", "cdn_waf"),
    "cloudfront_service":       ("Amazon.com, Inc.", ["cloud"], "Amazon", "CLOUDFRONT", [], "excluded", "cdn_waf"),
    "isp":                      ("Comcast Cable Communications, LLC", None, None, None, [], "excluded", "isp"),
    "security_vendor_org":      ("Palo Alto Networks, Inc", None, None, None, [], "excluded", "security_vendor"),
    "security_vendor_domain":   ("Some Org", None, None, None, ["cisco.com"], "excluded", "security_vendor"),
    "not_cisco":                ("City Office", None, None, None, ["sanfrancisco.gov"], "eligible", None),
    "cloud_tag":                ("Walmart Inc", ["cloud"], "Google", None, ["walmart.com"], "needs_attribution", "cloud_hosting"),
    "hosting_org_no_tag":       ("Hetzner Online GmbH", None, None, None, [], "needs_attribution", "cloud_hosting"),
    "plain_company":            ("ServiceNow, Inc.", ["self-signed"], None, None, ["service-now.com"], "eligible", None),
    "null_org":                 (None, None, None, None, [], "eligible", None),
    "unclassified_exact":       ("ACE", None, None, None, [], "needs_attribution", "unclassified_org"),
    "hosting_word":             ("HostGator.com LLC", None, None, None, [], "needs_attribution", "cloud_hosting"),
    "hosting_word_servers":     ("Servers.com, Inc.", None, None, None, [], "needs_attribution", "cloud_hosting"),
    "isp_word":                 ("Aussie Broadband", None, None, None, [], "excluded", "isp"),
    "not_colo_colombia":        ("Bancolombia S.A.", None, None, None, [], "eligible", None),
    "not_cloud_saas":           ("Cloudera, Inc.", None, None, None, [], "eligible", None),
    "not_ace_substring":       ("Ace Hardware Corp", None, None, None, [], "eligible", None),
}


def run(tmp_path, cfg):
    rows = [dict(case=name, part="t", ip_str=f"10.0.0.{i}", port=443, transport="tcp", org=o,
                 tags=t, cloud_provider=cp, cloud_service=cs, domains=d)
            for i, (name, (o, t, cp, cs, d, _, _)) in enumerate(CASES.items())]
    src, dst = tmp_path / "in.parquet", tmp_path / "out.parquet"
    pl.DataFrame(rows, schema_overrides={"tags": pl.List(pl.Utf8), "domains": pl.List(pl.Utf8)}).write_parquet(src)
    label_part(connect(), src, dst, cfg)
    out = pl.read_parquet(dst)
    return {r["case"]: (r["status"], r["reason"]) for r in out.iter_rows(named=True)}


@pytest.fixture
def cfg():
    return load_config("exclusions")


@pytest.mark.parametrize("name", list(CASES))
def test_rules(tmp_path, cfg, name):
    got = run(tmp_path, cfg)
    *_, status, reason = CASES[name]
    assert got[name] == (status, reason)


def test_security_switch_off(tmp_path, cfg):
    off = copy.deepcopy(cfg)
    off["security_vendors"]["enabled"] = False
    got = run(tmp_path, off)
    assert got["security_vendor_org"] == ("eligible", None)
    assert got["security_vendor_domain"] == ("eligible", None)
