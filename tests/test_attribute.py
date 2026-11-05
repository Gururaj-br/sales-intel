"""attribute.py rules on tiny hand-made rows (test fixtures, not real data)."""
import copy
import sys
from pathlib import Path

import polars as pl
import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "pipeline"))
from attribute import attribute_part  # noqa: E402
from common import connect, load_config  # noqa: E402

NA, EL, EX = "needs_attribution", "eligible", "excluded"
# name -> (status, org, cert_o, cert_cn, ip, hostnames, domains, expected tier, expected owner_key, expected note)
CASES = {
    "a_cert":            (EL, "Some Hoster", "Walmart Inc.", "www.walmart.com", "10.0.0.1", [], [], "A", "walmart", None),
    "a_beats_domain":    (NA, "Google LLC", "ServiceNow, Inc.", None, "10.0.0.2", ["x.service-now.com"], ["service-now.com"], "A", "servicenow", None),
    "a_factory_cert":    (NA, "Amazon", "Fortinet Ltd.", "support", "10.0.0.3", [], [], "D", None, "no_proof"),
    "a_placeholder":     (EL, "National Grid plc", "NA", None, "10.0.0.4", [], [], "C", "national grid", None),
    "a_infra_cert":      (NA, "Amazon", "CloudFlare, Inc.", None, "10.0.0.5", [], [], "D", None, "no_proof"),
    "a_default_cn":      (NA, "Amazon", "Walmart Inc.", "localhost", "10.0.0.6", [], [], "D", None, "no_proof"),
    "a_security_vendor": (EL, "Some Org", "Zscaler, Inc.", None, "10.0.0.7", [], [], "D", None, "security_vendor"),
    "b_domain":          (NA, "Google LLC", None, None, "10.0.0.8", ["shop.walmart.com"], ["walmart.com"], "B", "walmart.com", None),
    "b_reverse_dns":     (NA, "Hetzner", None, None, "23.88.82.152", ["static.152.82.88.23.clients.example.de"], ["example.de"], "D", None, "no_proof"),
    "rdns_hex":          (NA, "Altice", None, None, "47.21.62.18", ["ool-2f153e12.static.example.net"], ["example.net"], "D", None, "no_proof"),
    "rdns_hex_only":     (NA, "Telenet", None, None, "81.82.243.22", ["d5152f316.example.be"], ["example.be"], "D", None, "no_proof"),
    "rdns_zero_padded":  (NA, "Vodafone", None, None, "109.90.116.66", ["ip-109-090-116-066.um36.example.de"], ["example.de"], "D", None, "no_proof"),
    "rdns_three_parts":  (NA, "Speakeasy", None, None, "64.81.224.136", ["dsl081-224-136.chi1.example.net"], ["example.net"], "D", None, "no_proof"),
    "rdns_isp_word":     (NA, "Starlink", None, None, "153.66.200.232", ["customer.chcoilx1.isp.example.com"], ["example.com"], "D", None, "no_proof"),
    "not_rdns_number":   (NA, "Google LLC", None, None, "34.120.5.17", ["web17.prod-2.acmewidgets.com"], ["acmewidgets.com"], "B", "acmewidgets.com", None),
    "a_placeholder_norm": (NA, "Amazon", "My Company Ltd", None, "10.0.0.20", [], [], "D", None, "no_proof"),
    "b_infra_domain":   (NA, "Google LLC", None, None, "10.0.0.9", ["9.0.0.10.bc.googleusercontent.com"], ["googleusercontent.com"], "D", None, "no_proof"),
    "b_hoster_domain":   (NA, "Linode", None, None, "10.0.0.10", ["li1.members.linode.com"], ["linode.com"], "D", None, "no_proof"),
    "b_multiple":        (NA, "Google LLC", None, None, "10.0.0.11", ["a.one.com", "b.two.com"], ["one.com", "two.com"], "D", None, "multiple_domains"),
    "eligible_multiple": (EL, "Acme Widgets Inc", None, None, "10.0.0.12", ["a.one.com", "b.two.com"], ["one.com", "two.com"], "C", "acme widgets", None),
    "c_org":             (EL, "ServiceNow, Inc.", None, None, "10.0.0.13", [], [], "C", "servicenow", None),
    "eligible_no_org":   (EL, None, None, None, "10.0.0.14", [], [], "D", None, "no_org"),
    "na_no_proof":       (NA, "ACE", None, None, "10.0.0.15", [], [], "D", None, "no_proof"),
    # agreement cases (expected weights in AGREE below)
    "agree_domain_org":  (EL, "ServiceNow, Inc.", None, None, "10.0.0.30", ["x.service-now.com"], ["service-now.com"], "B", "servicenow", None),
    "agree_first_word":  (EL, "Ford Motor Company", None, None, "10.0.0.31", ["www.ford.com"], ["ford.com"], "B", "ford motor company", None),
    "agree_cert_org":    (EL, "SAP SE", "SAP SE", None, "10.0.0.32", [], [], "A", "sap se", None),
    "no_agree_short":    (EL, "Ed Group", None, None, "10.0.0.33", ["www.ed.ac.uk"], ["ed.ac.uk"], "B", "ed.ac.uk", None),
    "no_agree_mismatch": (EL, "Acme Widgets Inc", None, None, "10.0.0.34", ["shop.walmart.com"], ["walmart.com"], "B", "walmart.com", None),
    "no_agree_landlord": (NA, "Acmecloud", None, None, "10.0.0.35", ["www.acmecloud.com"], ["acmecloud.com"], "B", "acmecloud.com", None),
    "no_agree_first_word_names": (EL, "University of Florida", "University of Wisconsin", None, "10.0.0.36", [], [], "A", "university of wisconsin", None),
    "excluded_keeps_d": (EX, "Cloudflare", "Walmart Inc.", None, "10.0.0.16", ["shop.walmart.com"], ["walmart.com"], "D", None, "excluded"),
}


def run(tmp_path, cfg):
    rows = [dict(case=n, status=s, reason=None if s == EL else "x", org=o, cert_o=co, cert_cn=cn,
                 ip_str=ip, is_ipv6=False, hostnames=h, domains=d)
            for n, (s, o, co, cn, ip, h, d, *_) in CASES.items()]
    src, dst = tmp_path / "in.parquet", tmp_path / "out.parquet"
    pl.DataFrame(rows, schema_overrides={"hostnames": pl.List(pl.Utf8), "domains": pl.List(pl.Utf8)}).write_parquet(src)
    attribute_part(connect(), src, dst, cfg, load_config("scoring")["attribution_weight"])
    out = pl.read_parquet(dst)
    return {r["case"]: r for r in out.iter_rows(named=True)}


@pytest.fixture
def cfg():
    return load_config("exclusions")


@pytest.mark.parametrize("name", list(CASES))
def test_rules(tmp_path, cfg, name):
    got = run(tmp_path, cfg)[name]
    *_, tier, key, note = CASES[name]
    assert (got["tier"], got["owner_key"], got["attr_note"]) == (tier, key, note)


def test_weight_follows_tier(tmp_path, cfg):
    w = load_config("scoring")["attribution_weight"]
    got = run(tmp_path, cfg)
    assert got["a_cert"]["attr_weight"] == w["A_cert"]
    assert got["b_domain"]["attr_weight"] == w["B_hostname"]
    assert got["c_org"]["attr_weight"] == w["C_org"]
    assert got["na_no_proof"]["attr_weight"] is None


# name -> agreement expected? (weight = tier weight + bonus if agreed, capped at 1)
AGREE = {
    "agree_domain_org": True, "agree_first_word": True, "agree_cert_org": True,
    "a_beats_domain": True,   # cert ServiceNow + service-now.com on a cloud row
    "no_agree_short": False, "no_agree_mismatch": False, "no_agree_landlord": False,
    "no_agree_first_word_names": False, "c_org": False, "b_domain": False,
}


@pytest.mark.parametrize("name", list(AGREE))
def test_agreement(tmp_path, cfg, name):
    s = load_config("scoring")["attribution_weight"]
    base = {"A": s["A_cert"], "B": s["B_hostname"], "C": s["C_org"]}
    got = run(tmp_path, cfg)[name]
    want = min(1.0, base[got["tier"]] + (s["agreement"]["bonus"] if AGREE[name] else 0))
    assert got["attr_agree"] is AGREE[name]
    assert got["attr_weight"] == pytest.approx(want)


def test_agreed_domain_takes_org_name(tmp_path, cfg):
    got = run(tmp_path, cfg)["agree_domain_org"]
    assert (got["owner_name"], got["b_cand"]) == ("ServiceNow, Inc.", "service-now.com")


def test_multiple_domains_switch_off(tmp_path, cfg):
    off = copy.deepcopy(cfg)
    off["attribution"]["abstain_on_multiple_domains"] = False
    got = run(tmp_path, off)["b_multiple"]
    assert (got["tier"], got["owner_key"]) == ("B", "one.com")
