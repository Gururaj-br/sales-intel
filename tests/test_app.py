"""app/app.py runs on the real data/out files: ranked view, abstain view, abstained detail page."""
from pathlib import Path

import pytest
from streamlit.testing.v1 import AppTest

ROOT = Path(__file__).resolve().parent.parent
APP = str(ROOT / "app" / "app.py")
pytestmark = pytest.mark.skipif(not (ROOT / "data/out/accounts.parquet").exists(), reason="run score.py first")


def test_account_dropdown_keeps_choice():
    """Bug 2026-11-05: picking an account snapped back to the first one (unstable option order)."""
    import polars as pl
    acc = pl.read_parquet(ROOT / "data/out/accounts.parquet").filter(pl.col("status") == "ranked")
    order = acc.sort(["score", "owner_key"], descending=[True, False])["owner_key"].to_list()
    at = AppTest.from_file(APP, default_timeout=60).run()
    for key in order[1:4]:
        at.selectbox[0].set_value(key).run()
        assert at.selectbox[0].value == key                                   # choice sticks after rerun
        assert at.metric[0].value == f"#{order.index(key) + 1}"   # detail shows THAT account


def test_app_ranks_and_abstains():
    at = AppTest.from_file(APP, default_timeout=60).run()
    assert not at.exception
    assert "ranked accounts" in at.subheader[0].value
    at.checkbox[0].check().run()                       # show insufficient-evidence accounts
    assert "insufficient evidence" in at.subheader[0].value
    at.slider[0].set_value(1.0).run()                  # nothing reaches 1.0 -> every account abstains
    assert at.subheader[0].value.startswith("0 ranked")
    assert any("Not enough proof" in w.value for w in at.warning)        # detail page shows no score
    assert at.metric[0].value == "none" and at.metric[1].value == "n/a"


def test_every_finding_has_plain_text():
    """Each finding type in findings.yaml has a title/what/why/talk in explain.yaml."""
    import yaml
    cfg = yaml.safe_load((ROOT / "config/findings.yaml").read_text())
    names = ([r["name"] for r in cfg["exposed_ports"]] + [cfg["known_cves"]["name"]]
             + [r["name"] for r in cfg["tag_rules"]] + [r["name"] for r in cfg["module_rules"]])
    explain = yaml.safe_load((ROOT / "config/explain.yaml").read_text())["findings"]
    for n in names:
        assert set(explain[n]) >= {"title", "what", "why", "talk"}, n
