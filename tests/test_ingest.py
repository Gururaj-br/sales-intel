"""ingest.py on the 2,000-row sample (data/sample/s2k.jsonl = first 2,000 lines of part-00)."""
import sys
from pathlib import Path

import polars as pl
import pytest

ROOT = Path(__file__).resolve().parent.parent
SAMPLE = ROOT / "data/sample/s2k.jsonl"
sys.path.insert(0, str(ROOT / "pipeline"))
from common import connect, load_config  # noqa: E402
from ingest import ingest_part  # noqa: E402


@pytest.fixture(scope="module")
def obs(tmp_path_factory):
    if not SAMPLE.exists():
        pytest.skip("sample missing: zstd -dc data/raw/part-00.json.zst | head -n 2000 > data/sample/s2k.jsonl")
    out = tmp_path_factory.mktemp("obs")
    stats = ingest_part(connect(), SAMPLE, out, load_config("pipeline"))
    return pl.read_parquet(out / "s2k.parquet"), stats


def test_no_row_lost_except_duplicates(obs):
    df, m = obs
    assert m["rows_raw"] == 2000
    assert m["rows_rejected"] == 0  # typed read must not silently drop records
    assert m["rows_in"] == 2000
    assert m["rows_out"] + m["dupes_dropped"] + m["rows_no_ip"] == m["rows_in"]
    assert m["rows_no_ip"] == 0  # IPv6 rows used to vanish here


def test_ipv6_rows_kept(obs):
    df, _ = obs
    assert df["is_ipv6"].sum() == 24
    assert df.filter(pl.col("is_ipv6"))["ip_str"].str.contains(":").all()


def test_one_row_per_ip_port_transport(obs):
    df, _ = obs
    assert df.select("ip_str", "port", "transport").is_duplicated().sum() == 0


def test_nested_fields_extracted(obs):
    df, _ = obs
    assert df["cve_max_cvss"].drop_nulls().len() > 0
    assert df["cert_o"].drop_nulls().len() > 0
    assert df["country"].null_count() == 0
