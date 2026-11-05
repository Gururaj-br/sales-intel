"""Orchestrator: layout + meta checks, S3 backend on a fake client, and land -> split -> process -> score locally."""
import io
import json
import sys
from pathlib import Path

import polars as pl
import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
from orchestrator.storage import (S3Storage, bronze_key, done_key, gold_key, landing_key,  # noqa: E402
                                  make_partition, partition_from_landing_key, parts_of, silver_key, split_key)

SAMPLE = ROOT / "data/sample/s2k.jsonl"     # 2,000 real records (git-ignored)
P = make_partition({"dt": "2026-11-01", "source": "shodan", "file": "sample_assignment.json.zst"}, use_hour=False)


class FakeS3:
    """Just enough of boto3's S3 client, backed by a dict (no network, no AWS account)."""
    def __init__(self):
        self.objects = {}

    def list_objects_v2(self, Bucket, Prefix, MaxKeys=1000):
        keys = sorted(k for (b, k) in self.objects if b == Bucket and k.startswith(Prefix))[:MaxKeys]
        return {"Contents": [{"Key": k} for k in keys]} if keys else {}

    def get_paginator(self, name):
        fake = self
        class P:
            def paginate(self, Bucket, Prefix):
                yield fake.list_objects_v2(Bucket, Prefix)
        return P()

    def upload_file(self, filename, bucket, key):
        self.objects[(bucket, key)] = Path(filename).read_bytes()

    def download_file(self, bucket, key, filename):
        Path(filename).write_bytes(self.objects[(bucket, key)])

    def get_object(self, Bucket, Key):
        return {"Body": io.BytesIO(self.objects[(Bucket, Key)])}

    def put_object(self, Bucket, Key, Body):
        self.objects[(Bucket, Key)] = Body


def test_layout_keys():
    assert landing_key(P) == "landing/dt=2026-11-01/source=shodan/sample_assignment.json.zst"
    assert bronze_key(P, 26) == "bronze/dt=2026-11-01/source=shodan/file=sample_assignment/part-00026.json.zst"
    assert silver_key("findings", P, 0) == "silver/findings/dt=2026-11-01/source=shodan/file=sample_assignment/part-00000.parquet"
    assert split_key(P) == "manifest/dt=2026-11-01/source=shodan/file=sample_assignment/_split.json"
    assert done_key(P, 3) == "manifest/dt=2026-11-01/source=shodan/file=sample_assignment/part-00003.done.json"
    assert gold_key("accounts.parquet", "2026-11-05") == "gold/dt=2026-11-05/accounts.parquet"
    assert gold_key("accounts.parquet") == "gold/latest/accounts.parquet"
    ph = make_partition({"dt": "2026-11-01", "source": "Shodan", "file": "x.json.zst", "hr": "7"}, use_hour=True)
    assert landing_key(ph) == "landing/dt=2026-11-01/source=shodan/hr=07/x.json.zst"     # lowercased, padded
    assert partition_from_landing_key(landing_key(ph)) == ph


def test_meta_is_checked():
    with pytest.raises(ValueError):                       # bad date
        make_partition({"dt": "2026-13-01", "source": "shodan", "file": "a"}, False)
    with pytest.raises(ValueError):                       # hour given but hours are off: mixed depths
        make_partition({"dt": "2026-11-01", "source": "shodan", "file": "a", "hr": "01"}, False)
    with pytest.raises(ValueError):                       # hours on but no hour
        make_partition({"dt": "2026-11-01", "source": "shodan", "file": "a"}, True)
    with pytest.raises(ValueError):                       # missing source
        make_partition({"dt": "2026-11-01", "file": "a"}, False)


def test_s3_storage_round_trip(tmp_path):
    st = S3Storage("bkt", "", client=FakeS3())            # bucket root, same keys as local
    f = tmp_path / "x.parquet"
    f.write_bytes(b"data")
    st.put(f, silver_key("findings", P, 3))
    st.write_json(split_key(P), {"parts": 1})
    assert st.exists(silver_key("findings", P, 3)) and not st.exists(silver_key("findings", P, 4))
    assert st.list("silver/", ".parquet") == [silver_key("findings", P, 3)]
    assert st.get(silver_key("findings", P, 3), tmp_path / "dl").read_bytes() == b"data"
    assert (tmp_path / "dl" / silver_key("findings", P, 3)).exists()     # full key path kept: no name clashes
    assert st.read_json(split_key(P)) == {"parts": 1}
    assert st.uri == "s3://bkt" and S3Storage("bkt", "lake", client=FakeS3()).uri == "s3://bkt/lake"


@pytest.mark.skipif(not SAMPLE.exists(), reason="needs data/sample/s2k.jsonl")
def test_land_split_process_score_local(tmp_path, monkeypatch):
    zstd = pytest.importorskip("zstandard")
    from orchestrator.process import process_files, score_and_publish
    from orchestrator.split import split_file
    from orchestrator.storage import LocalStorage

    # landed file = two zstd frames back to back (like `cat a.zst b.zst`): must read as one stream
    lines = SAMPLE.read_bytes().splitlines(keepends=True)
    src = tmp_path / "sample_assignment.json.zst"
    src.write_bytes(zstd.ZstdCompressor().compress(b"".join(lines[:1200]))
                    + zstd.ZstdCompressor().compress(b"".join(lines[1200:])))

    ocfg = {"storage": "local", "local": {"root": str(tmp_path / "lake")}, "layout": {"use_hour": False},
            "split": {"records_per_part": 700, "zstd_level": 3},
            "process": {"workers": 1, "duckdb_memory_per_worker": "1GB", "duckdb_threads_per_worker": 2,
                        "work_dir": str(tmp_path / "work")},
            "llm_step": False, "publish_to": str(tmp_path / "out")}
    monkeypatch.setattr("orchestrator.process.write_summaries", lambda rows: None)   # keep logs/ untouched
    st = LocalStorage(ocfg["local"]["root"])
    st.put(src, landing_key(P))                                        # = `land`

    meta = split_file(st, P, 700, work_dir=tmp_path / "work")
    assert meta["records"] == 2000 and meta["parts"] == 3              # 700 + 700 + 600
    assert meta["bytes"] == src.stat().st_size and len(meta["sha256"]) == 64
    assert list(parts_of(st, P)) == [0, 1, 2]
    with zstd.ZstdDecompressor().stream_reader(open(st.root / bronze_key(P, 2), "rb")) as r:
        assert r.read().count(b"\n") == 600                            # records copied, not changed
    assert (st.root / landing_key(P)).read_bytes() == src.read_bytes()  # landing untouched

    rows = process_files(ocfg, [P])
    assert sorted(r["part"] for r in rows) == [0, 1, 2]
    assert sum(r["rows_raw"] for r in rows) == 2000
    for stage in ("observations", "eligibility", "attribution", "findings"):
        assert (st.root / silver_key(stage, P, 1)).exists()
    obs = pl.read_parquet(st.root / silver_key("observations", P, 1))
    assert obs["part"].unique().to_list() == ["2026-11-01_shodan_sample_assignment_part-00001"]   # unique id
    assert all(r.get("skipped") for r in process_files(ocfg, [P]))      # rerun skips done parts

    acc = score_and_publish(ocfg, llm=False, run_dt="2026-11-05")
    for key in ("gold/dt=2026-11-05/accounts.parquet", "gold/latest/accounts.parquet"):
        assert (st.root / key).exists()
    assert acc.height == pl.read_parquet(tmp_path / "out" / "accounts.parquet").height
    assert json.loads((st.root / split_key(P)).read_text())["parts"] == 3
