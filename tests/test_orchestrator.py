"""Orchestrator: split -> process -> score on local storage, and the S3 backend on a fake client."""
import io
import json
import sys
from pathlib import Path

import polars as pl
import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
from orchestrator.storage import S3Storage, bronze_key, chunks_of, silver_key  # noqa: E402

SAMPLE = ROOT / "data/sample/s2k.jsonl"     # 2,000 real records (git-ignored)


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


def test_s3_storage_round_trip(tmp_path):
    st = S3Storage("bkt", "lake", client=FakeS3())
    f = tmp_path / "x.parquet"
    f.write_bytes(b"data")
    st.put(f, silver_key("findings", "src1", 3))
    st.write_json("bronze/source=src1/_meta.json", {"chunks": 1})
    assert st.exists("silver/findings/source=src1/chunk=00003/src1_c00003.parquet")
    assert not st.exists("silver/findings/source=src1/chunk=00004/src1_c00004.parquet")
    assert st.list("silver/", ".parquet") == ["silver/findings/source=src1/chunk=00003/src1_c00003.parquet"]
    assert st.get(silver_key("findings", "src1", 3), tmp_path / "dl").read_bytes() == b"data"
    assert st.read_json("bronze/source=src1/_meta.json") == {"chunks": 1}
    assert st.uri == "s3://bkt/lake"


def test_hive_layout():
    assert bronze_key("scan_a", 7) == "bronze/source=scan_a/chunk=00007/scan_a_c00007.json.zst"
    assert silver_key("eligibility", "scan_a", 7) == "silver/eligibility/source=scan_a/chunk=00007/scan_a_c00007.parquet"


@pytest.mark.skipif(not SAMPLE.exists(), reason="needs data/sample/s2k.jsonl")
def test_split_process_score_local(tmp_path, monkeypatch):
    zstd = pytest.importorskip("zstandard")
    from orchestrator.process import process_sources, score_and_publish
    from orchestrator.split import split_file
    from orchestrator.storage import LocalStorage

    # input = two zstd frames back to back (like `cat a.zst b.zst`): must read as one stream
    lines = SAMPLE.read_bytes().splitlines(keepends=True)
    big = tmp_path / "big.json.zst"
    big.write_bytes(zstd.ZstdCompressor().compress(b"".join(lines[:1200]))
                    + zstd.ZstdCompressor().compress(b"".join(lines[1200:])))

    ocfg = {"storage": "local", "local": {"root": str(tmp_path / "lake")},
            "split": {"records_per_chunk": 700, "zstd_level": 3},
            "process": {"workers": 1, "duckdb_memory_per_worker": "1GB", "duckdb_threads_per_worker": 2,
                        "work_dir": str(tmp_path / "work")},
            "llm_step": False, "publish_to": str(tmp_path / "out")}
    monkeypatch.setattr("orchestrator.process.write_summaries", lambda rows: None)   # keep logs/ untouched
    st = LocalStorage(ocfg["local"]["root"])

    meta = split_file(st, str(big), "t1", 700, work_dir=tmp_path)
    assert meta["records"] == 2000 and meta["chunks"] == 3          # 700 + 700 + 600
    assert list(chunks_of(st, "t1")) == [0, 1, 2]
    with zstd.ZstdDecompressor().stream_reader(open(st.root / bronze_key("t1", 2), "rb")) as r:
        assert r.read().count(b"\n") == 600                            # records copied, not changed

    rows = process_sources(ocfg, ["t1"])
    assert sorted(r["chunk"] for r in rows) == [0, 1, 2]
    assert sum(r["rows_raw"] for r in rows) == 2000
    for stage in ("observations", "eligibility", "attribution", "findings"):
        assert (st.root / silver_key(stage, "t1", 1)).exists()
    assert all(r.get("skipped") for r in process_sources(ocfg, ["t1"]))   # rerun skips done chunks

    acc = score_and_publish(ocfg, llm=False)
    assert (tmp_path / "out" / "accounts.parquet").exists() and (st.root / "gold/accounts.parquet").exists()
    assert acc.height == pl.read_parquet(tmp_path / "out" / "accounts.parquet").height
    assert json.loads((st.root / "bronze/source=t1/_meta.json").read_text())["chunks"] == 3
