"""storage.py: one small interface, two backends (local folder or S3). Same paths in both.

Every path is a key relative to the storage root, written with "/" (e.g.
"bronze/source=scan_2026_11/chunk=00000/scan_2026_11_c00000.json.zst").
  LocalStorage  root = a folder (relative to the repo or absolute)
  S3Storage     root = s3://bucket/prefix ; credentials from the normal AWS chain
                (AWS_PROFILE / ~/.aws/credentials / env vars / IAM role). Nothing secret in the repo.

Stages always run on LOCAL files (DuckDB is fastest on local disk): a worker fetches its
chunk, processes it in a scratch folder, and puts the results back.
"""
from __future__ import annotations

import json
import shutil
from pathlib import Path
from typing import BinaryIO, Iterator

ROOT = Path(__file__).resolve().parent.parent


class LocalStorage:
    def __init__(self, root: str):
        self.root = Path(root) if Path(root).is_absolute() else ROOT / root
        self.uri = str(self.root)

    def _p(self, key: str) -> Path:
        return self.root / key

    def exists(self, key: str) -> bool:
        return self._p(key).exists()

    def list(self, prefix: str, suffix: str = "") -> list[str]:
        base = self._p(prefix)
        if not base.exists():
            return []
        return sorted(str(p.relative_to(self.root)).replace("\\", "/")
                      for p in base.rglob(f"*{suffix}") if p.is_file())

    def get(self, key: str, local_dir: Path) -> Path:
        """Local path of `key`. Local storage: the file itself, no copy."""
        return self._p(key)

    def put(self, local: Path, key: str) -> None:
        dst = self._p(key)
        dst.parent.mkdir(parents=True, exist_ok=True)
        tmp = dst.with_name(dst.name + ".tmp")
        shutil.copyfile(local, tmp)
        tmp.replace(dst)           # readers never see a half-written file

    def open_read(self, key_or_path: str) -> BinaryIO:
        p = Path(key_or_path)
        return open(p if p.is_absolute() or p.exists() else self._p(key_or_path), "rb")

    def read_json(self, key: str) -> dict:
        return json.loads(self._p(key).read_text())

    def write_json(self, key: str, obj: dict) -> None:
        dst = self._p(key)
        dst.parent.mkdir(parents=True, exist_ok=True)
        dst.write_text(json.dumps(obj, indent=2, default=str))


class S3Storage:
    def __init__(self, bucket: str, prefix: str = "", region: str | None = None,
                 profile: str | None = None, client=None):
        if client is None:
            import boto3  # only needed when storage: s3
            client = boto3.Session(profile_name=profile or None, region_name=region or None).client("s3")
        self.s3, self.bucket, self.prefix = client, bucket, prefix.strip("/")
        self.uri = f"s3://{bucket}/{self.prefix}"

    def _k(self, key: str) -> str:
        return f"{self.prefix}/{key}" if self.prefix else key

    def exists(self, key: str) -> bool:
        resp = self.s3.list_objects_v2(Bucket=self.bucket, Prefix=self._k(key), MaxKeys=1)
        return any(o["Key"] == self._k(key) for o in resp.get("Contents", []))

    def list(self, prefix: str, suffix: str = "") -> list[str]:
        out, cut = [], len(self.prefix) + 1 if self.prefix else 0
        for page in self.s3.get_paginator("list_objects_v2").paginate(Bucket=self.bucket, Prefix=self._k(prefix)):
            out += [o["Key"][cut:] for o in page.get("Contents", []) if o["Key"].endswith(suffix)]
        return sorted(out)

    def get(self, key: str, local_dir: Path) -> Path:
        local = local_dir / Path(key).name
        local.parent.mkdir(parents=True, exist_ok=True)
        self.s3.download_file(self.bucket, self._k(key), str(local))
        return local

    def put(self, local: Path, key: str) -> None:
        self.s3.upload_file(str(local), self.bucket, self._k(key))   # multipart for big files

    def open_read(self, key_or_uri: str) -> BinaryIO:
        """Streaming read; accepts a key under the root or a full s3://bucket/key."""
        bucket, key = self.bucket, self._k(key_or_uri)
        if key_or_uri.startswith("s3://"):
            bucket, _, key = key_or_uri[5:].partition("/")
        return self.s3.get_object(Bucket=bucket, Key=key)["Body"]

    def read_json(self, key: str) -> dict:
        return json.loads(self.s3.get_object(Bucket=self.bucket, Key=self._k(key))["Body"].read())

    def write_json(self, key: str, obj: dict) -> None:
        self.s3.put_object(Bucket=self.bucket, Key=self._k(key),
                           Body=json.dumps(obj, indent=2, default=str).encode())


def get_storage(cfg: dict):
    """cfg = config/orchestrator.yaml. storage: local | s3."""
    kind = cfg["storage"]
    if kind == "local":
        return LocalStorage(cfg["local"]["root"])
    if kind == "s3":
        s = cfg["s3"]
        return S3Storage(s["bucket"], s.get("prefix", ""), s.get("region"), s.get("profile"))
    raise ValueError(f"storage must be 'local' or 's3', got {kind!r}")


def open_input(uri: str, storage) -> BinaryIO:
    """The big input file: a local path, or s3://bucket/key (read with the S3 client)."""
    if uri.startswith("s3://"):
        if not isinstance(storage, S3Storage):
            import boto3
            bucket, _, key = uri[5:].partition("/")
            return boto3.client("s3").get_object(Bucket=bucket, Key=key)["Body"]
        return storage.open_read(uri)
    p = Path(uri)
    return open(p if p.is_absolute() else ROOT / p, "rb")


# ---------------- the lake layout (Hive-style partitions) ----------------
STAGES = ["observations", "eligibility", "attribution", "findings"]


def chunk_name(source: str, chunk: int) -> str:
    return f"{source}_c{chunk:05d}"


def bronze_key(source: str, chunk: int) -> str:
    return f"bronze/source={source}/chunk={chunk:05d}/{chunk_name(source, chunk)}.json.zst"


def silver_key(stage: str, source: str, chunk: int) -> str:
    return f"silver/{stage}/source={source}/chunk={chunk:05d}/{chunk_name(source, chunk)}.parquet"


def meta_key(source: str) -> str:
    return f"bronze/source={source}/_meta.json"


def done_key(source: str, chunk: int) -> str:
    return f"manifest/source={source}/chunk={chunk:05d}.json"


def gold_key(name: str) -> str:
    return f"gold/{name}"


def chunks_of(storage, source: str) -> Iterator[int]:
    """Chunk numbers present in bronze for a source (from the folder names)."""
    for key in storage.list(f"bronze/source={source}/", ".json.zst"):
        yield int(key.split("chunk=")[1].split("/")[0])
