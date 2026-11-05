"""storage.py: one small interface, two backends (local folder or S3), and the lake layout.

Every path is a key relative to the storage root, written with "/". The root is a local folder
(`local.root`) or an S3 bucket (+ optional prefix); the keys are identical in both:

  landing/dt=<YYYY-MM-DD>/source=<src>[/hr=<HH>]/<file>.json.zst          the original file, untouched
  bronze/dt=…/source=…[/hr=…]/file=<file id>/part-00000.json.zst           split parts
  silver/<stage>/dt=…/source=…[/hr=…]/file=<file id>/part-00000.parquet    one per part per stage
  manifest/dt=…/source=…[/hr=…]/file=<file id>/_split.json                 what was split
  manifest/dt=…/source=…[/hr=…]/file=<file id>/part-00000.done.json        part finished (+ row counts)
  gold/dt=<run date>/accounts.parquet, account_findings.parquet           snapshot per scoring day
  gold/latest/…                                                           newest snapshot (copied to data/out)

`hr=` is on for the whole lake or off for the whole lake (config `layout.use_hour`), never mixed,
so every path has the same depth and Hive readers (DuckDB, Athena, Spark) see the same columns.
S3 credentials come from the normal AWS chain (AWS_PROFILE / ~/.aws/credentials / env vars / IAM role).
"""
from __future__ import annotations

import json
import os
import re
import shutil
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import BinaryIO, Iterator

ROOT = Path(__file__).resolve().parent.parent
STAGES = ["observations", "eligibility", "attribution", "findings"]

SOURCE_RE = re.compile(r"^[a-z0-9_-]+$")


# ---------------- partition = the meta details of one landed file ----------------
@dataclass(frozen=True)
class Partition:
    dt: str              # YYYY-MM-DD: the date the file belongs to
    source: str          # data provider / feed, e.g. "shodan"
    file: str            # landed file name, e.g. "sample_assignment.json.zst"
    hr: str | None = None  # HH, only when layout.use_hour is true

    @property
    def file_id(self) -> str:
        """sample_assignment.json.zst -> sample_assignment (safe as a folder name)."""
        return re.sub(r"[^A-Za-z0-9_-]", "_", self.file.split(".")[0])

    def prefix(self) -> str:
        """dt=…/source=…[/hr=…]  (the landing folder of this file)."""
        return f"dt={self.dt}/source={self.source}" + (f"/hr={self.hr}" if self.hr is not None else "")

    def path(self) -> str:
        """dt=…/source=…[/hr=…]/file=…  (bronze, silver and manifest folders of this file)."""
        return f"{self.prefix()}/file={self.file_id}"

    def label(self) -> str:
        return self.path().replace("/", " ")


def make_partition(meta: dict, use_hour: bool) -> Partition:
    """Check the meta details ({"dt", "source", "file", "hr"?}) and build a Partition."""
    missing = [k for k in ("dt", "source", "file") if not meta.get(k)]
    if missing:
        raise ValueError(f"meta is missing {missing}: need dt, source, file (+ hr if layout.use_hour)")
    date.fromisoformat(meta["dt"])                                  # raises on a bad date
    src = str(meta["source"]).lower()
    if not SOURCE_RE.match(src):
        raise ValueError(f"source {src!r}: use lowercase letters, digits, _ and - only")
    hr = meta.get("hr")
    if use_hour:
        if hr is None or not re.fullmatch(r"\d{1,2}", str(hr)) or int(hr) > 23:
            raise ValueError("layout.use_hour is true: meta needs hr = 00..23")
        hr = f"{int(hr):02d}"
    elif hr is not None:
        raise ValueError("layout.use_hour is false: do not pass hr (paths would have mixed depths)")
    return Partition(meta["dt"], src, Path(str(meta["file"])).name, hr)


def partition_from_landing_key(key: str) -> Partition:
    """landing/dt=…/source=…[/hr=…]/<file> -> Partition (used by `run --pending`)."""
    parts = key.split("/")
    kv = dict(p.split("=", 1) for p in parts[1:-1])
    return Partition(kv["dt"], kv["source"], parts[-1], kv.get("hr"))


def landing_key(p: Partition) -> str:
    return f"landing/{p.prefix()}/{p.file}"


def bronze_key(p: Partition, n: int) -> str:
    return f"bronze/{p.path()}/part-{n:05d}.json.zst"


def silver_key(stage: str, p: Partition, n: int) -> str:
    return f"silver/{stage}/{p.path()}/part-{n:05d}.parquet"


def split_key(p: Partition) -> str:
    return f"manifest/{p.path()}/_split.json"


def done_key(p: Partition, n: int) -> str:
    return f"manifest/{p.path()}/part-{n:05d}.done.json"


def gold_key(name: str, run_dt: str | None = None) -> str:
    return f"gold/{'latest' if run_dt is None else 'dt=' + run_dt}/{name}"


def parts_of(storage, p: Partition) -> Iterator[int]:
    """Part numbers present in bronze for one landed file."""
    for key in storage.list(f"bronze/{p.path()}/", ".json.zst"):
        yield int(key.rsplit("part-", 1)[1].split(".")[0])


# ---------------- backends ----------------
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

    def get(self, key: str, local_dir: Path, name: str | None = None) -> Path:
        """A local path for `key`. No copy: the lake file itself, or a symlink called `name`."""
        if name is None:
            return self._p(key)
        link = local_dir / name
        link.parent.mkdir(parents=True, exist_ok=True)
        link.symlink_to(self._p(key))
        return link

    def put(self, local: Path, key: str) -> None:
        dst = self._p(key)
        dst.parent.mkdir(parents=True, exist_ok=True)
        tmp = dst.with_name(dst.name + ".tmp")
        shutil.copyfile(local, tmp)
        tmp.replace(dst)           # readers never see a half-written file

    def open_read(self, key: str) -> BinaryIO:
        return open(self._p(key), "rb")

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
        self.s3, self.bucket, self.prefix = client, bucket, (prefix or "").strip("/")
        self.uri = f"s3://{bucket}" + (f"/{self.prefix}" if self.prefix else "")

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

    def get(self, key: str, local_dir: Path, name: str | None = None) -> Path:
        """Download `key` to local_dir/<name> (default: the full key path, so names never collide)."""
        local = local_dir / (name or key)
        local.parent.mkdir(parents=True, exist_ok=True)
        self.s3.download_file(self.bucket, self._k(key), str(local))
        return local

    def put(self, local: Path, key: str) -> None:
        self.s3.upload_file(str(local), self.bucket, self._k(key))   # multipart for big files

    def open_read(self, key: str) -> BinaryIO:
        """Streaming read (used to split a big landed file without downloading it first)."""
        return self.s3.get_object(Bucket=self.bucket, Key=self._k(key))["Body"]

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


def local_input(path: str) -> Path:
    p = Path(os.path.expanduser(path))
    return p if p.is_absolute() else ROOT / p
