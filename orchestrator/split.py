"""split.py: a landed file -> bronze part files, under the same dt/source[/hr] partition.

  landing/dt=2026-11-01/source=shodan/sample_assignment.json.zst              (read, never changed)
  bronze/dt=2026-11-01/source=shodan/file=sample_assignment/part-00000.json.zst
  bronze/…/file=sample_assignment/part-00001.json.zst …
  manifest/dt=2026-11-01/source=shodan/file=sample_assignment/_split.json      what was split

Streams: the landed file is never fully decompressed to disk or memory, so memory use does not
grow with file size, and on S3 it is read straight from the bucket. Records are copied
byte-for-byte, not parsed, so splitting cannot change the data. Several zstd frames in one file
(e.g. files joined with `cat`) are read as one stream. Size and sha256 of the landed file are
recorded, so a re-upload of the same content under another name can be spotted.
(Tested on small inputs; not yet run on the full 18 GB file.)
"""
from __future__ import annotations

import hashlib
import io
import tempfile
import time
from contextlib import closing
from datetime import datetime, timezone
from pathlib import Path

from orchestrator.storage import Partition, bronze_key, landing_key, split_key


class _Hashing(io.RawIOBase):
    """Wraps the input stream: counts bytes and computes sha256 while the split reads it."""
    def __init__(self, raw):
        self.raw, self.sha, self.size = raw, hashlib.sha256(), 0

    def readable(self):
        return True

    def readinto(self, b):
        data = self.raw.read(len(b))
        n = len(data)
        b[:n] = data
        self.sha.update(data)
        self.size += n
        return n


def split_file(storage, p: Partition, records_per_part: int, zstd_level: int = 3,
               work_dir: Path | None = None, force: bool = False) -> dict:
    """Split landing/<partition>/<file> into parts of `records_per_part` lines. Returns _split.json."""
    import zstandard as zstd   # only needed for the split step

    if storage.exists(split_key(p)) and not force:
        meta = storage.read_json(split_key(p))
        print(f"split: {p.label()} already split into {meta['parts']} parts (use --force to redo)")
        return meta
    if not storage.exists(landing_key(p)):
        raise FileNotFoundError(f"not in landing: {storage.uri}/{landing_key(p)}  (use `land` first)")

    if work_dir:
        Path(work_dir).mkdir(parents=True, exist_ok=True)
    t0 = time.time()
    cctx = zstd.ZstdCompressor(level=zstd_level, threads=-1)
    part, in_part, records, writer, local = 0, 0, 0, None, None
    with tempfile.TemporaryDirectory(dir=work_dir) as tmp, closing(storage.open_read(landing_key(p))) as raw:
        src = _Hashing(raw)
        lines = io.BufferedReader(zstd.ZstdDecompressor().stream_reader(io.BufferedReader(src, 1 << 20),
                                                                        read_across_frames=True),
                                  buffer_size=1 << 20)

        def close_part():
            writer.close()                                   # flushes and closes the file
            storage.put(local, bronze_key(p, part))
            local.unlink()
            print(f"  part-{part:05d}: {in_part:,} records ({time.time() - t0:.0f}s)", flush=True)

        for line in lines:
            if not line.strip():
                continue
            if writer is None:
                local = Path(tmp) / f"part-{part:05d}.json.zst"
                writer = cctx.stream_writer(open(local, "wb"))
            writer.write(line if line.endswith(b"\n") else line + b"\n")
            in_part += 1
            records += 1
            if in_part == records_per_part:
                close_part()
                part, in_part, writer = part + 1, 0, None
        if writer is not None:
            close_part()
            part += 1

    meta = {"dt": p.dt, "source": p.source, "hr": p.hr, "file": p.file, "file_id": p.file_id,
            "landing": f"{storage.uri}/{landing_key(p)}", "bytes": src.size, "sha256": src.sha.hexdigest(),
            "records": records, "parts": part, "records_per_part": records_per_part,
            "seconds": round(time.time() - t0, 1),
            "split_at": datetime.now(timezone.utc).isoformat(timespec="seconds")}
    storage.write_json(split_key(p), meta)     # written LAST = split is complete
    print(f"split: {records:,} records -> {part} parts in {meta['seconds']}s")
    return meta
