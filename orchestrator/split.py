"""split.py: one big scan file (NDJSON, .json.zst) -> bronze chunks in Hive-style folders.

  bronze/source=<source>/chunk=00000/<source>_c00000.json.zst
  bronze/source=<source>/chunk=00001/...
  bronze/source=<source>/_meta.json          what was split, when, how many records

Streams: the input is never fully decompressed to disk or memory, so memory use does not grow
with file size, and s3:// inputs are read straight from S3. (Tested on small inputs; not yet run
on the full 18 GB file.) Records are copied byte-for-byte,
not parsed, so splitting cannot change the data. Several zstd frames in one file
(e.g. files concatenated with `cat`) are read as one stream.
"""
from __future__ import annotations

import io
import tempfile
from contextlib import closing
import time
from datetime import datetime, timezone
from pathlib import Path

from orchestrator.storage import bronze_key, meta_key, open_input


def split_file(storage, input_uri: str, source: str, records_per_chunk: int,
               zstd_level: int = 3, work_dir: Path | None = None, force: bool = False) -> dict:
    """Split `input_uri` into chunks of `records_per_chunk` lines. Returns the meta dict."""
    import zstandard as zstd   # only needed for the split step

    if storage.exists(meta_key(source)) and not force:
        meta = storage.read_json(meta_key(source))
        print(f"split: source={source} already split into {meta['chunks']} chunks (use --force to redo)")
        return meta

    if work_dir:
        Path(work_dir).mkdir(parents=True, exist_ok=True)
    t0 = time.time()
    cctx = zstd.ZstdCompressor(level=zstd_level, threads=-1)
    chunk, in_chunk, records, writer, local = 0, 0, 0, None, None
    with tempfile.TemporaryDirectory(dir=work_dir) as tmp, closing(open_input(input_uri, storage)) as raw:
        lines = io.BufferedReader(zstd.ZstdDecompressor().stream_reader(raw, read_across_frames=True),
                                  buffer_size=1 << 20)

        def close_chunk():
            writer.close()                                   # flushes and closes the file
            storage.put(local, bronze_key(source, chunk))
            local.unlink()
            print(f"  chunk {chunk:05d}: {in_chunk:,} records ({time.time() - t0:.0f}s)", flush=True)

        for line in lines:
            if not line.strip():
                continue
            if writer is None:
                local = Path(tmp) / f"chunk{chunk:05d}.json.zst"
                writer = cctx.stream_writer(open(local, "wb"))
            writer.write(line if line.endswith(b"\n") else line + b"\n")
            in_chunk += 1
            records += 1
            if in_chunk == records_per_chunk:
                close_chunk()
                chunk, in_chunk, writer = chunk + 1, 0, None
        if writer is not None:
            close_chunk()
            chunk += 1

    meta = {"source": source, "input": input_uri, "records": records, "chunks": chunk,
            "records_per_chunk": records_per_chunk, "seconds": round(time.time() - t0, 1),
            "split_at": datetime.now(timezone.utc).isoformat(timespec="seconds")}
    storage.write_json(meta_key(source), meta)
    print(f"split: {records:,} records -> {chunk} chunks in {meta['seconds']}s")
    return meta
