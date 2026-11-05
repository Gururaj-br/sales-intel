# Orchestrator

Takes big scan files (millions of NDJSON records, `.json.zst`) from a landing folder, splits each into
part files, processes the parts in parallel and publishes the ranked accounts for the app.
The same code runs on a **local folder** or an **S3 bucket**: only `config/orchestrator.yaml` changes.

```
land     -> landing/dt=<date>/source=<src>/<file>                          original file, never changed
split    -> bronze/dt=…/source=…/file=<file id>/part-00000.json.zst …      500k records per part
process  -> silver/<stage>/dt=…/source=…/file=…/part-00000.parquet …       4 stages per part, in parallel
            manifest/dt=…/source=…/file=…/_split.json, part-00000.done.json
score    -> gold/dt=<run date>/ + gold/latest/  -> copied to data/out (the app)
```

## Storage layout
One root: `data/lake/` locally, or the S3 bucket (optionally under a prefix). Below it, the keys are identical.
```
<root>/
├── landing/dt=2026-11-01/source=shodan/sample_assignment.json.zst
├── bronze/dt=2026-11-01/source=shodan/file=sample_assignment/part-00000.json.zst … part-00026.json.zst
├── silver/observations|eligibility|attribution|findings/dt=2026-11-01/source=shodan/file=sample_assignment/part-00000.parquet …
├── manifest/dt=2026-11-01/source=shodan/file=sample_assignment/_split.json, part-00000.done.json …
└── gold/dt=2026-11-05/…, gold/latest/accounts.parquet, account_findings.parquet
```
- `dt` / `source` / `file` (and optional `hr`) are Hive partitions: `read_parquet('…/silver/findings/**/*.parquet', hive_partitioning = true)`
  in DuckDB, or an Athena / Spark table, gives them as columns and only reads the folders a filter needs.
- `hr=HH` is switched on for the whole lake with `layout.use_hour: true` (then every file must come with an hour).
  It is never mixed, because paths of different depths break Hive readers.
- `file=` is the landed file name without extension: several files from one source on one day never collide.

## The meta details
Every command except `status` and `score` works on one or more landed files, identified by:

| key | required | example | rule |
|---|---|---|---|
| `dt` | yes | `2026-11-01` | a real date, `YYYY-MM-DD` |
| `source` | yes | `shodan` | lowercase letters, digits, `_`, `-` |
| `file` | yes | `sample_assignment.json.zst` | the file name in landing |
| `hr` | only if `layout.use_hour` | `14` | `00`–`23`; refused when hours are off |

Ways to pass them:
```bash
--dt 2026-11-01 --source shodan --file sample_assignment.json.zst          # flags
--meta meta.json                                                            # one or many files
--pending                                                                   # every landed file not finished yet
```
```json
{"files": [
  {"dt": "2026-11-01", "source": "shodan", "file": "sample_assignment.json.zst"},
  {"dt": "2026-11-02", "source": "shodan", "file": "scan_2026_11_02.json.zst"}
]}
```
`_split.json` also records what is measured, not passed: file size, sha256 (to spot the same content landed twice
under another name), record count, part count and split time.

## Setup
```bash
pip install -r requirements.txt        # includes zstandard (split) and boto3 (S3)
```
For S3, the only extra thing needed is a normal AWS login. Any of these works; nothing goes in the repo:
- `aws configure` (writes `~/.aws/credentials`), or
- `AWS_PROFILE=...`, or `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` in the shell or in `.env`, or
- an IAM role (EC2 / ECS / Lambda).

Then set `s3.bucket` and `s3.region` in `config/orchestrator.yaml` (the bucket must exist).
Permissions needed on that bucket: `s3:ListBucket`, `s3:GetObject`, `s3:PutObject`.

## Usage (from the repo root)
```bash
# 1. put a file into landing (on S3 the provider can also upload straight to s3://<bucket>/landing/dt=…/source=…/)
python -m orchestrator land --input /path/to/sample_assignment.json.zst --dt 2026-11-01 --source shodan

# 2. everything in one go: split -> process -> score -> data/out
python -m orchestrator run --dt 2026-11-01 --source shodan --file sample_assignment.json.zst
python -m orchestrator run --pending                 # all landed files not finished yet (cron / S3 event)
python -m orchestrator run --pending --storage s3    # the same against the S3 bucket

# or step by step
python -m orchestrator split   --dt 2026-11-01 --source shodan --file sample_assignment.json.zst
python -m orchestrator process --dt 2026-11-01 --source shodan --file sample_assignment.json.zst --workers 3
python -m orchestrator score            # everything in silver; --no-llm = reuse cached labels only
python -m orchestrator score --dry-run  # only count the owner names the LLM would label (no API call)
python -m orchestrator status           # landed files, parts done, gold present

# smaller parts for a quick look
python -m orchestrator split --dt 2026-11-01 --source shodan --file demo.json.zst --part-size 100000
```

## Raw data is kept twice
`landing/` keeps the file as delivered; `bronze/` keeps the same records as parts, copied byte-for-byte. Silver keeps only the
fields the rules use, so nothing is lost: to use another field later, add it to `pipeline/ingest.py` and rerun `process --force`.
On S3, a lifecycle rule can move `landing/` to archive storage after N days (`_split.json` keeps size + sha256).

## Reruns
| Step | On a rerun |
|---|---|
| `split` | skipped when `_split.json` exists (`--force` redoes it) |
| `process` | parts with a done marker are skipped; only new or failed parts run |
| `score` | always rebuilt (seconds); the LLM only gets owner names not in the cache, so a rerun with no new names costs nothing |

## Settings (`config/orchestrator.yaml`)
| key | meaning |
|---|---|
| `storage` | `local` or `s3` (CLI `--storage`) |
| `local.root` / `s3.bucket`, `s3.prefix` | the root of the lake |
| `layout.use_hour` | add `hr=` to every path |
| `split.records_per_part` | part size (default 500,000 records, about 0.75 GB compressed; CLI `--part-size`) |
| `process.workers` | parts at the same time; `workers x duckdb_memory_per_worker` must fit in RAM |
| `llm_step` | label new owner names with the LLM before scoring (cached; needs `GROQ_API_KEY`; CLI `--no-llm`) |
| `publish_to` | where gold/latest is copied for the app (`data/out`) |

Totals of the last `process` run go to `logs/` (`parts_summary.csv`, `eligibility_summary.csv`, `attribution_summary.csv`).

## What has been tested
- End to end on local storage (`tests/test_orchestrator.py`): a 2,000-record file made of two zstd frames is landed,
  split into 3 parts, processed through all 4 stages, a rerun skips the finished parts, and score writes
  `gold/dt=…`, `gold/latest` and the app files. Layout keys and meta checks (bad date, hour rules) are tested too.
- The S3 backend against a fake in-memory S3 client (same calls as boto3).
- A manual run on the first 500,000 records of the real file: landed, split into 5 parts of 100,000, processed in 32 s on 2 workers.
- **Not yet:** a run on the full 18 GB file, or against a real AWS bucket.

## Running at production volume (hundreds to thousands of files a month)
Each command is a plain, stateless step, so it maps directly onto standard AWS pieces. No Spark or Airflow needed:
1. A file is uploaded to `s3://<bucket>/landing/dt=…/source=…/`. An S3 event (or a daily cron) starts
   `python -m orchestrator run --pending --storage s3` in a container (ECS / AWS Batch).
2. For more speed, several containers run `process` side by side. Finished parts (marker present) are skipped,
   so overlapping workers only waste a little work. For strict one-part-per-worker, put part ids on an SQS queue.
3. `score` reads all silver findings (well under 1% of the input size) and writes `gold/dt=<today>/` and `gold/latest/`.
4. The app keeps reading only `data/out`: publishing means copying two small Parquet files.

Cost grows with data volume only in `split` and `process`, which scale out by adding workers.
The LLM step grows only with **new** owner names, because every answer is cached.
