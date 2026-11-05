# Orchestrator

Takes big scan files (one file = millions of NDJSON records, `.json.zst`), splits each into
chunks, processes the chunks in parallel and publishes the ranked accounts for the app.
The same code runs on a **local folder** or on **Amazon S3**: only `config/orchestrator.yaml` changes.

```
input file (local or s3://)
  -> split    -> bronze/source=<id>/chunk=<n>/<id>_c<n>.json.zst      (Hive-style partitions)
  -> process  -> silver/<stage>/source=<id>/chunk=<n>/<id>_c<n>.parquet  (4 stages, chunks in parallel)
              -> manifest/source=<id>/chunk=<n>.json                   ("done" marker + row counts)
  -> score    -> gold/accounts.parquet, gold/account_findings.parquet -> copied to data/out (the app)
```

## Why chunks
- **Memory stays flat.** A worker only ever holds one chunk (500k records by default), whatever the file size.
- **Parallel.** Chunks don't depend on each other, so N workers run N chunks at once
  (one machine: `workers` in config; many machines: run `process` on each, see below).
- **Restartable.** Each chunk writes a done marker last. If a run crashes, run it again: finished chunks are skipped.
- **Repeat scans don't count twice.** The same IP + port seen in several chunks or files keeps only its
  latest sighting at the score step.

## What has been tested
- End to end on local storage (`tests/test_orchestrator.py`): a 2,000-record file made of two zstd frames
  -> 3 chunks -> all 4 stages -> rerun skips finished chunks -> score -> app files.
- The S3 backend against a fake in-memory S3 client (same calls as boto3).
- **Not yet:** a run on the full 18 GB file, or against a real AWS bucket. The chunk size is based on a real
  500,000-record chunk, which processed in a 4 GB DuckDB memory cap.

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
# everything in one go: split -> process -> score -> data/out
python -m orchestrator run --input data/input/scan.json.zst --source scan_2026_11

# same, on S3 (input can be local or already in S3)
python -m orchestrator run --storage s3 --input s3://my-bucket/incoming/scan.json.zst --source scan_2026_11

# triggered with meta details (what an S3 upload event or a scheduler would pass)
python -m orchestrator run --meta meta.json

# or step by step
python -m orchestrator split   --input data/input/scan.json.zst --source scan_2026_11
python -m orchestrator process --source scan_2026_11 --workers 3
python -m orchestrator score            # all sources together; --no-llm = reuse cached labels only
python -m orchestrator status           # what is in the lake
```
`meta.json`:
```json
{"sources": [
  {"source": "scan_2026_11_01", "input": "s3://my-bucket/incoming/scan_2026_11_01.json.zst"},
  {"source": "scan_2026_11_02", "input": "s3://my-bucket/incoming/scan_2026_11_02.json.zst"}
]}
```
`source` is any id made of letters, digits, `_` and `-` (default: the input file name).

## Settings (`config/orchestrator.yaml`)
| key | meaning |
|---|---|
| `storage` | `local` or `s3` |
| `local.root` / `s3.bucket`, `s3.prefix` | where the lake lives |
| `split.records_per_chunk` | chunk size (default 500,000 records, about 0.75 GB compressed) |
| `process.workers` | chunks at the same time; `workers x duckdb_memory_per_worker` must fit in RAM |
| `llm_step` | label new owner names with the LLM before scoring (cached; needs `GROQ_API_KEY`) |
| `publish_to` | where gold is copied for the app (`data/out`) |

## Running at production volume (hundreds to thousands of files a month)
Each command is a plain, stateless step, so it maps directly onto standard AWS pieces. No Spark or Airflow needed:
1. A new file lands in `s3://<bucket>/incoming/`. An S3 event (or a daily cron) starts
   `python -m orchestrator split --input s3://... --source <id>` in a container (ECS / AWS Batch).
2. `process` fans out: run several containers, each with `process --source <id>`. A chunk already done
   (marker present) is skipped, so overlapping workers do no harm beyond wasted work.
   For strict one-chunk-per-worker, put chunk ids on an SQS queue and give each worker one.
3. Once a day, `score` reads all silver findings (tiny: well under 1% of the input size) and writes gold.
4. The app keeps reading only `data/out`. Publishing gold means copying two small Parquet files.

Cost grows with data volume only in `split` and `process`, which scale out by adding workers.
The LLM step grows only with **new** owner names, because every answer is cached.
