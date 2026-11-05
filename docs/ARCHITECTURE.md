# ARCHITECTURE

```
scan file (.json.zst) lands in landing/dt=<date>/source=<src>/   (local folder or S3 bucket)
 -> split         orchestrator/split.py      bronze parts: bronze/dt=…/source=…/file=…/part-NNNNN.json.zst
 -> ingest        pipeline/ingest.py         one row per IP + port + transport, typed columns, pandera check
 -> eligibility   pipeline/eligibility.py    excluded (CDN/WAF, ISP, honeypot, security vendor) | needs proof (cloud, hosting) | eligible
 -> attribution   pipeline/attribute.py      owner proof: A cert O, B one company domain, C registry org; D = none (dropped)
 -> findings      pipeline/findings.py       rule-based problems, one row per IP + port + finding
 -> classify      pipeline/classify.py       LLM label per owner name (offline, cached): company / infrastructure / default / unknown
 -> score         pipeline/score.py          one row per account: score, confidence, reasons, pitch; abstain below 0.5
 -> serve         app/app.py                 Streamlit, reads data/out only
```
Steps ingest to findings run per part (independent, in parallel, restartable: `orchestrator/process.py`).
Classify and score run once over all findings.

## Outputs the app reads (`data/out`)
- `accounts.parquet`: owner_key, name, main_tier, country, domain, n_ips, n_findings, last_seen, score, confidence,
  reasons (top 3), pitch, status (ranked / abstain)
- `account_findings.parquet`: the finding rows behind every account (ip_str, port, finding, severity, evidence,
  detail, ts, tier, attr_weight, attr_agree, country, domain, ...). The app recomputes confidence from these.

## Storage layout (local folder or S3 bucket, same keys)
```
landing/dt=<date>/source=<src>[/hr=<HH>]/<file>.json.zst                     original file, never changed
bronze/dt=…/source=…[/hr=…]/file=<file id>/part-00000.json.zst               500k records per part
silver/<stage>/dt=…/source=…[/hr=…]/file=<file id>/part-00000.parquet        stage = observations | eligibility | attribution | findings
manifest/dt=…/source=…[/hr=…]/file=<file id>/_split.json, part-00000.done.json
gold/dt=<run date>/…, gold/latest/accounts.parquet, account_findings.parquet
```
Details and production setup: [orchestrator/README.md](../orchestrator/README.md).
