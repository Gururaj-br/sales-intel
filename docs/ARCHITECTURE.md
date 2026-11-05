# ARCHITECTURE

```
scan file (.json.zst, local or S3)
 -> split         orchestrator/split.py      bronze chunks, Hive folders: bronze/source=<id>/chunk=<n>/
 -> ingest        pipeline/ingest.py         one row per IP + port + transport, typed columns, pandera check
 -> eligibility   pipeline/eligibility.py    excluded (CDN/WAF, ISP, honeypot, security vendor) | needs proof (cloud, hosting) | eligible
 -> attribution   pipeline/attribute.py      owner proof: A cert O, B one company domain, C registry org; D = none (dropped)
 -> findings      pipeline/findings.py       rule-based problems, one row per IP + port + finding
 -> classify      pipeline/classify.py       LLM label per owner name (offline, cached): company / infrastructure / default / unknown
 -> score         pipeline/score.py          one row per account: score, confidence, reasons, pitch; abstain below 0.5
 -> serve         app/app.py                 Streamlit, reads data/out only
```
Steps ingest to findings run per chunk (independent, in parallel, restartable: `orchestrator/process.py`).
Classify and score run once over all findings.

## Outputs the app reads (`data/out`)
- `accounts.parquet`: owner_key, name, main_tier, country, domain, n_ips, n_findings, last_seen, score, confidence,
  reasons (top 3), pitch, status (ranked / abstain)
- `account_findings.parquet`: the finding rows behind every account (ip_str, port, finding, severity, evidence,
  detail, ts, tier, attr_weight, attr_agree, country, domain, ...). The app recomputes confidence from these.

## Storage layout (local folder or S3, same keys)
```
bronze/source=<id>/chunk=<n>/<id>_c<n>.json.zst      bronze/source=<id>/_meta.json
silver/<stage>/source=<id>/chunk=<n>/<id>_c<n>.parquet
manifest/source=<id>/chunk=<n>.json                  (chunk done + row counts)
gold/accounts.parquet, gold/account_findings.parquet
```
Details and production setup: [orchestrator/README.md](../orchestrator/README.md).
