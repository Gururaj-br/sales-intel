# CLAUDE.md (agent context; read first)
Project rules for the AI coding assistant used to build this repo.

## Rules for the agent
- Data is Shodan-style scan records (one per IP+port). Not company data. Do not invent fields; check with DESCRIBE.
- Never score CDN/WAF vendors, clouds without attribution, ISPs, residential.
- Prefer abstain to guess. Confidence must be computed, never constant.
- Keep it simple: DuckDB + Polars + pandera + Streamlit. No Spark/Airflow.
- Hosted app reads only precomputed output. By default no LLM calls, no keys. Optional "Ask" chat is behind
  `config/app.yaml` (chat.enabled, default false); when on, the LLM only turns the question into filters
  (JSON schema, temp 0, unknown fallback, cost logged) and the key comes from env/Streamlit secrets, never the repo.
- Every LLM step: JSON schema, temp 0, cache, unknown fallback, gold-set check, cost logged.
- Every change must report an eval delta; do not weaken tests to pass.
- Config in `config/*.yaml`, not hardcoded.
- Log mistakes you made and how they were caught in `DEV_LOG.md`.

## Verified gotchas
`rows` reserved in DuckDB; use `sample_size=200000`, `maximum_object_size=33554432`, `ignore_errors=true`.
`domains` == derived from `hostnames`; hostname != owner.
