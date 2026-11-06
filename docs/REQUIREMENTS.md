# REQUIREMENTS

## Functional (rep use cases -> features)
| Rep question | Feature |
|---|---|
| Who do I call this week? | Ranked account list; filters: country, finding type, confidence |
| Why this account? | Account page: exact evidence (IPs, ports, versions, dates) |
| What do I pitch? | Pitch angle from finding type |
| Can I trust it? | Live confidence; "insufficient evidence" label instead of a guessed score |
| How do I reach them? | Domain, country, CSV export (no contact enrichment) |
| What's new? | New exposures since previous part (needs multiple parts) |

## Non-functional
- Idempotent ingestion: per-part manifest (name, hash, row count, status); reruns skip completed parts.
- Stateless per-part processing; scale-out = one worker per part. Read only needed columns.
- Data contracts (pandera) at each stage; bad parts quarantined.
- Hosted app: small file (<~200 MB); rankings precomputed. Only the optional Ask AI tab calls an LLM, key in Streamlit secrets, capped per session.
- Every LLM step: schema-validated JSON, temperature 0, cache by content hash, "unknown" fallback, measured accuracy and cost.

## Edge cases
Duplicate/variant org names; missing/malformed domains; infra domains (amazonaws, incapdns); cloud IPs owned by provider;
ISP/residential; honeypots; error banners ("SSL Error", "400 Bad Request"); schema drift between parts; shared edge IPs;
huge accounts (500 identical hosts -> diminishing returns); thin evidence -> abstain; reruns/partial failures.

## Out of scope
Outreach email generation, contact enrichment, fancy UI, Spark/Airflow/K8s.
